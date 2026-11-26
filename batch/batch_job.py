
import os
import io
import boto3
import pandas as pd
import pyarrow.parquet as pq
import psycopg

from botocore.config import Config
from dotenv import load_dotenv


# --------------------------------------------------
# Configuration
# --------------------------------------------------

load_dotenv(override=True)

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "chargepulse")
DB_USER = os.getenv("POSTGRES_USER", "chargepulse")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "chargepulse_dev")


# --------------------------------------------------
# Read raw events from S3
# --------------------------------------------------

def read_raw_events():

    if not S3_BUCKET:
        raise ValueError("S3_BUCKET_NAME is missing from .env")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name=AWS_REGION,
        config=Config(
            s3={"addressing_style": "path"}
        )
    )

    paginator = s3.get_paginator("list_objects_v2")

    events = []
    files_processed = 0

    print("Reading raw Parquet files from S3...")

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix="raw/"
    ):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            response = s3.get_object(
                Bucket=S3_BUCKET,
                Key=key
            )

            parquet_data = response["Body"].read()

            table = pq.read_table(
                io.BytesIO(parquet_data)
            )

            df = table.to_pandas()

            if not df.empty:
                events.append(df)

            files_processed += 1

    if not events:
        raise ValueError("No raw Parquet events found in S3")

    combined_df = pd.concat(
        events,
        ignore_index=True
    )

    print(f"Raw files processed: {files_processed}")

    return combined_df


# --------------------------------------------------
# Transform and aggregate events
# --------------------------------------------------

def create_hourly_aggregates(df):

    required_columns = [
        "event_id",
        "event_ts",
        "station_id",
        "session_id",
        "energy_delta_kwh",
        "power_kw"
    ]

    missing = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )

    # Parse timestamps as UTC
    df["event_ts"] = pd.to_datetime(
        df["event_ts"],
        utc=True,
        errors="coerce"
    )

    # Remove invalid records
    df = df.dropna(
        subset=[
            "event_id",
            "event_ts",
            "station_id",
            "session_id"
        ]
    )

    # Remove duplicate events
    df = df.drop_duplicates(
        subset=["event_id"],
        keep="first"
    )

    # Convert numeric measures
    df["energy_delta_kwh"] = pd.to_numeric(
        df["energy_delta_kwh"],
        errors="coerce"
    )

    df["power_kw"] = pd.to_numeric(
        df["power_kw"],
        errors="coerce"
    )

    df = df.dropna(
        subset=[
            "energy_delta_kwh",
            "power_kw"
        ]
    )

    if df.empty:
        raise ValueError("No valid events remain after cleaning")

    print(f"Unique events: {len(df)}")

    # Calculate hourly windows
    df["window_start"] = df["event_ts"].dt.floor("h")

    df["window_end"] = (
        df["window_start"] + pd.Timedelta(hours=1)
    )

    # Aggregate by station and hour
    aggregates = (
        df.groupby(
            [
                "window_start",
                "window_end",
                "station_id"
            ],
            as_index=False
        )
        .agg(
            total_energy_kwh=(
                "energy_delta_kwh",
                "sum"
            ),
            avg_power_kw=(
                "power_kw",
                "mean"
            ),
            session_count=(
                "session_id",
                "nunique"
            )
        )
    )

    print(f"Batch aggregate rows: {len(aggregates)}")

    # Calculate event-time boundary
    batch_boundary = (
        df["event_ts"].max().floor("h")
        + pd.Timedelta(hours=1)
    )

    print(f"Batch event-time boundary: {batch_boundary}")

    return aggregates, batch_boundary


# --------------------------------------------------
# Write batch results to PostgreSQL
# --------------------------------------------------

def write_to_postgres(aggregates, batch_boundary):

    print("Writing batch results to PostgreSQL...")

    connection = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    try:

        with connection.cursor() as cur:

            # Replace previous batch results
            cur.execute("DELETE FROM batch_metrics")

            insert_query = """
                INSERT INTO batch_metrics (
                    window_start,
                    window_end,
                    station_id,
                    total_energy_kwh,
                    avg_power_kw,
                    session_count
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            for _, row in aggregates.iterrows():

                cur.execute(
                    insert_query,
                    (
                        row["window_start"].to_pydatetime(),
                        row["window_end"].to_pydatetime(),
                        str(row["station_id"]),
                        float(row["total_energy_kwh"]),
                        float(row["avg_power_kw"]),
                        int(row["session_count"])
                    )
                )

            # Update Lambda Architecture boundary
            cur.execute(
                """
                INSERT INTO pipeline_metadata (
                    key,
                    value,
                    updated_at
                )
                VALUES (
                    'batch_completed_at',
                    %s,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (key)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (batch_boundary.isoformat(),)
            )

        connection.commit()

        print("PostgreSQL batch tables updated.")
        print("Batch event-time boundary updated.")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


# --------------------------------------------------
# Main batch job
# --------------------------------------------------

def main():

    print("===================================")
    print("ChargePulse Batch Layer started")
    print("===================================")

    raw_df = read_raw_events()

    aggregates, batch_boundary = create_hourly_aggregates(raw_df)

    write_to_postgres(
        aggregates,
        batch_boundary
    )

    print("===================================")
    print("Batch Layer completed successfully.")
    print("===================================")


if __name__ == "__main__":
    main()

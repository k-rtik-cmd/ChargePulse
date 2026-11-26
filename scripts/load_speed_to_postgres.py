
import psycopg
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "streaming" / "output_v2"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "chargepulse",
    "user": "chargepulse",
    "password": "chargepulse_dev",
}

EPOCH = datetime(1970, 1, 1)

def ns_to_datetime(value):
    """Convert Spark timestamps or nanoseconds to Python datetime."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    return EPOCH + timedelta(microseconds=int(value) // 1000)


def main():
    files = sorted(OUTPUT_DIR.glob("part-*.parquet"))

    total_rows = 0
    inserted_rows = 0

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for file in files:
                table = pq.read_table(file)

                for row in table.to_pylist():
                    window_data = row["window"]

                    window_start = ns_to_datetime(window_data["start"])
                    window_end = ns_to_datetime(window_data["end"])

                    cur.execute("""
                        INSERT INTO speed_metrics (
                            window_start,
                            window_end,
                            station_id,
                            total_energy_kwh,
                            avg_power_kw,
                            session_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (
                            window_start,
                            window_end,
                            station_id
                        ) DO NOTHING
                    """, (
                        window_start,
                        window_end,
                        row["station_id"],
                        row["total_energy_kwh"],
                        row["average_power_kw"],
                        row["active_sessions"],
                    ))

                    total_rows += 1
                    inserted_rows += cur.rowcount

        conn.commit()

    print(f"Parquet rows processed: {total_rows}")
    print(f"New rows inserted: {inserted_rows}")
    print(f"Duplicate rows skipped: {total_rows - inserted_rows}")


if __name__ == "__main__":
    main()

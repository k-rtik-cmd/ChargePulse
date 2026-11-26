
import argparse
import os
import io
import json
import time
import random
import uuid
import boto3

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaProducer
import os


load_dotenv()

STATIONS = ["ST001", "ST002", "ST003", "ST004"]
CONNECTORS = ["C01", "C02", "C03", "C04"]
BATCH_SIZE = 25


def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "station_id": random.choice(STATIONS),
        "connector_id": random.choice(CONNECTORS),
        "vehicle_id": f"EV{random.randint(1000, 9999)}",
        "energy_delta_kwh": round(random.uniform(0.1, 2.0), 3),
        "power_kw": round(random.uniform(7.0, 150.0), 2),
        "session_status": random.choice(
            ["charging", "charging", "completed"]
        )
    }


def upload_batch_to_s3(events, s3, bucket):
    """Archive events as Parquet before publishing them to Kafka."""

    groups = defaultdict(list)

    for event in events:
        event_time = datetime.fromisoformat(event["event_ts"])
        partition = event_time.strftime(
            "year=%Y/month=%m/day=%d/hour=%H"
        )
        groups[partition].append(event)

    for partition, records in groups.items():
        table = pa.Table.from_pylist(records)

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        key = (
            f"raw/{partition}/"
            f"events-{uuid.uuid4().hex}.parquet"
        )

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue()
        )

        print(f"S3 archived: s3://{bucket}/{key}")


def publish_batch_to_kafka(events, producer):
    """Publish archived events to Kafka."""

    futures = [
        producer.send("charge-events", event)
        for event in events
    ]

    for future in futures:
        future.get(timeout=10)



def run_producer(total_events, events_per_second, duplicate_rate, late_rate):

    producer = None
    bucket = os.getenv("S3_BUCKET_NAME")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    if not bucket:
        raise ValueError("S3_BUCKET_NAME missing from .env")

    if events_per_second <= 0:
        raise ValueError("events_per_second must be greater than zero")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name=region
    )

    interval = 1.0 / events_per_second
    start_time = time.perf_counter()

    previous_events = []
    batch = []
    published = 0

    try:
        # Initialize Kafka producer before generating events.
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5
        )

        print("ChargePulse producer started.")
        print(f"Target rate: {events_per_second} events/second")
        print(f"S3 bucket: {bucket}")

        for count in range(total_events):

            if previous_events and random.random() < duplicate_rate:
                event = random.choice(previous_events).copy()

            else:
                event = generate_event()

                if random.random() < late_rate:
                    old_time = (
                        datetime.now(timezone.utc)
                        - timedelta(seconds=random.randint(10, 120))
                    )
                    event["event_ts"] = old_time.isoformat()

                previous_events.append(event.copy())

            batch.append(event)

            if len(batch) >= BATCH_SIZE:

                # Archive to S3 first.
                upload_batch_to_s3(batch, s3, bucket)

                # Publish to Kafka only after successful S3 upload.
                publish_batch_to_kafka(batch, producer)

                published += len(batch)
                batch.clear()

            next_event_time = start_time + (count + 1) * interval
            sleep_time = next_event_time - time.perf_counter()

            if sleep_time > 0:
                time.sleep(sleep_time)

        # Process final partial batch.
        if batch:
            upload_batch_to_s3(batch, s3, bucket)
            publish_batch_to_kafka(batch, producer)
            published += len(batch)

        producer.flush()

    finally:
        if producer is not None:
            producer.close()

    elapsed = time.perf_counter() - start_time

    print(
        f"\nPublished {published} events to S3 and Kafka "
        f"in {elapsed:.2f} seconds."
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ChargePulse Lambda Architecture producer"
    )

    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--duplicate-rate", type=float, default=0.05)
    parser.add_argument("--late-rate", type=float, default=0.05)

    args = parser.parse_args()

    if args.count <= 0 or args.rate <= 0:
        parser.error("--count and --rate must be positive")

    if not 0 <= args.duplicate_rate <= 1:
        parser.error("--duplicate-rate must be between 0 and 1")

    if not 0 <= args.late_rate <= 1:
        parser.error("--late-rate must be between 0 and 1")

    run_producer(
        args.count,
        args.rate,
        args.duplicate_rate,
        args.late_rate
    )

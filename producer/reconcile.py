"""Split-write reconciliation: compare one event-time hour in S3 with Kafka.

  python reconcile.py --hour 2026-11-26T17            # report only
  python reconcile.py --hour 2026-11-26T17 --apply    # repair both sides

* In S3 but not in Kafka (Kafka publish failed): re-published to Kafka.
* In Kafka but not in S3 (WAL lost before upload): written to S3 as a
  `repair-*.parquet` object in the same hour partition. The batch layer
  de-duplicates on event_id, so a repair can never double count.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pyarrow.parquet as pq  # noqa: E402
from kafka import KafkaConsumer, TopicPartition  # noqa: E402

from chargepulse_common.contract import (  # noqa: E402
    EVENTS_TOPIC,
    RAW_EVENTS_PREFIX,
    format_ts,
    hour_partition,
    parse_ts,
)
from producer import KafkaPublisher, build_s3_client, env  # noqa: E402
from wal import EVENT_ARROW_SCHEMA, _to_arrow  # noqa: E402

log = logging.getLogger("chargepulse.reconcile")


def s3_events_for_hour(s3, bucket: str, hour: datetime) -> dict[str, dict]:
    prefix = f"{RAW_EVENTS_PREFIX}/{hour_partition(hour)}/"
    events: dict[str, dict] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            for row in pq.read_table(io.BytesIO(body)).to_pylist():
                for ts_field in ("event_ts", "ingest_ts"):
                    if isinstance(row.get(ts_field), datetime):
                        row[ts_field] = format_ts(row[ts_field])
                events[row["event_id"]] = row
    return events


def kafka_events_for_hour(bootstrap: str, hour: datetime, max_lateness: timedelta) -> dict[str, dict]:
    """Read the topic between hour start and hour end + allowed lateness."""
    start_ms = int(hour.timestamp() * 1000)
    end_ms = int((hour + timedelta(hours=1) + max_lateness).timestamp() * 1000)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap.split(","),
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=5000,
    )
    partitions = [TopicPartition(EVENTS_TOPIC, p) for p in consumer.partitions_for_topic(EVENTS_TOPIC)]
    consumer.assign(partitions)
    starts = consumer.offsets_for_times({tp: start_ms for tp in partitions})
    ends = consumer.end_offsets(partitions)
    active = set()
    for tp in partitions:
        if starts[tp] is not None and starts[tp].offset < ends[tp]:
            consumer.seek(tp, starts[tp].offset)
            active.add(tp)

    hour_end = hour + timedelta(hours=1)
    events: dict[str, dict] = {}
    while active:
        batch = consumer.poll(timeout_ms=2000, max_records=5000)
        if not batch:
            break
        for tp, messages in batch.items():
            for msg in messages:
                if msg.timestamp > end_ms or msg.offset >= ends[tp] - 1:
                    active.discard(tp)
                event = json.loads(msg.value.decode("utf-8"))
                ts = parse_ts(event.get("event_ts"))
                if ts and hour <= ts < hour_end:
                    events[event["event_id"]] = event
            if tp not in active:
                consumer.pause(tp)
    consumer.close()
    return events


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--hour", required=True, help="event-time hour in UTC, e.g. 2026-11-26T17")
    p.add_argument("--apply", action="store_true", help="repair the differences")
    p.add_argument("--max-lateness-minutes", type=int, default=30)
    p.add_argument("--bucket", default=env("S3_BUCKET", None))
    p.add_argument("--bootstrap", default=env("KAFKA_BOOTSTRAP", "kafka:29092"))
    args = p.parse_args(argv)

    hour = datetime.strptime(args.hour, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    s3 = build_s3_client()
    s3_events = s3_events_for_hour(s3, args.bucket, hour)
    kafka_events = kafka_events_for_hour(args.bootstrap, hour, timedelta(minutes=args.max_lateness_minutes))

    missing_in_kafka = sorted(set(s3_events) - set(kafka_events))
    missing_in_s3 = sorted(set(kafka_events) - set(s3_events))
    report = {
        "hour": hour.isoformat(),
        "s3_unique_events": len(s3_events),
        "kafka_unique_events": len(kafka_events),
        "missing_in_kafka": len(missing_in_kafka),
        "missing_in_s3": len(missing_in_s3),
    }
    print(json.dumps(report, indent=2))

    if args.apply and missing_in_kafka:
        publisher = KafkaPublisher(args.bootstrap, Path("/tmp/reconcile_kafka_failed.log"))
        for event_id in missing_in_kafka:
            publisher.send(EVENTS_TOPIC, s3_events[event_id])
        publisher.close()
        log.info("re-published %d events to Kafka", len(missing_in_kafka))

    if args.apply and missing_in_s3:
        table = _to_arrow([kafka_events[i] for i in missing_in_s3], EVENT_ARROW_SCHEMA)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        key = f"{RAW_EVENTS_PREFIX}/{hour_partition(hour)}/repair-{stamp}.parquet"
        s3.put_object(Bucket=args.bucket, Key=key, Body=buffer.getvalue())
        log.info("wrote %d Kafka-only events to s3://%s/%s", len(missing_in_s3), args.bucket, key)

    return 0


if __name__ == "__main__":
    sys.exit(main())

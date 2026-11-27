"""ChargePulse producer: simulated EV charging events -> WAL/S3 + Kafka.

Runs forever by default at EVENT_RATE events/second. Every setting can be
given as an environment variable (see .env.example) or a CLI flag, e.g.

  # continuous service (docker compose)
  python producer.py

  # one-off burst of 500 events that are 40-60 minutes late
  python producer.py --count 500 --rate 200 --duplicate-rate 0 \
      --late-rate 1 --late-min-seconds 2400 --late-max-seconds 3600 \
      --instance late-burst
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from kafka import KafkaProducer  # noqa: E402
from kafka.errors import KafkaError  # noqa: E402

from chargepulse_common.contract import (  # noqa: E402
    DEAD_LETTER_PREFIX,
    EVENTS_TOPIC,
    RAW_EVENTS_PREFIX,
    RAW_SETTLEMENTS_PREFIX,
    SETTLEMENTS_TOPIC,
    validate_event,
    validate_settlement,
)
from chargepulse_common.tariffs import load_tariffs, station_ids  # noqa: E402
from simulator import ChargingSimulator, SimulatorConfig  # noqa: E402
from wal import (  # noqa: E402
    EVENT_ARROW_SCHEMA,
    SETTLEMENT_ARROW_SCHEMA,
    ArchiveStream,
    S3Archiver,
    SegmentLog,
)

log = logging.getLogger("chargepulse.producer")


def env(name: str, default, cast=str):
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return cast(value)


def build_s3_client():
    """Default AWS credential chain (env vars, or the EC2 instance profile).

    S3_ENDPOINT_URL is only set for the local S3-compatible store.
    """
    endpoint = os.getenv("S3_ENDPOINT_URL") or None
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            retries={"max_attempts": 5, "mode": "standard"},
            s3={"addressing_style": "path" if endpoint else "auto"},
        ),
    )


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:  # noqa: BLE001
        log.info("creating bucket %s", bucket)
        s3.create_bucket(Bucket=bucket)


class KafkaPublisher:
    """Kafka side of the split write, with an in-memory retry queue.

    Failed event IDs are also appended to a ledger file so the reconcile
    tool (reconcile.py) can be pointed at the affected hours.
    """

    def __init__(self, bootstrap: str, ledger_path: Path):
        self.bootstrap = bootstrap
        self.ledger_path = ledger_path
        self.retry_queue: deque[tuple[str, dict]] = deque(maxlen=1_000_000)
        self.sent = 0
        self.failed = 0
        self.producer = self._connect()

    def _connect(self) -> KafkaProducer:
        delay = 2
        while True:
            try:
                return KafkaProducer(
                    bootstrap_servers=self.bootstrap.split(","),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
                    acks="all",
                    linger_ms=20,
                    retries=5,
                    max_block_ms=5000,
                    request_timeout_ms=10000,
                )
            except KafkaError:  # broker not reachable yet
                log.warning("Kafka not reachable at %s, retrying in %ss", self.bootstrap, delay)
                time.sleep(delay)
                delay = min(delay * 2, 30)

    def _on_error(self, topic: str, record: dict, exc: Exception) -> None:
        self.failed += 1
        self.retry_queue.append((topic, record))
        record_id = record.get("event_id") or record.get("report_id")
        with open(self.ledger_path, "a", encoding="utf-8") as ledger:
            ledger.write(f"{datetime.now(timezone.utc).isoformat()}\t{topic}\t{record_id}\t{exc!r}\n")

    def _on_success(self, _metadata) -> None:
        self.sent += 1

    def send(self, topic: str, record: dict) -> None:
        try:
            future = self.producer.send(topic, key=record.get("station_id"), value=record)
            future.add_callback(self._on_success)
            future.add_errback(partial(self._on_error, topic, record))
        except KafkaError as exc:
            self._on_error(topic, record, exc)

    def retry_failed(self, limit: int = 5000) -> int:
        count = 0
        while self.retry_queue and count < limit:
            topic, record = self.retry_queue.popleft()
            self.send(topic, record)
            count += 1
        return count

    def close(self) -> None:
        try:
            self.producer.flush(timeout=30)
        finally:
            self.producer.close(timeout=10)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ChargePulse Lambda producer (split write)")
    p.add_argument("--rate", type=float, default=env("EVENT_RATE", 50.0, float),
                   help="events per second (default 50)")
    p.add_argument("--count", type=int, default=env("EVENT_COUNT", 0, int),
                   help="stop after N events; 0 = run forever")
    p.add_argument("--duplicate-rate", type=float, default=env("DUPLICATE_RATE", 0.02, float))
    p.add_argument("--late-rate", type=float, default=env("LATE_RATE", 0.03, float))
    p.add_argument("--late-min-seconds", type=float, default=env("LATE_MIN_SECONDS", 30.0, float))
    p.add_argument("--late-max-seconds", type=float, default=env("LATE_MAX_SECONDS", 240.0, float))
    p.add_argument("--invalid-rate", type=float, default=env("INVALID_RATE", 0.002, float))
    p.add_argument("--mismatch-rate", type=float, default=env("SETTLEMENT_MISMATCH_RATE", 0.05, float))
    p.add_argument("--seed", type=int, default=env("SEED", None, int))
    p.add_argument("--instance", default=env("PRODUCER_INSTANCE", "producer"),
                   help="name used in S3 object keys and the WAL directory")
    p.add_argument("--wal-dir", default=env("WAL_DIR", "/var/lib/chargepulse/wal"))
    p.add_argument("--bootstrap", default=env("KAFKA_BOOTSTRAP", "kafka:29092"))
    p.add_argument("--bucket", default=env("S3_BUCKET", None))
    args = p.parse_args(argv)
    if args.count < 0:
        p.error("--count must be >= 0")
    if not args.bucket:
        p.error("S3_BUCKET (or --bucket) is required")
    return args


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    config = SimulatorConfig(
        events_per_second=args.rate,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        late_min_seconds=args.late_min_seconds,
        late_max_seconds=args.late_max_seconds,
        invalid_rate=args.invalid_rate,
        settlement_mismatch_rate=args.mismatch_rate,
        seed=args.seed,
    )
    tariffs = load_tariffs()
    stations = station_ids(tariffs)
    simulator = ChargingSimulator(tariffs, config)

    wal_root = Path(args.wal_dir) / args.instance
    events_log = SegmentLog(wal_root / "events")
    settlements_log = SegmentLog(wal_root / "settlements")
    dead_log = SegmentLog(wal_root / "dead-letter")

    s3 = build_s3_client()
    if env("S3_CREATE_BUCKET", "false").lower() == "true":
        ensure_bucket(s3, args.bucket)
    archiver = S3Archiver(
        s3,
        args.bucket,
        args.instance,
        [
            ArchiveStream(events_log, RAW_EVENTS_PREFIX, "event_ts", EVENT_ARROW_SCHEMA),
            ArchiveStream(settlements_log, RAW_SETTLEMENTS_PREFIX, "reported_at", SETTLEMENT_ARROW_SCHEMA),
            ArchiveStream(dead_log, DEAD_LETTER_PREFIX, "ingest_ts", None),
        ],
        on_upload=lambda key, n: log.info("S3 archived %d records -> s3://%s/%s", n, args.bucket, key),
    )
    archiver.start(interval=5.0)

    kafka = KafkaPublisher(args.bootstrap, wal_root / "kafka_failed.log")

    stop = {"flag": False}

    def request_stop(signum, _frame):
        log.info("signal %s received, shutting down", signum)
        stop["flag"] = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    log.info(
        "producer '%s' started: %.1f ev/s, duplicates=%.3f late=%.3f (%s-%ss) invalid=%.3f, bucket=%s, kafka=%s",
        args.instance, args.rate, args.duplicate_rate, args.late_rate,
        args.late_min_seconds, args.late_max_seconds, args.invalid_rate, args.bucket, args.bootstrap,
    )

    interval = 1.0 / args.rate
    next_emit = time.perf_counter()
    last_housekeeping = last_report = time.monotonic()
    emitted = dead = 0
    window_start, window_emitted = time.monotonic(), 0

    while not stop["flag"] and (args.count == 0 or emitted < args.count):
        now = datetime.now(timezone.utc)
        record = simulator.next_event(now)
        errors = validate_event(record, stations, now)
        if errors:
            dead_log.append({**record, "errors": errors}, now)
            dead += 1
        else:
            events_log.append(record, now)
            kafka.send(EVENTS_TOPIC, record)

        for report in simulator.due_settlements(now):
            if validate_settlement(report):
                dead_log.append({**report, "errors": validate_settlement(report)}, now)
                continue
            settlements_log.append(report, now)
            kafka.send(SETTLEMENTS_TOPIC, report)

        emitted += 1
        window_emitted += 1

        mono = time.monotonic()
        if mono - last_housekeeping >= 1.0:
            for seg_log in (events_log, settlements_log, dead_log):
                seg_log.sync()
                seg_log.rotate(now)
            kafka.retry_failed()
            last_housekeeping = mono
        if mono - last_report >= 30.0:
            rate = window_emitted / (mono - window_start)
            log.info(
                "emitted=%d (%.1f/s) kafka_acked=%d kafka_failed=%d retry_queue=%d dead_letter=%d "
                "s3_objects=%d stats=%s",
                emitted, rate, kafka.sent, kafka.failed, len(kafka.retry_queue), dead,
                archiver.uploaded_objects, simulator.stats,
            )
            last_report, window_start, window_emitted = mono, mono, 0

        next_emit += interval
        sleep_for = next_emit - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)
        elif sleep_for < -1.0:  # fell far behind (e.g. Kafka blocked); don't burst
            next_emit = time.perf_counter()

    log.info("stopping: flushing Kafka and archiving remaining WAL segments")
    kafka.retry_failed(limit=len(kafka.retry_queue))
    kafka.close()
    for seg_log in (events_log, settlements_log, dead_log):
        seg_log.close()
    archiver.stop()
    try:
        archiver.drain()
    except Exception as exc:  # noqa: BLE001
        log.error("final S3 upload failed, segments kept in %s: %s", wal_root, exc)
        return 1
    log.info("done: emitted=%d kafka_acked=%d kafka_failed=%d dead_letter=%d",
             emitted, kafka.sent, kafka.failed, dead)
    return 0


if __name__ == "__main__":
    sys.exit(main())

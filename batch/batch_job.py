"""ChargePulse batch layer (PySpark): recompute hourly batch views from S3.

For every completed event-time hour (hour end + BATCH_GRACE_SECONDS has
passed) inside the look-back window, the job fingerprints the raw S3 objects
of that hour. If the hour was never processed, or its fingerprint changed
(late events or a repair object arrived), the hour is recomputed:

  raw/events/dt=../hr=..  --validate--dedup(event_id)--tariffs--> 1-min windows
  raw/settlements/..      --join session energy-----------------> reconciliation

Writes are idempotent. The S3 outputs use fixed object keys (overwrite).
In PostgreSQL, the hour's rows are deleted and re-inserted, and
`batch_runs` is upserted in the same transaction. Because of that
transaction, the serving boundary only moves once the hour's batch rows are
visible. Re-running an hour therefore replaces its previous result rather
than adding to it.

  spark-submit batch_job.py                  # scheduled mode
  spark-submit batch_job.py --hour 2026-11-26T17   # force one hour
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from chargepulse_common import db
from chargepulse_common.contract import (
    BATCH_PREFIX,
    DEAD_LETTER_PREFIX,
    META_PREFIX,
    RAW_EVENTS_PREFIX,
    RAW_SETTLEMENTS_PREFIX,
    hour_partition,
    hour_start,
)
from chargepulse_common.tariffs import load_tariffs, station_ids
from chargepulse_common.transforms import (
    AGGREGATE_COLUMNS,
    EVENT_PARQUET_SCHEMA,
    SETTLEMENT_PARQUET_SCHEMA,
    batch_aggregates,
    batch_unique_events,
    normalize_events,
    session_reconciliation,
    tariffs_dataframe,
    valid_event_condition,
)

BUCKET = os.environ["S3_BUCKET"]
GRACE = timedelta(seconds=int(os.getenv("BATCH_GRACE_SECONDS", "300")))
LOOKBACK_HOURS = int(os.getenv("BATCH_LOOKBACK_HOURS", "48"))
LATE_THRESHOLD_SECONDS = int(os.getenv("LATE_THRESHOLD_SECONDS", "120"))
SPEED_RETENTION_HOURS = int(os.getenv("SPEED_RETENTION_HOURS", "48"))
WORK_DIR = Path(os.getenv("BATCH_WORK_DIR", "/var/lib/chargepulse/batch-work"))
TARIFF_PATH = os.environ["TARIFF_PATH"]

RECON_COLUMNS = (
    "session_id", "report_id", "station_id", "tariff_id", "session_end_ts", "reported_at",
    "event_count", "calc_energy_kwh", "calc_amount", "billed_energy_kwh", "billed_amount",
    "energy_diff_kwh", "amount_diff", "reconciliation_status", "settlement_status",
)


def log(msg: str) -> None:
    print(f"[batch] {datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


def s3_client():
    endpoint = os.getenv("S3_ENDPOINT_URL") or None
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(retries={"max_attempts": 5, "mode": "standard"},
                      s3={"addressing_style": "path" if endpoint else "auto"}),
    )


def list_objects(s3, prefix: str) -> list[dict]:
    out = []
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
            out.extend(page.get("Contents", []))
    except s3.exceptions.NoSuchBucket:
        return []  # producer has not created/written the bucket yet
    return out


class HourInputs:
    def __init__(self, s3, hour: datetime):
        part = hour_partition(hour)
        prev = hour_partition(hour - timedelta(hours=1))
        self.hour = hour
        self.events = list_objects(s3, f"{RAW_EVENTS_PREFIX}/{part}/")
        self.prev_events = list_objects(s3, f"{RAW_EVENTS_PREFIX}/{prev}/")
        self.settlements = list_objects(s3, f"{RAW_SETTLEMENTS_PREFIX}/{part}/")
        self.dead_letter = list_objects(s3, f"{DEAD_LETTER_PREFIX}/{part}/")

    @property
    def fingerprint(self) -> str:
        digest = hashlib.sha256()
        for obj in sorted(self.events + self.settlements + self.dead_letter, key=lambda o: o["Key"]):
            digest.update(f"{obj['Key']}|{obj['ETag']}|{obj['Size']}\n".encode())
        return digest.hexdigest()


def download(s3, objects: list[dict], target: Path) -> Path:
    target.mkdir(parents=True, exist_ok=True)
    for obj in objects:
        s3.download_file(BUCKET, obj["Key"], str(target / obj["Key"].replace("/", "__")))
    return target


def count_dead_letter(s3, objects: list[dict]) -> int:
    total = 0
    for obj in objects:
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        total += sum(1 for line in body.splitlines() if line.strip())
    return total


def upload_single_parquet(s3, df: DataFrame, local_dir: Path, key: str) -> None:
    df.coalesce(1).write.mode("overwrite").parquet(str(local_dir))
    part = next(local_dir.glob("part-*.parquet"))
    s3.upload_file(str(part), BUCKET, key)


def read_events(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.schema(EVENT_PARQUET_SCHEMA).parquet(str(path))


def process_hour(spark, s3, conn, inputs: HourInputs, tariffs_df, stations) -> dict:
    hour = inputs.hour
    hour_end = hour + timedelta(hours=1)
    started = datetime.now(timezone.utc)
    work = WORK_DIR / hour.strftime("%Y%m%dT%H")
    shutil.rmtree(work, ignore_errors=True)

    # ---- batch view: 1-minute windows per station -----------------------
    events_raw = read_events(spark, download(s3, inputs.events, work / "events"))
    events = normalize_events(events_raw).filter(
        (F.col("event_time") >= F.lit(hour)) & (F.col("event_time") < F.lit(hour_end))
    ).cache()
    raw_rows = events_raw.count()
    valid_rows = events.filter(valid_event_condition(stations)).count()
    in_hour = events_raw.filter(
        (F.col("event_ts") >= F.lit(hour)) & (F.col("event_ts") < F.lit(hour_end))
    )
    unique = batch_unique_events(in_hour, stations).cache()
    unique_rows = unique.count()
    late_rows = unique.filter(
        F.col("ingest_time").cast("long") - F.col("event_time").cast("long") > LATE_THRESHOLD_SECONDS
    ).count()
    aggregates = batch_aggregates(unique, tariffs_df)
    agg_rows = [tuple(r) for r in aggregates.select(*AGGREGATE_COLUMNS).collect()]

    # ---- session reconciliation for settlements reported in this hour -----
    recon_rows = []
    if inputs.settlements:
        settlements = spark.read.schema(SETTLEMENT_PARQUET_SCHEMA).parquet(
            str(download(s3, inputs.settlements, work / "settlements"))
        )
        session_events = unique
        if inputs.prev_events:
            prev = normalize_events(read_events(spark, download(s3, inputs.prev_events, work / "prev")))
            session_events = (
                prev.filter(valid_event_condition(stations))
                .unionByName(unique)
                .dropDuplicates(["event_id"])
            )
        recon = session_reconciliation(session_events, settlements, tariffs_df)
        recon_rows = [tuple(r) for r in recon.select(*RECON_COLUMNS).collect()]
        upload_single_parquet(
            s3, recon, work / "out-recon",
            f"{BATCH_PREFIX}/session_reconciliation/{hour_partition(hour)}/part-00000.parquet",
        )

    # ---- authoritative output to S3 (fixed key -> overwrite) -------------
    upload_single_parquet(
        s3, aggregates, work / "out-agg",
        f"{BATCH_PREFIX}/station_minute/{hour_partition(hour)}/part-00000.parquet",
    )
    dead_letter_rows = count_dead_letter(s3, inputs.dead_letter)

    stats = {
        "hour_start": hour,
        "hour_end": hour_end,
        "fingerprint": inputs.fingerprint,
        "raw_files": len(inputs.events),
        "raw_rows": raw_rows,
        "invalid_rows": raw_rows - valid_rows,
        "dead_letter_rows": dead_letter_rows,
        "duplicates_removed": valid_rows - unique_rows,
        "unique_events": unique_rows,
        "late_events": late_rows,
        "settlements": len(recon_rows),
        "aggregate_rows": len(agg_rows),
        "started_at": started,
    }

    # ---- serving layer: replace the hour + advance boundary atomically ----
    placeholders = ", ".join(["%s"] * len(AGGREGATE_COLUMNS))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "DELETE FROM batch_view WHERE window_start >= %s AND window_start < %s",
            (hour, hour_end),
        )
        cur.executemany(
            f"INSERT INTO batch_view ({', '.join(AGGREGATE_COLUMNS)}) VALUES ({placeholders})",
            agg_rows,
        )
        cur.execute("DELETE FROM session_reconciliation WHERE settlement_hour = %s", (hour,))
        if recon_rows:
            cur.executemany(
                f"INSERT INTO session_reconciliation ({', '.join(RECON_COLUMNS)}, settlement_hour) "
                f"VALUES ({', '.join(['%s'] * len(RECON_COLUMNS))}, %s) "
                "ON CONFLICT (session_id) DO UPDATE SET "
                + ", ".join(f"{c} = EXCLUDED.{c}" for c in RECON_COLUMNS[1:])
                + ", settlement_hour = EXCLUDED.settlement_hour",
                [row + (hour,) for row in recon_rows],
            )
        cur.execute(
            """
            INSERT INTO batch_runs (hour_start, hour_end, fingerprint, raw_files, raw_rows,
                invalid_rows, dead_letter_rows, duplicates_removed, unique_events, late_events,
                settlements, aggregate_rows, started_at, finished_at, run_count)
            VALUES (%(hour_start)s, %(hour_end)s, %(fingerprint)s, %(raw_files)s, %(raw_rows)s,
                %(invalid_rows)s, %(dead_letter_rows)s, %(duplicates_removed)s, %(unique_events)s,
                %(late_events)s, %(settlements)s, %(aggregate_rows)s, %(started_at)s, now(), 1)
            ON CONFLICT (hour_start) DO UPDATE SET
                fingerprint = EXCLUDED.fingerprint, raw_files = EXCLUDED.raw_files,
                raw_rows = EXCLUDED.raw_rows, invalid_rows = EXCLUDED.invalid_rows,
                dead_letter_rows = EXCLUDED.dead_letter_rows,
                duplicates_removed = EXCLUDED.duplicates_removed,
                unique_events = EXCLUDED.unique_events, late_events = EXCLUDED.late_events,
                settlements = EXCLUDED.settlements, aggregate_rows = EXCLUDED.aggregate_rows,
                started_at = EXCLUDED.started_at, finished_at = now(),
                run_count = batch_runs.run_count + 1
            """,
            stats,
        )

    manifest = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in stats.items()}
    manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{META_PREFIX}/batch_runs/{hour_partition(hour)}/manifest.json",
        Body=json.dumps(manifest, indent=2).encode(),
    )
    events.unpersist()
    unique.unpersist()
    shutil.rmtree(work, ignore_errors=True)
    return stats


def processed_fingerprints(conn) -> dict[datetime, str]:
    with conn.cursor() as cur:
        cur.execute("SELECT hour_start, fingerprint FROM batch_runs")
        return {row[0]: row[1] for row in cur.fetchall()}


def prune_speed_view(conn) -> int:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM speed_view
            WHERE window_start < (SELECT batch_boundary FROM serving_boundary)
                                 - make_interval(hours => %s)
            """,
            (SPEED_RETENTION_HOURS,),
        )
        return cur.rowcount


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="ChargePulse batch layer")
    parser.add_argument("--hour", action="append", default=[],
                        help="force recompute of a UTC hour, e.g. 2026-11-26T17 (repeatable)")
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    last_complete = hour_start(now - GRACE) - timedelta(hours=1)

    forced = [datetime.strptime(h, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc) for h in args.hour]
    for hour in forced:
        if hour > last_complete:
            log(f"refusing {hour:%Y-%m-%dT%H}: hour not complete yet (grace {GRACE})")
            return 2

    s3 = s3_client()
    conn = db.connect()
    conn.autocommit = True
    done = processed_fingerprints(conn)

    if forced:
        candidates = [(h, HourInputs(s3, h)) for h in sorted(forced)]
    else:
        candidates = []
        for i in range(LOOKBACK_HOURS - 1, -1, -1):
            hour = last_complete - timedelta(hours=i)
            inputs = HourInputs(s3, hour)
            if not inputs.events:
                continue
            if done.get(hour) != inputs.fingerprint:
                candidates.append((hour, inputs))

    if not candidates:
        log(f"nothing to do; completed hours up to {last_complete:%Y-%m-%dT%H} are current")
        conn.close()
        return 0

    spark = (
        SparkSession.builder.appName("chargepulse-batch-layer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    tariffs_df = tariffs_dataframe(spark, TARIFF_PATH).cache()
    stations = station_ids(load_tariffs(TARIFF_PATH))

    try:
        for hour, inputs in candidates:
            reason = "forced" if forced else ("new" if hour not in done else "raw data changed")
            t0 = time.monotonic()
            stats = process_hour(spark, s3, conn, inputs, tariffs_df, stations)
            log(
                f"hour {hour:%Y-%m-%dT%H} ({reason}) done in {time.monotonic() - t0:.1f}s: "
                f"files={stats['raw_files']} raw={stats['raw_rows']} unique={stats['unique_events']} "
                f"duplicates_removed={stats['duplicates_removed']} invalid={stats['invalid_rows']} "
                f"dead_letter={stats['dead_letter_rows']} late={stats['late_events']} "
                f"settlements={stats['settlements']} windows={stats['aggregate_rows']}"
            )
        pruned = prune_speed_view(conn)
        if pruned:
            log(f"pruned {pruned} speed_view rows older than retention")
    except Exception as exc:
        # Stop at the first failure so the boundary never skips an hour.
        log(f"FAILED: {exc!r}")
        raise
    finally:
        conn.close()
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

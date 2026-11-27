"""ChargePulse speed layer (Spark Structured Streaming).

Kafka `charge-events` -> validate -> watermark + de-duplicate -> tariff
pricing -> 1-minute tumbling windows per station -> upsert into the
PostgreSQL `speed_view` table.

* Output mode `update`: each micro-batch emits the windows whose totals
  changed, and the sink upserts on (window_start, station_id). Replaying a
  micro-batch after a crash rewrites the same values, so the sink is
  idempotent.
* Events later than SPEED_WATERMARK are dropped here. They are still in the
  S3 master dataset, and the next batch run for that hour includes them.
* State and Kafka offsets are checkpointed to SPEED_CHECKPOINT_DIR.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from chargepulse_common import db
from chargepulse_common.contract import EVENTS_TOPIC
from chargepulse_common.tariffs import load_tariffs, station_ids
from chargepulse_common.transforms import (
    AGGREGATE_COLUMNS,
    EVENT_JSON_SCHEMA,
    speed_aggregates,
    tariffs_dataframe,
)

log = logging.getLogger("chargepulse.speed")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
WATERMARK = os.getenv("SPEED_WATERMARK", "2 minutes")
TRIGGER = os.getenv("SPEED_TRIGGER", "10 seconds")
CHECKPOINT_DIR = os.getenv("SPEED_CHECKPOINT_DIR", "/var/lib/chargepulse/checkpoints/speed-v1")
STARTING_OFFSETS = os.getenv("SPEED_STARTING_OFFSETS", "earliest")
TARIFF_PATH = os.environ["TARIFF_PATH"]

UPSERT_SQL = f"""
INSERT INTO speed_view ({", ".join(AGGREGATE_COLUMNS)}, updated_at)
VALUES ({", ".join(["%s"] * len(AGGREGATE_COLUMNS))}, now())
ON CONFLICT (window_start, station_id) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    event_count = EXCLUDED.event_count,
    session_count = EXCLUDED.session_count,
    total_energy_kwh = EXCLUDED.total_energy_kwh,
    est_energy_cost = EXCLUDED.est_energy_cost,
    avg_power_kw = EXCLUDED.avg_power_kw,
    max_power_kw = EXCLUDED.max_power_kw,
    updated_at = now()
"""


def build_query(spark: SparkSession, stations: list[str]) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", EVENTS_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 200000)
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_JSON_SCHEMA).alias("e")
    ).select("e.*")

    return speed_aggregates(parsed, tariffs_dataframe(spark, TARIFF_PATH), stations, WATERMARK)


def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
    rows = batch_df.select(*AGGREGATE_COLUMNS).collect()
    if not rows:
        return
    with db.connect() as conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, [tuple(r) for r in rows])
    print(f"[speed] batch {batch_id}: upserted {len(rows)} station-minute windows", flush=True)


def main() -> None:
    spark = (
        SparkSession.builder.appName("chargepulse-speed-layer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stations = station_ids(load_tariffs(TARIFF_PATH))
    aggregates = build_query(spark, stations)

    query = (
        aggregates.writeStream.outputMode("update")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER)
        .queryName("speed_view")
        .start()
    )
    print(
        f"[speed] running: topic={EVENTS_TOPIC} watermark={WATERMARK} trigger={TRIGGER} "
        f"checkpoint={CHECKPOINT_DIR}",
        flush=True,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

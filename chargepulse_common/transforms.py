"""Spark transformations shared by the speed layer and the batch layer.

Both layers run the same pipeline:

    normalize_events -> filter(valid_event_condition) -> <dedup>
        -> enrich_with_tariffs -> aggregate_station_minute

The only difference is the dedup step: the batch layer removes duplicate
event_ids across the whole hour (`dropDuplicates`), while the speed layer can
only remember event_ids for as long as its watermark allows
(`dropDuplicatesWithinWatermark`). Any formula change happens here, once.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .contract import (
    EVENT_FIELDS,
    MAX_FUTURE_SKEW,
    SESSION_STATUSES,
    SETTLEMENT_FIELDS,
)

WINDOW_DURATION = "1 minute"

_SPARK_TYPES = {
    "string": T.StringType(),
    "double": T.DoubleType(),
    "int": T.IntegerType(),
    "timestamp": T.TimestampType(),
}


def _schema(fields, timestamps_as_string: bool) -> T.StructType:
    out = []
    for name, logical, _required in fields:
        if logical == "timestamp" and timestamps_as_string:
            spark_type = T.StringType()
        else:
            spark_type = _SPARK_TYPES[logical]
        out.append(T.StructField(name, spark_type, True))
    return T.StructType(out)


# Kafka carries JSON with ISO-8601 strings; S3 Parquet stores real timestamps.
EVENT_JSON_SCHEMA = _schema(EVENT_FIELDS, timestamps_as_string=True)
EVENT_PARQUET_SCHEMA = _schema(EVENT_FIELDS, timestamps_as_string=False)
SETTLEMENT_PARQUET_SCHEMA = _schema(SETTLEMENT_FIELDS, timestamps_as_string=False)

AGGREGATE_COLUMNS = (
    "window_start",
    "window_end",
    "station_id",
    "event_count",
    "session_count",
    "total_energy_kwh",
    "est_energy_cost",
    "avg_power_kw",
    "max_power_kw",
)


def normalize_events(df: DataFrame) -> DataFrame:
    """Cast to the contract types. Works on JSON (string ts) and Parquet input.

    `try_cast` turns malformed values into NULL (rejected by
    `valid_event_condition`) instead of failing the job: under Spark 4's ANSI
    mode a plain cast of one bad record would crash-loop the stream.
    """
    return df.select(
        F.col("event_id").try_cast("string").alias("event_id"),
        F.col("event_ts").try_cast("timestamp").alias("event_time"),
        F.col("ingest_ts").try_cast("timestamp").alias("ingest_time"),
        F.col("session_id").try_cast("string").alias("session_id"),
        F.col("station_id").try_cast("string").alias("station_id"),
        F.col("connector_id").try_cast("string").alias("connector_id"),
        F.col("vehicle_id").try_cast("string").alias("vehicle_id"),
        F.col("energy_delta_kwh").try_cast("double").alias("energy_delta_kwh"),
        F.col("power_kw").try_cast("double").alias("power_kw"),
        F.col("session_status").try_cast("string").alias("session_status"),
    )


def valid_event_condition(known_stations: list[str]) -> Column:
    """Spark twin of `contract.validate_event` (applied to normalized events)."""
    skew_seconds = int(MAX_FUTURE_SKEW.total_seconds())
    return (
        F.col("event_id").isNotNull()
        & (F.col("event_id") != "")
        & F.col("event_time").isNotNull()
        & F.col("ingest_time").isNotNull()
        & (
            F.col("event_time")
            <= F.current_timestamp() + F.expr(f"INTERVAL {skew_seconds} SECONDS")
        )
        & F.col("session_id").isNotNull()
        & F.col("station_id").isin(known_stations)
        & F.col("energy_delta_kwh").isNotNull()
        & (F.col("energy_delta_kwh") >= 0)
        & F.col("power_kw").isNotNull()
        & (F.col("power_kw") >= 0)
        & F.col("session_status").isin(list(SESSION_STATUSES))
    )


def tariffs_dataframe(spark: SparkSession, tariff_path: str) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("station_id", T.StringType()),
            T.StructField("tariff_id", T.StringType()),
            T.StructField("price_per_kwh", T.DoubleType()),
            T.StructField("service_fee", T.DoubleType()),
            T.StructField("alert_threshold_kwh", T.DoubleType()),
            T.StructField("effective_from", T.StringType()),
            T.StructField("effective_to", T.StringType()),
        ]
    )
    return (
        spark.read.option("header", True)
        .schema(schema)
        .csv(tariff_path)
        .withColumn("effective_from", F.col("effective_from").cast("timestamp"))
        .withColumn("effective_to", F.col("effective_to").cast("timestamp"))
        .withColumnRenamed("station_id", "t_station_id")
    )


def enrich_with_tariffs(events: DataFrame, tariffs: DataFrame) -> DataFrame:
    """Attach the tariff in force at event time and price the energy delta.

    The service fee is charged once per session, so it is applied in
    session reconciliation, not per event.
    """
    joined = events.join(
        F.broadcast(tariffs),
        (events.station_id == tariffs.t_station_id)
        & (events.event_time >= tariffs.effective_from)
        & (events.event_time < tariffs.effective_to),
        "left",
    )
    return joined.withColumn(
        "est_energy_cost", F.col("energy_delta_kwh") * F.col("price_per_kwh")
    ).drop("t_station_id", "effective_from", "effective_to")


def aggregate_station_minute(events: DataFrame) -> DataFrame:
    """1-minute tumbling event-time windows per station (speed and batch views)."""
    return (
        events.groupBy(F.window("event_time", WINDOW_DURATION), "station_id")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.approx_count_distinct("session_id").alias("session_count"),
            F.sum("energy_delta_kwh").alias("total_energy_kwh"),
            F.sum("est_energy_cost").alias("est_energy_cost"),
            F.avg("power_kw").alias("avg_power_kw"),
            F.max("power_kw").alias("max_power_kw"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            *AGGREGATE_COLUMNS[2:],
        )
    )


def speed_aggregates(
    parsed: DataFrame, tariffs: DataFrame, stations: list[str], watermark: str
) -> DataFrame:
    """Speed-layer pipeline over a streaming DataFrame of contract records."""
    events = normalize_events(parsed).filter(valid_event_condition(stations))
    deduplicated = events.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(
        ["event_id"]
    )
    return aggregate_station_minute(enrich_with_tariffs(deduplicated, tariffs))


def batch_unique_events(raw: DataFrame, stations: list[str]) -> DataFrame:
    """Batch-layer validation + global de-duplication on event_id."""
    return (
        normalize_events(raw)
        .filter(valid_event_condition(stations))
        .dropDuplicates(["event_id"])
    )


def batch_aggregates(unique_events: DataFrame, tariffs: DataFrame) -> DataFrame:
    return aggregate_station_minute(enrich_with_tariffs(unique_events, tariffs))


def session_reconciliation(
    events: DataFrame, settlements: DataFrame, tariffs: DataFrame
) -> DataFrame:
    """Compare calculated session energy/cost with the reported settlement.

    `events` must already be validated and de-duplicated.
    """
    session_energy = events.groupBy("session_id").agg(
        F.sum("energy_delta_kwh").alias("calc_energy_kwh"),
        F.count(F.lit(1)).alias("event_count"),
    )
    reports = settlements.dropDuplicates(["report_id"]).select(
        "report_id",
        "session_id",
        F.col("station_id"),
        "billed_energy_kwh",
        "billed_amount",
        "settlement_status",
        F.col("session_end_ts").cast("timestamp").alias("session_end_ts"),
        F.col("reported_at").cast("timestamp").alias("reported_at"),
    )
    priced = reports.join(
        F.broadcast(tariffs),
        (reports.station_id == tariffs.t_station_id)
        & (reports.session_end_ts >= tariffs.effective_from)
        & (reports.session_end_ts < tariffs.effective_to),
        "left",
    )
    joined = priced.join(session_energy, "session_id", "left").fillna(
        {"calc_energy_kwh": 0.0, "event_count": 0}
    )
    calc_cost = F.col("calc_energy_kwh") * F.col("price_per_kwh") + F.col("service_fee")
    energy_diff = F.col("billed_energy_kwh") - F.col("calc_energy_kwh")
    return joined.select(
        "session_id",
        "report_id",
        "station_id",
        "tariff_id",
        "session_end_ts",
        "reported_at",
        "event_count",
        F.round("calc_energy_kwh", 4).alias("calc_energy_kwh"),
        F.round(calc_cost, 4).alias("calc_amount"),
        "billed_energy_kwh",
        "billed_amount",
        F.round(energy_diff, 4).alias("energy_diff_kwh"),
        F.round(F.col("billed_amount") - calc_cost, 4).alias("amount_diff"),
        F.when(F.col("event_count") == 0, F.lit("missing_events"))
        .when(
            F.abs(energy_diff) > F.greatest(F.lit(0.005), F.col("calc_energy_kwh") * 0.02),
            F.lit("mismatch"),
        )
        .otherwise(F.lit("matched"))
        .alias("reconciliation_status"),
        "settlement_status",
    )

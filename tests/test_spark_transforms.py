"""Spark tests: batch and speed pipelines must agree on the same input.

Needs Java + PySpark, so run them inside the Spark image:

  docker compose run --rm --no-deps batch-scheduler python3 -m pytest -q tests/test_spark_transforms.py
"""

import json
import os
import shutil
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")
if not (os.getenv("JAVA_HOME") or shutil.which("java")):
    pytest.skip("Java is required for Spark tests", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from chargepulse_common.contract import format_ts, validate_event  # noqa: E402
from chargepulse_common.tariffs import DEFAULT_TARIFF_PATH, load_tariffs, station_ids  # noqa: E402
from chargepulse_common.transforms import (  # noqa: E402
    AGGREGATE_COLUMNS,
    EVENT_JSON_SCHEMA,
    SETTLEMENT_PARQUET_SCHEMA,
    batch_aggregates,
    batch_unique_events,
    normalize_events,
    session_reconciliation,
    speed_aggregates,
    tariffs_dataframe,
    valid_event_condition,
)
from simulator import ChargingSimulator, SimulatorConfig  # noqa: E402

TARIFFS = load_tariffs()
STATIONS = station_ids(TARIFFS)
T0 = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=30)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("chargepulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def tariffs(spark):
    return tariffs_dataframe(spark, str(DEFAULT_TARIFF_PATH)).cache()


def simulated_events(n=3000, **cfg):
    sim = ChargingSimulator(TARIFFS, SimulatorConfig(seed=11, **cfg))
    return sim, [sim.next_event(T0 + timedelta(seconds=i * 0.02)) for i in range(n)]


def assert_same_aggregates(left, right):
    """Exact on keys and counts; floats within 1e-9 relative (summation order)."""
    left = sorted(tuple(r) for r in left)
    right = sorted(tuple(r) for r in right)
    assert len(left) == len(right)
    for a, b in zip(left, right):
        for x, y in zip(a, b):
            if isinstance(x, float):
                assert x == pytest.approx(y, rel=1e-9), (a, b)
            else:
                assert x == y, (a, b)


def test_speed_and_batch_produce_identical_aggregates(spark, tariffs, tmp_path):
    # duplicates on, no lateness beyond the watermark -> both layers must agree
    _, events = simulated_events(duplicate_rate=0.1, late_rate=0, invalid_rate=0.01)
    source = tmp_path / "stream"
    source.mkdir()
    (source / "events.json").write_text("\n".join(json.dumps(e) for e in events))

    # speed path: real streaming query over the same records
    stream = spark.readStream.schema(EVENT_JSON_SCHEMA).json(str(source))
    query = (
        speed_aggregates(stream, tariffs, STATIONS, "10 minutes")
        .writeStream.outputMode("update").format("memory").queryName("speed_test")
        .option("checkpointLocation", str(tmp_path / "chk")).start()
    )
    query.processAllAvailable()
    query.stop()
    speed_rows = spark.sql("SELECT * FROM speed_test").select(*AGGREGATE_COLUMNS).collect()

    # batch path
    raw = spark.read.schema(EVENT_JSON_SCHEMA).json(str(source))
    batch_rows = batch_aggregates(batch_unique_events(raw, STATIONS), tariffs).select(
        *AGGREGATE_COLUMNS).collect()

    assert speed_rows, "speed layer produced no output"
    assert_same_aggregates(speed_rows, batch_rows)


def test_batch_dedup_removes_every_duplicate(spark):
    sim, events = simulated_events(duplicate_rate=0.2, late_rate=0, invalid_rate=0)
    raw = spark.read.schema(EVENT_JSON_SCHEMA).json(
        spark.sparkContext.parallelize([json.dumps(e) for e in events]))
    unique = batch_unique_events(raw, STATIONS)
    assert unique.count() == len(events) - sim.stats["duplicates"]


def test_spark_validation_matches_python_contract(spark):
    _, events = simulated_events(n=2000, duplicate_rate=0, late_rate=0.1, invalid_rate=0.2)
    check_time = datetime.now(timezone.utc)
    python_valid = {e["event_id"] for e in events if not validate_event(e, STATIONS, check_time)}
    raw = spark.read.schema(EVENT_JSON_SCHEMA).json(
        spark.sparkContext.parallelize([json.dumps(e) for e in events]))
    spark_valid = {
        r.event_id for r in normalize_events(raw).filter(valid_event_condition(STATIONS)).collect()
    }
    assert spark_valid == python_valid


def test_session_reconciliation_flags_mismatches(spark, tariffs):
    sim, events = simulated_events(n=4000, duplicate_rate=0.05, late_rate=0, invalid_rate=0,
                                   settlement_mismatch_rate=0.3)
    reports = sim.due_settlements(T0 + timedelta(hours=1))
    raw = spark.read.schema(EVENT_JSON_SCHEMA).json(
        spark.sparkContext.parallelize([json.dumps(e) for e in events]))
    unique = batch_unique_events(raw, STATIONS)
    for r in reports:
        for f in ("session_end_ts", "reported_at"):
            r[f] = datetime.fromisoformat(r[f])
    settlements = spark.createDataFrame(
        [tuple(r[f.name] for f in SETTLEMENT_PARQUET_SCHEMA.fields) for r in reports],
        SETTLEMENT_PARQUET_SCHEMA,
    )
    result = {r.session_id: r for r in session_reconciliation(unique, settlements, tariffs).collect()}
    assert len(result) == len(reports)
    for report in reports:
        row = result[report["session_id"]]
        expected = "mismatch" if report["settlement_status"] == "disputed" else "matched"
        assert row.reconciliation_status == expected, (report, row)
    assert any(r.reconciliation_status == "mismatch" for r in result.values())

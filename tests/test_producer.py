import io
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from chargepulse_common.contract import parse_ts, validate_event
from chargepulse_common.tariffs import load_tariffs, station_ids
from simulator import ChargingSimulator, SimulatorConfig
from wal import EVENT_ARROW_SCHEMA, ArchiveStream, S3Archiver, SegmentLog

TARIFFS = load_tariffs()
STATIONS = station_ids(TARIFFS)
T0 = datetime(2026, 11, 26, 17, 58, 30, tzinfo=timezone.utc)
CHECK_AT = T0 + timedelta(minutes=5)


def simulate(n, **cfg):
    sim = ChargingSimulator(TARIFFS, SimulatorConfig(seed=7, **cfg))
    events = [sim.next_event(T0 + timedelta(seconds=i * 0.02)) for i in range(n)]
    return sim, events


def test_clean_stream_is_valid_unique_and_on_time():
    _, events = simulate(2000, duplicate_rate=0, late_rate=0, invalid_rate=0)
    assert all(validate_event(e, STATIONS, CHECK_AT) == [] for e in events)
    assert len({e["event_id"] for e in events}) == len(events)
    assert all(e["event_ts"] == e["ingest_ts"] for e in events)


def test_duplicate_switch_resends_existing_event_ids():
    sim, events = simulate(2000, duplicate_rate=0.2, late_rate=0, invalid_rate=0)
    unique = len({e["event_id"] for e in events})
    assert len(events) - unique == sim.stats["duplicates"] > 300


def test_late_switch_pushes_event_time_back():
    sim, events = simulate(2000, duplicate_rate=0, late_rate=0.5, late_min_seconds=60,
                           late_max_seconds=120, invalid_rate=0)
    lags = [(parse_ts(e["ingest_ts"]) - parse_ts(e["event_ts"])).total_seconds() for e in events]
    late = [lag for lag in lags if lag > 0]
    assert len(late) == sim.stats["late"] > 800
    assert all(60 <= lag <= 120 for lag in late)


def test_invalid_switch_produces_records_that_fail_validation():
    sim, events = simulate(2000, duplicate_rate=0, late_rate=0, invalid_rate=0.1)
    invalid = [e for e in events if validate_event(e, STATIONS, CHECK_AT)]
    assert len(invalid) == sim.stats["invalid"] > 100


def test_sessions_complete_and_settle_with_tariff_amounts():
    sim, events = simulate(3000, duplicate_rate=0, late_rate=0, invalid_rate=0)
    completed = {e["session_id"] for e in events if e["session_status"] == "completed"}
    reports = sim.due_settlements(T0 + timedelta(minutes=10))
    assert completed and {r["session_id"] for r in reports} == completed
    for report in reports:
        assert report["billed_amount"] > 0 and report["reported_at"]


@pytest.mark.parametrize(
    "overrides",
    [{"events_per_second": 0}, {"events_per_second": -1}, {"duplicate_rate": 1.5},
     {"late_rate": -0.1}, {"late_min_seconds": 10, "late_max_seconds": 5}],
)
def test_invalid_configuration_is_rejected(overrides):
    with pytest.raises(ValueError):
        SimulatorConfig(**overrides).validate()


class FakeS3:
    def __init__(self, fail=False):
        self.objects, self.fail = {}, fail

    def put_object(self, Bucket, Key, Body):
        if self.fail:
            raise ConnectionError("S3 down")
        self.objects[Key] = Body


def test_wal_segments_are_archived_by_event_hour(tmp_path):
    log = SegmentLog(tmp_path / "events")
    _, events = simulate(300, duplicate_rate=0, late_rate=0, invalid_rate=0)
    late = dict(events[0], event_id="late-1", event_ts="2026-11-26T16:59:59.000+00:00")
    for e in events + [late]:
        log.append(e, T0)
    log.close()

    s3 = FakeS3()
    archiver = S3Archiver(s3, "bucket", "producer",
                          [ArchiveStream(log, "raw/events", "event_ts", EVENT_ARROW_SCHEMA)])
    assert archiver.drain() == 301
    assert sorted(s3.objects) == [
        "raw/events/dt=2026-11-26/hr=16/producer-20261126T1758.parquet",
        "raw/events/dt=2026-11-26/hr=17/producer-20261126T1758.parquet",
    ]
    key = "raw/events/dt=2026-11-26/hr=17/producer-20261126T1758.parquet"
    table = pq.read_table(io.BytesIO(s3.objects[key]))
    assert table.num_rows == 300 and table.schema.equals(EVENT_ARROW_SCHEMA)
    assert log.closed_segments() == []


def test_s3_failure_keeps_segment_and_retry_uses_same_key(tmp_path):
    log = SegmentLog(tmp_path / "events")
    _, events = simulate(10, duplicate_rate=0, late_rate=0, invalid_rate=0)
    for e in events:
        log.append(e, T0)
    log.close()
    stream = ArchiveStream(log, "raw/events", "event_ts", EVENT_ARROW_SCHEMA)
    with pytest.raises(ConnectionError):
        S3Archiver(FakeS3(fail=True), "bucket", "producer", [stream]).drain()
    assert len(log.closed_segments()) == 1  # still on local disk
    s3 = FakeS3()
    S3Archiver(s3, "bucket", "producer", [stream]).drain()
    assert list(s3.objects) == ["raw/events/dt=2026-11-26/hr=17/producer-20261126T1758.parquet"]
    assert log.closed_segments() == []


def test_unclosed_segment_is_recovered_after_crash(tmp_path):
    log = SegmentLog(tmp_path / "events")
    log.append({"event_id": "x", "event_ts": "2026-11-26T17:00:00+00:00"}, T0)
    log.sync()
    log._handle.close()  # simulate a crash: the OS releases the file, it is never renamed
    recovered = SegmentLog(tmp_path / "events")
    assert [p.name for p in recovered.closed_segments()] == ["20261126T1758.jsonl"]


def test_segments_rotate_each_minute(tmp_path):
    log = SegmentLog(tmp_path / "events")
    log.append({"a": 1}, T0)
    log.append({"a": 2}, T0 + timedelta(minutes=1))
    assert [p.name for p in log.closed_segments()] == ["20261126T1758.jsonl"]

from datetime import datetime, timedelta, timezone

import pytest

from chargepulse_common.contract import (
    EVENT_FIELD_NAMES,
    format_ts,
    hour_partition,
    parse_ts,
    validate_event,
    validate_settlement,
)
from chargepulse_common.tariffs import load_tariffs, session_cost, station_ids, tariff_at

NOW = datetime(2026, 11, 26, 17, 30, tzinfo=timezone.utc)
STATIONS = ["ST001", "ST002"]


def good_event(**overrides):
    event = {
        "event_id": "e-1",
        "event_ts": format_ts(NOW),
        "ingest_ts": format_ts(NOW),
        "session_id": "s-1",
        "station_id": "ST001",
        "connector_id": "C01",
        "vehicle_id": "EV1234",
        "energy_delta_kwh": 0.02,
        "power_kw": 50.0,
        "session_status": "charging",
        "schema_version": 1,
    }
    event.update(overrides)
    return event


def test_contract_has_required_fields():
    for field in ("event_id", "event_ts", "station_id", "energy_delta_kwh"):
        assert field in EVENT_FIELD_NAMES


def test_valid_event_passes():
    assert validate_event(good_event(), STATIONS, NOW) == []


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"event_id": None}, "missing:event_id"),
        ({"event_ts": "not-a-timestamp"}, "invalid:event_ts"),
        ({"event_ts": format_ts(NOW + timedelta(hours=1))}, "future:event_ts"),
        ({"station_id": "ST999"}, "unknown:station_id"),
        ({"energy_delta_kwh": -0.5}, "negative:energy_delta_kwh"),
        ({"power_kw": "fast"}, "invalid:power_kw"),
        ({"session_status": "paused"}, "invalid:session_status"),
    ],
)
def test_invalid_events_are_rejected(overrides, expected):
    assert expected in validate_event(good_event(**overrides), STATIONS, NOW)


def test_settlement_validation():
    report = {
        "report_id": "r", "session_id": "s", "station_id": "ST001",
        "billed_energy_kwh": 1.0, "billed_amount": 2.0, "settlement_status": "settled",
        "session_end_ts": format_ts(NOW), "reported_at": format_ts(NOW), "schema_version": 1,
    }
    assert validate_settlement(report) == []
    assert "invalid:billed_amount" in validate_settlement({**report, "billed_amount": -1})


def test_timestamps_round_trip_in_utc():
    ts = parse_ts("2026-11-26T22:30:00.123+05:00")
    assert ts.utcoffset() == timedelta(0)
    assert format_ts(ts) == "2026-11-26T17:30:00.123+00:00"
    assert parse_ts("garbage") is None


def test_hour_partition_layout():
    assert hour_partition(NOW) == "dt=2026-11-26/hr=17"


def test_effective_dated_tariffs():
    tariffs = load_tariffs()
    assert "ST001" in station_ids(tariffs)
    before = tariff_at(tariffs, "ST007", datetime(2026, 11, 30, tzinfo=timezone.utc))
    after = tariff_at(tariffs, "ST007", datetime(2026, 12, 2, tzinfo=timezone.utc))
    assert before.tariff_id != after.tariff_id
    assert session_cost(10, before) == pytest.approx(10 * before.price_per_kwh + before.service_fee)
    with pytest.raises(LookupError):
        tariff_at(tariffs, "ST999", NOW)

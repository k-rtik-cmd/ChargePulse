"""ChargePulse event contract: field names, types and validation rules.

Every record that enters the pipeline is described here. The Spark versions
of the same schema and rules live in `chargepulse_common.transforms`, and
`tests/test_spark_transforms.py` checks that the two agree.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

# --------------------------------------------------------------------------
# Topics and S3 layout
# --------------------------------------------------------------------------

EVENTS_TOPIC = "charge-events"
SETTLEMENTS_TOPIC = "charge-settlements"

RAW_EVENTS_PREFIX = "raw/events"
RAW_SETTLEMENTS_PREFIX = "raw/settlements"
DEAD_LETTER_PREFIX = "dead-letter/events"
BATCH_PREFIX = "batch"
META_PREFIX = "meta"

SESSION_STATUSES = ("charging", "completed")
SETTLEMENT_STATUSES = ("settled", "disputed")

# Events may claim to be at most this far in the future (clock skew).
MAX_FUTURE_SKEW = timedelta(minutes=5)

# --------------------------------------------------------------------------
# Charging-session event
#   unique event ID  -> event_id
#   event timestamp  -> event_ts (when the measurement happened)
#   entity key       -> station_id (aggregation key), session_id
#   numeric measures -> energy_delta_kwh, power_kw
# ingest_ts is stamped by the producer when it emits the record; the gap
# between ingest_ts and event_ts is how late the event is.
# --------------------------------------------------------------------------

EVENT_FIELDS: tuple[tuple[str, str, bool], ...] = (
    # (name, logical type, required)
    ("event_id", "string", True),
    ("event_ts", "timestamp", True),
    ("ingest_ts", "timestamp", True),
    ("session_id", "string", True),
    ("station_id", "string", True),
    ("connector_id", "string", False),
    ("vehicle_id", "string", False),
    ("energy_delta_kwh", "double", True),
    ("power_kw", "double", True),
    ("session_status", "string", True),
    ("schema_version", "int", True),
)

SETTLEMENT_FIELDS: tuple[tuple[str, str, bool], ...] = (
    ("report_id", "string", True),
    ("session_id", "string", True),
    ("station_id", "string", True),
    ("billed_energy_kwh", "double", True),
    ("billed_amount", "double", True),
    ("settlement_status", "string", True),
    ("session_end_ts", "timestamp", True),
    ("reported_at", "timestamp", True),
    ("schema_version", "int", True),
)

EVENT_FIELD_NAMES = tuple(name for name, _, _ in EVENT_FIELDS)
SETTLEMENT_FIELD_NAMES = tuple(name for name, _, _ in SETTLEMENT_FIELDS)


def parse_ts(value: Any) -> datetime | None:
    """Parse an ISO-8601 string or datetime into an aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        try:
            ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def format_ts(ts: datetime) -> str:
    """Serialise a datetime the way every ChargePulse record does."""
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event(
    event: dict[str, Any],
    known_stations: Iterable[str],
    now: datetime | None = None,
) -> list[str]:
    """Return the list of rule violations for a charging event (empty = valid).

    Mirrors `transforms.valid_event_condition`; keep the two in sync.
    """
    errors: list[str] = []
    now = now or datetime.now(timezone.utc)

    for name, _, required in EVENT_FIELDS:
        if required and event.get(name) in (None, ""):
            errors.append(f"missing:{name}")

    event_ts = parse_ts(event.get("event_ts"))
    if event.get("event_ts") not in (None, "") and event_ts is None:
        errors.append("invalid:event_ts")
    elif event_ts is not None and event_ts > now + MAX_FUTURE_SKEW:
        errors.append("future:event_ts")

    if event.get("ingest_ts") not in (None, "") and parse_ts(event.get("ingest_ts")) is None:
        errors.append("invalid:ingest_ts")

    station = event.get("station_id")
    if station not in (None, "") and station not in set(known_stations):
        errors.append("unknown:station_id")

    for measure in ("energy_delta_kwh", "power_kw"):
        value = event.get(measure)
        if value is None:
            continue
        if not _is_number(value):
            errors.append(f"invalid:{measure}")
        elif value < 0:
            errors.append(f"negative:{measure}")

    status = event.get("session_status")
    if status not in (None, "") and status not in SESSION_STATUSES:
        errors.append("invalid:session_status")

    return errors


def validate_settlement(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for name, _, required in SETTLEMENT_FIELDS:
        if required and report.get(name) in (None, ""):
            errors.append(f"missing:{name}")
    for measure in ("billed_energy_kwh", "billed_amount"):
        value = report.get(measure)
        if value is not None and (not _is_number(value) or value < 0):
            errors.append(f"invalid:{measure}")
    if report.get("settlement_status") not in (None, "", *SETTLEMENT_STATUSES):
        errors.append("invalid:settlement_status")
    for ts_field in ("session_end_ts", "reported_at"):
        if report.get(ts_field) not in (None, "") and parse_ts(report[ts_field]) is None:
            errors.append(f"invalid:{ts_field}")
    return errors


def hour_partition(ts: datetime) -> str:
    """S3 partition path for an event-time hour, e.g. dt=2026-11-26/hr=09."""
    ts = ts.astimezone(timezone.utc)
    return f"dt={ts:%Y-%m-%d}/hr={ts:%H}"


def hour_start(ts: datetime) -> datetime:
    return ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

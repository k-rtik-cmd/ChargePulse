"""Simulated EV charging network: sessions, meter events and settlement reports."""

from __future__ import annotations

import heapq
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from chargepulse_common import SCHEMA_VERSION
from chargepulse_common.contract import format_ts
from chargepulse_common.tariffs import Tariff, session_cost, station_ids, tariff_at

CONNECTORS_PER_STATION = 4


@dataclass
class SimulatorConfig:
    events_per_second: float = 50.0
    duplicate_rate: float = 0.02          # share of emitted records that re-send a recent event
    late_rate: float = 0.03               # share of new events whose event_ts is pushed back
    late_min_seconds: float = 30.0
    late_max_seconds: float = 240.0
    invalid_rate: float = 0.0             # share of new events deliberately broken
    settlement_delay_min: float = 2.0     # settlement report lag after session end
    settlement_delay_max: float = 15.0
    settlement_mismatch_rate: float = 0.05
    session_events_min: int = 20
    session_events_max: int = 120
    seed: int | None = None

    def validate(self) -> None:
        if self.events_per_second <= 0:
            raise ValueError("events_per_second must be greater than zero")
        for name in ("duplicate_rate", "late_rate", "invalid_rate", "settlement_mismatch_rate"):
            value = getattr(self, name)
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between 0 and 1")
        if not 0 <= self.late_min_seconds <= self.late_max_seconds:
            raise ValueError("late_min_seconds must be between 0 and late_max_seconds")
        if not 0 <= self.settlement_delay_min <= self.settlement_delay_max:
            raise ValueError("settlement delay range is invalid")
        if not 1 <= self.session_events_min <= self.session_events_max:
            raise ValueError("session event range is invalid")


@dataclass
class _Session:
    session_id: str
    station_id: str
    connector_id: str
    vehicle_id: str
    power_kw: float
    events_left: int
    energy_kwh: float = 0.0


@dataclass(order=True)
class _PendingSettlement:
    due: datetime
    report: dict = field(compare=False)


class ChargingSimulator:
    """Generates charging events plus the duplicate/late/invalid variants on demand."""

    def __init__(self, tariffs: list[Tariff], config: SimulatorConfig):
        config.validate()
        self.config = config
        self.tariffs = tariffs
        self.rng = random.Random(config.seed)
        self.connectors = [
            (station, f"C{n:02d}")
            for station in station_ids(tariffs)
            for n in range(1, CONNECTORS_PER_STATION + 1)
        ]
        # Average seconds between two meter readings on the same connector.
        self.reading_interval_s = len(self.connectors) / config.events_per_second
        self.sessions: dict[tuple[str, str], _Session] = {}
        self.recent_events: list[dict] = []
        self.pending_settlements: list[_PendingSettlement] = []
        self.stats = {"events": 0, "duplicates": 0, "late": 0, "invalid": 0, "settlements": 0}

    # ------------------------------------------------------------------
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _start_session(self, station: str, connector: str) -> _Session:
        return _Session(
            session_id=self._uuid(),
            station_id=station,
            connector_id=connector,
            vehicle_id=f"EV{self.rng.randint(1000, 9999)}",
            power_kw=round(self.rng.uniform(7.0, 150.0), 2),
            events_left=self.rng.randint(
                self.config.session_events_min, self.config.session_events_max
            ),
        )

    def _remember(self, event: dict) -> None:
        self.recent_events.append(event)
        if len(self.recent_events) > 500:
            del self.recent_events[:250]

    def _schedule_settlement(self, session: _Session, end_ts: datetime) -> None:
        cfg = self.config
        tariff = tariff_at(self.tariffs, session.station_id, end_ts)
        billed_energy = session.energy_kwh * (1 + self.rng.gauss(0, 0.002))
        status = "settled"
        if self.rng.random() < cfg.settlement_mismatch_rate:
            billed_energy *= 1 + self.rng.choice([-1, 1]) * self.rng.uniform(0.05, 0.15)
            status = "disputed"
        billed_energy = round(max(billed_energy, 0.0), 4)
        due = end_ts + timedelta(
            seconds=self.rng.uniform(cfg.settlement_delay_min, cfg.settlement_delay_max)
        )
        report = {
            "report_id": self._uuid(),
            "session_id": session.session_id,
            "station_id": session.station_id,
            "billed_energy_kwh": billed_energy,
            "billed_amount": session_cost(billed_energy, tariff),
            "settlement_status": status,
            "session_end_ts": format_ts(end_ts),
            "reported_at": None,  # stamped when emitted
            "schema_version": SCHEMA_VERSION,
        }
        heapq.heappush(self.pending_settlements, _PendingSettlement(due, report))

    def _break(self, event: dict) -> dict:
        """Corrupt an event so it fails validation (dead-letter demo)."""
        broken = dict(event)
        fault = self.rng.choice(["negative_energy", "unknown_station", "missing_id", "bad_ts"])
        if fault == "negative_energy":
            broken["energy_delta_kwh"] = -abs(broken["energy_delta_kwh"]) - 0.01
        elif fault == "unknown_station":
            broken["station_id"] = "ST999"
        elif fault == "missing_id":
            broken["event_id"] = None
        else:
            broken["event_ts"] = "not-a-timestamp"
        return broken

    # ------------------------------------------------------------------
    def next_event(self, now: datetime | None = None) -> dict:
        """Return the next record to emit (may be a duplicate, late or invalid)."""
        cfg = self.config
        now = now or datetime.now(timezone.utc)

        if self.recent_events and self.rng.random() < cfg.duplicate_rate:
            self.stats["duplicates"] += 1
            duplicate = dict(self.rng.choice(self.recent_events))
            duplicate["ingest_ts"] = format_ts(now)
            return duplicate

        station, connector = self.rng.choice(self.connectors)
        session = self.sessions.get((station, connector))
        if session is None:
            session = self._start_session(station, connector)
            self.sessions[(station, connector)] = session

        power = round(max(session.power_kw * self.rng.uniform(0.95, 1.05), 0.0), 2)
        energy = round(power * self.reading_interval_s / 3600.0, 5)
        session.energy_kwh += energy
        session.events_left -= 1
        status = "completed" if session.events_left <= 0 else "charging"

        event_ts = now
        is_late = self.rng.random() < cfg.late_rate
        if is_late:
            event_ts = now - timedelta(
                seconds=self.rng.uniform(cfg.late_min_seconds, cfg.late_max_seconds)
            )

        event = {
            "event_id": self._uuid(),
            "event_ts": format_ts(event_ts),
            "ingest_ts": format_ts(now),
            "session_id": session.session_id,
            "station_id": station,
            "connector_id": connector,
            "vehicle_id": session.vehicle_id,
            "energy_delta_kwh": energy,
            "power_kw": power,
            "session_status": status,
            "schema_version": SCHEMA_VERSION,
        }

        if status == "completed":
            del self.sessions[(station, connector)]
            self._schedule_settlement(session, now)

        if cfg.invalid_rate and self.rng.random() < cfg.invalid_rate:
            self.stats["invalid"] += 1
            return self._break(event)

        self.stats["events"] += 1
        self.stats["late"] += int(is_late)
        self._remember(event)
        return event

    def due_settlements(self, now: datetime | None = None) -> list[dict]:
        now = now or datetime.now(timezone.utc)
        due = []
        while self.pending_settlements and self.pending_settlements[0].due <= now:
            report = heapq.heappop(self.pending_settlements).report
            report["reported_at"] = format_ts(now)
            due.append(report)
        self.stats["settlements"] += len(due)
        return due

"""Effective-dated tariff / station policy reference data (config/tariffs.csv)."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .contract import parse_ts

DEFAULT_TARIFF_PATH = Path(
    os.getenv(
        "TARIFF_PATH",
        Path(__file__).resolve().parent.parent / "config" / "tariffs.csv",
    )
)


@dataclass(frozen=True)
class Tariff:
    station_id: str
    tariff_id: str
    price_per_kwh: float
    service_fee: float
    alert_threshold_kwh: float
    effective_from: datetime
    effective_to: datetime


def load_tariffs(path: str | Path = DEFAULT_TARIFF_PATH) -> list[Tariff]:
    with open(path, newline="", encoding="utf-8") as handle:
        return [
            Tariff(
                station_id=row["station_id"],
                tariff_id=row["tariff_id"],
                price_per_kwh=float(row["price_per_kwh"]),
                service_fee=float(row["service_fee"]),
                alert_threshold_kwh=float(row["alert_threshold_kwh"]),
                effective_from=parse_ts(row["effective_from"]),
                effective_to=parse_ts(row["effective_to"]),
            )
            for row in csv.DictReader(handle)
        ]


def station_ids(tariffs: list[Tariff]) -> list[str]:
    return sorted({t.station_id for t in tariffs})


def tariff_at(tariffs: list[Tariff], station_id: str, ts: datetime) -> Tariff:
    """Tariff in force for a station at a point in time [from, to)."""
    for tariff in tariffs:
        if (
            tariff.station_id == station_id
            and tariff.effective_from <= ts < tariff.effective_to
        ):
            return tariff
    raise LookupError(f"No tariff for {station_id} at {ts.isoformat()}")


def session_cost(energy_kwh: float, tariff: Tariff) -> float:
    """Cost formula shared by the producer (billing) and Spark (estimates)."""
    return round(energy_kwh * tariff.price_per_kwh + tariff.service_fee, 4)

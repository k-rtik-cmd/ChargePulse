"""Local write-ahead log (WAL) and the S3 archiver that drains it.

Split-write ordering per record:
  1. append to the local WAL segment (fsync'd every second)  <- durable first
  2. publish to Kafka                                         <- speed path
  3. a background thread uploads each closed one-minute segment
     to S3 as Parquet, then deletes it                        <- batch path

S3 therefore receives one file per minute per hour partition instead of
thousands of tiny objects. If S3 is unavailable, segments stay on disk and
are retried. Object keys are deterministic, so retrying a segment after a
crash overwrites the same object rather than duplicating it.
"""

from __future__ import annotations

import io
import json
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from chargepulse_common.contract import (
    EVENT_FIELDS,
    SETTLEMENT_FIELDS,
    hour_partition,
    parse_ts,
)

log = logging.getLogger("chargepulse.wal")

_ARROW_TYPES = {
    "string": pa.string(),
    "double": pa.float64(),
    "int": pa.int32(),
    "timestamp": pa.timestamp("ms", tz="UTC"),
}


def arrow_schema(fields) -> pa.Schema:
    return pa.schema([pa.field(name, _ARROW_TYPES[kind]) for name, kind, _ in fields])


EVENT_ARROW_SCHEMA = arrow_schema(EVENT_FIELDS)
SETTLEMENT_ARROW_SCHEMA = arrow_schema(SETTLEMENT_FIELDS)


class SegmentLog:
    """Append-only JSONL segments, one per ingest minute."""

    OPEN_SUFFIX = ".jsonl.open"
    CLOSED_SUFFIX = ".jsonl"

    def __init__(self, directory: Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._handle = None
        self._segment: str | None = None
        self._recover()

    def _recover(self) -> None:
        """Close segments left open by a crash so they still get archived."""
        for path in self.directory.glob(f"*{self.OPEN_SUFFIX}"):
            path.rename(path.with_name(path.name[: -len(".open")]))
            log.warning("recovered unclosed WAL segment %s", path.name)

    @staticmethod
    def segment_name(now: datetime) -> str:
        return now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M")

    def append(self, record: dict, now: datetime) -> None:
        name = self.segment_name(now)
        line = json.dumps(record, separators=(",", ":")) + "\n"
        with self._lock:
            if name != self._segment:
                self._close_locked()
                self._segment = name
                self._handle = open(self.directory / f"{name}{self.OPEN_SUFFIX}", "a", encoding="utf-8")
            self._handle.write(line)

    def sync(self) -> None:
        with self._lock:
            if self._handle:
                self._handle.flush()
                os.fsync(self._handle.fileno())

    def rotate(self, now: datetime) -> None:
        """Close the active segment once its minute has passed (or on shutdown)."""
        with self._lock:
            if self._segment and (now is None or self.segment_name(now) != self._segment):
                self._close_locked()

    def _close_locked(self) -> None:
        if self._handle is None:
            return
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()
        open_path = self.directory / f"{self._segment}{self.OPEN_SUFFIX}"
        open_path.rename(self.directory / f"{self._segment}{self.CLOSED_SUFFIX}")
        self._handle = None
        self._segment = None

    def close(self) -> None:
        with self._lock:
            self._close_locked()

    def closed_segments(self) -> list[Path]:
        return sorted(self.directory.glob(f"*{self.CLOSED_SUFFIX}"))


def _read_segment(path: Path) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _to_arrow(records: list[dict], schema: pa.Schema) -> pa.Table:
    columns = {}
    for field in schema:
        values = [r.get(field.name) for r in records]
        if pa.types.is_timestamp(field.type):
            values = [parse_ts(v) for v in values]
        columns[field.name] = pa.array(values, type=field.type)
    return pa.Table.from_pydict(columns, schema=schema)


class ArchiveStream:
    """How one WAL directory maps onto S3."""

    def __init__(self, log_: SegmentLog, prefix: str, partition_field: str,
                 schema: pa.Schema | None):
        self.log = log_
        self.prefix = prefix
        self.partition_field = partition_field
        self.schema = schema  # None -> keep raw JSON lines (dead-letter)


class S3Archiver:
    def __init__(self, s3, bucket: str, instance: str, streams: list[ArchiveStream],
                 on_upload: Callable[[str, int], None] | None = None):
        self.s3 = s3
        self.bucket = bucket
        self.instance = instance
        self.streams = streams
        self.on_upload = on_upload
        self.uploaded_objects = 0
        self.failures = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def object_key(self, stream: ArchiveStream, partition: str, segment: str) -> str:
        ext = "parquet" if stream.schema is not None else "jsonl"
        return f"{stream.prefix}/{partition}/{self.instance}-{segment}.{ext}"

    def upload_segment(self, stream: ArchiveStream, path: Path) -> int:
        records = _read_segment(path)
        segment = path.name[: -len(SegmentLog.CLOSED_SUFFIX)]
        groups: dict[str, list[dict]] = defaultdict(list)
        for record in records:
            ts = parse_ts(record.get(stream.partition_field)) or parse_ts(record.get("ingest_ts"))
            if ts is None:
                ts = datetime.strptime(segment, "%Y%m%dT%H%M").replace(tzinfo=timezone.utc)
            groups[hour_partition(ts)].append(record)

        for partition, rows in sorted(groups.items()):
            if stream.schema is not None:
                buffer = io.BytesIO()
                pq.write_table(_to_arrow(rows, stream.schema), buffer, compression="snappy")
                body = buffer.getvalue()
            else:
                body = "".join(json.dumps(r) + "\n" for r in rows).encode("utf-8")
            key = self.object_key(stream, partition, segment)
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
            self.uploaded_objects += 1
            if self.on_upload:
                self.on_upload(key, len(rows))
        path.unlink()
        return len(records)

    def drain(self) -> int:
        """Upload every closed segment. Raises on the first S3 failure."""
        total = 0
        for stream in self.streams:
            for path in stream.log.closed_segments():
                total += self.upload_segment(stream, path)
        return total

    def _run(self, interval: float) -> None:
        backoff = interval
        while not self._stop.is_set():
            try:
                self.drain()
                backoff = interval
            except Exception as exc:  # noqa: BLE001 - keep segments, retry later
                self.failures += 1
                backoff = min(backoff * 2, 60)
                log.error("S3 upload failed (%s); segments kept on disk, retry in %ss", exc, backoff)
            self._stop.wait(backoff)

    def start(self, interval: float = 5.0) -> None:
        self._thread = threading.Thread(target=self._run, args=(interval,), daemon=True, name="s3-archiver")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=30)


def wait_for(condition: Callable[[], bool], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.2)
    return condition()

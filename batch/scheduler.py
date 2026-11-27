"""Runs the batch job on a fixed schedule (hourly by default).

Runs are aligned to the clock: with BATCH_INTERVAL_SECONDS=3600 and
BATCH_GRACE_SECONDS=300, the job runs at hh:05 every hour, once the
previous hour plus its late-data grace period has passed. It also runs once
at start-up to catch up.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone

INTERVAL = int(os.getenv("BATCH_INTERVAL_SECONDS", "3600"))
GRACE = int(os.getenv("BATCH_GRACE_SECONDS", "300"))

SUBMIT = [
    "/opt/spark/bin/spark-submit",
    "--master", os.getenv("BATCH_SPARK_MASTER", "local[2]"),
    "--driver-memory", os.getenv("BATCH_DRIVER_MEMORY", "1g"),
    "/opt/chargepulse/batch/batch_job.py",
]


def log(msg: str) -> None:
    print(f"[scheduler] {datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


def next_run(now: float) -> float:
    return ((now - GRACE) // INTERVAL + 1) * INTERVAL + GRACE


def main() -> int:
    if INTERVAL <= 0:
        log("BATCH_INTERVAL_SECONDS must be > 0")
        return 1
    log(f"interval={INTERVAL}s grace={GRACE}s")
    while True:
        started = time.time()
        log("starting batch run")
        code = subprocess.call(SUBMIT)
        log(f"batch run finished with exit code {code} in {time.time() - started:.0f}s")
        wake = next_run(time.time())
        log(f"next run at {datetime.fromtimestamp(wake, timezone.utc):%Y-%m-%dT%H:%M:%SZ}")
        # Sleep in short steps and re-check the wall clock, so an early wake-up
        # (VM clock drift, host suspend) can't start the run before the hour has
        # closed - that would skip the hour until the next interval.
        while (remaining := wake - time.time()) > 0:
            time.sleep(min(remaining, 60))


if __name__ == "__main__":
    sys.exit(main())

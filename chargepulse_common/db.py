"""PostgreSQL connection helper for the Spark jobs (credentials from env only)."""

from __future__ import annotations

import os
import time


def connect(retries: int = 10):
    import psycopg

    last_error = None
    for attempt in range(retries):
        try:
            return psycopg.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
                options="-c timezone=UTC",
                connect_timeout=10,
            )
        except psycopg.OperationalError as exc:
            last_error = exc
            time.sleep(min(2 ** attempt, 30))
    raise last_error

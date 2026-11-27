# Technology Decision Record: ChargePulse

Architecture: **Option A, Lambda (dual path)**. One section per layer. *Phase 2 change* notes record where the implementation refines the Phase 1 report, and why.

---

## 1. Language: Python 3.12 (producer) / PySpark 4.0.1 (jobs)

- **Decision:** Python everywhere. The producer runs on `python:3.12.12-slim-bookworm`. The Spark jobs use PySpark on the `apache/spark:4.0.1` image (Python 3.10).
- **Why:** One language lets the batch and speed layers import the same `chargepulse_common` package (contract, tariffs, transforms). PySpark is a first-class API for both Structured Streaming and batch.
- **Alternatives:** Scala Spark (no shared Python contract with the producer); Flink with PyFlink (a weaker Python story and a second engine for batch).

## 2. Message log / broker: Apache Kafka 4.1.1 (KRaft)

- **Decision:** Single broker, KRaft mode (no ZooKeeper). Topics `charge-events` and `charge-settlements`, 3 partitions each, keyed by `station_id`. Per-topic `retention.ms=604800000` (7 days) is set by `kafka-init` and re-applied on every start.
- **Why:** Durable, ordered, replayable log. The Spark Kafka source is mature, and its offsets live in the stream checkpoint. Topic auto-creation is disabled, so typos can't create topics.
- **Watch-outs handled:** Separate `INTERNAL` (containers) and `HOST` (127.0.0.1 only) listeners. The healthcheck gates `kafka-init`, which in turn gates the producer and the speed layer.
- **Alternatives:** Redpanda (lighter, but less familiar to the team); Pulsar (too heavy for one node).

## 3. Stream processing (speed layer): Spark Structured Streaming 4.0.1

- **Decision:** `local[2]` driver, 10-second processing-time trigger. Event-time **1-minute tumbling windows** per station. **Watermark 2 minutes**, `dropDuplicatesWithinWatermark(event_id)`, RocksDB state store, checkpoint on a named volume. Output mode `update`, written with `foreachBatch` as an idempotent `INSERT … ON CONFLICT DO UPDATE` into `speed_view`.
- **Why:** The same engine and the same transformation code as the batch layer. A watermark gives a precise, documented late-data policy. Upserting makes replays after a crash harmless.
- **Late-data policy:** Events up to 2 minutes behind the max event time seen update their window. Later events are dropped by the speed layer only. They are still in S3, and the batch run for their hour includes them, which is exactly the reconciliation Lambda provides.
- **Phase 2 change:** The report said "~1-second windows". The implementation uses 1-minute windows (the guideline's example) because the batch view must have the **same grain** for the merged view and the speed-vs-batch comparison. The report's "speed outputs to `s3://…/speed/` every minute" was dropped: speed views are disposable, recomputable estimates, and writing them to S3 only adds small files nobody reads.

## 4. Batch processing: PySpark 4.0.1, hourly, idempotent per hour

- **Decision:** `batch-scheduler` runs `batch_job.py` with `spark-submit` at **hh:00 + 5 min grace** (configurable via `BATCH_INTERVAL_SECONDS` and `BATCH_GRACE_SECONDS`). For each completed hour in a 48-hour look-back it fingerprints the raw objects (key, ETag, size). It recomputes the hour if the hour is new or the fingerprint changed (late data, repair objects).
- **Idempotency:** Global `dropDuplicates(event_id)`. S3 outputs use fixed keys (`…/part-00000.parquet`, `manifest.json`) and are overwritten. In Postgres the hour is `DELETE`d and re-`INSERT`ed, and `batch_runs` is upserted, all **in one transaction**. Re-running an hour replaces it.
- **Why PySpark:** It's the Phase 1 decision, and it shares `chargepulse_common.transforms` with the speed layer (see the consistency section in the README).
- **Phase 2 change:** Raw objects are downloaded with boto3 and read with `spark.read.parquet` on local disk rather than through `s3a://`. This avoids bundling `hadoop-aws` plus the ~500 MB AWS SDK bundle in the image. One hour is ~180k rows (~60 objects), so this costs nothing at our volume.

## 5. Raw archive: Amazon S3 + Parquet

- **Decision:** `raw/events/dt=YYYY-MM-DD/hr=HH/` partitioned by **event time**, Snappy Parquet with an explicit Arrow schema. One object per minute per hour partition (≈60–70 objects/hour, not ~180k).
- **Split write:** The producer appends to a local write-ahead log (fsync), publishes to Kafka, and a background thread uploads each closed one-minute segment to S3. See the README for the failure analysis.
- **Phase 2 change:** The report's "S3 first, then Kafka" per event would have meant either one S3 PUT per event (tiny files, and S3 latency on every event) or holding events back from Kafka for a minute (no speed layer). The local WAL keeps the report's guarantee, *no event reaches Kafka without a durable master copy*, without either problem. The partition naming changed from `year=/month=/day=/hour=` to the guideline's `dt=/hr=`.

## 6. Catalog / ad hoc query (optional): not used

- The serving store already holds the queryable batch outputs. Athena could be pointed at `batch/` and `raw/` later with partition projection on `dt`/`hr`.

## 7. Serving store: PostgreSQL 16

- **Decision:** `speed_view` and `batch_view` (same columns, primary key `(window_start, station_id)`), `batch_runs` (one row per hour; `MAX(hour_end)` is the boundary), `session_reconciliation`, `station_tariffs`. Views: `serving_boundary`, `merged_view`, `batch_hourly`, `speed_hourly`, `layer_comparison`, `station_alerts`. The schema is created by `serving/init/*.sql` on first start.
- **Why:** Supports transactional replace-an-hour plus boundary updates, which is what makes the merge free of double counting. Grafana has a native driver. The data is small (pre-aggregated).
- Bound to `127.0.0.1`; credentials only from `.env`.

## 8. BI and dashboards: Grafana 11.5.2

- **Decision:** The datasource and dashboard are **provisioned from the repository** (`grafana/provisioning`, `grafana/dashboards/chargepulse.json`), so the export is the source. It has 13 panels with a 10-second auto-refresh, all reading live from PostgreSQL.
- **Why:** Native PostgreSQL datasource, file provisioning (reproducible), time-series first. The admin password comes from `.env`; sign-up and anonymous access are disabled.

## 9. Infrastructure

- One t3.xlarge, Ubuntu 24.04, 30+ GB gp3, one Compose project.
- **Phase 2 change:** our AWS Academy Data Engineering sandbox denies every instance type above `*.small` (IAM statement `Ec2RestrictInstanceTypes`). The deployed instance is therefore a **t3.small (2 vCPU / 2 GB RAM) with a 4 GB swap file**. The JVM sizes are configurable (`KAFKA_HEAP_OPTS`, `SPEED_DRIVER_MEMORY`, `BATCH_DRIVER_MEMORY`), and `deploy/ec2_deploy.py` sets 384 MB / 640 MB / 640 MB on small instances. The code is unchanged; on a t3.xlarge the defaults apply. Every image is pinned (`apache/kafka:4.1.1`, `apache/spark:4.0.1`, `postgres:16.16`, `grafana/grafana:11.5.2`, `python:3.12.12-slim-bookworm`, local store `pgsty/silo:RELEASE.2026-11-12T00-00-00Z`), and so is every Python library (`==`).
- Credentials: the EC2 instance profile (`LabInstanceProfile`) through the default boto3 chain, or temporary Learner Lab keys in `.env`. `.env` is git-ignored.
- Security Group: 22 and 3000 from the team's /32 only. Everything else is bound to `127.0.0.1`.
- **Approximate memory budget:** Kafka 1 GB heap, speed layer 1 GB driver, batch 1 GB driver while running, Postgres + Grafana + producer < 1 GB. That leaves headroom on 16 GB.

# ChargePulse architecture

Option A: **Lambda architecture (dual path)**, on one Ubuntu EC2 instance (t3.xlarge, 4 vCPU / 16 GB RAM, ≥30 GB gp3). Everything runs in one Docker Compose project except Amazon S3.

```mermaid
flowchart LR
    subgraph EC2["EC2 t3.xlarge · Docker Compose project 'chargepulse'"]
        direction LR
        P["<b>producer</b><br/>Python 3.12<br/>charging sessions · settlements<br/>dupes / late / invalid switches"]
        WAL[("producer-wal volume<br/>1-min JSONL segments<br/>(write-ahead log)")]
        K[["<b>kafka</b> 4.1.1 (KRaft)<br/>charge-events ×3<br/>charge-settlements ×3<br/>retention 7 d<br/>:29092 internal · 127.0.0.1:9092"]]
        SL["<b>speed-layer</b><br/>Spark 4.0.1 Structured Streaming<br/>watermark 2 min · 1-min tumbling windows<br/>checkpoint: spark-checkpoints volume<br/>UI 127.0.0.1:4040"]
        BS["<b>batch-scheduler</b><br/>PySpark 4.0.1 · hourly at hh:05<br/>idempotent per-hour recompute"]
        PG[("<b>postgres</b> 16<br/>speed_view · batch_view · batch_runs<br/>session_reconciliation · station_tariffs<br/>merged_view · layer_comparison<br/>127.0.0.1:5432")]
        G["<b>grafana</b> 11.5.2<br/>provisioned dashboard<br/>refresh 10 s<br/>:3000 (Security Group: team /32)"]
        SH[["chargepulse_common<br/>contract · tariffs · transforms"]]
    end

    S3[("<b>Amazon S3</b> (master dataset)<br/>raw/events/dt=YYYY-MM-DD/hr=HH/<br/>raw/settlements/dt=…/hr=…/<br/>dead-letter/events/dt=…/hr=…/<br/>batch/station_minute · batch/session_reconciliation<br/>meta/batch_runs/…/manifest.json")]
    U(("Team browser<br/>your IP /32"))

    P -- "1 · append + fsync" --> WAL
    P -- "2 · publish (acks=all)" --> K
    WAL -- "3 · Parquet, 1 object/min/hour-partition (HTTPS 443)" --> S3
    K -- "charge-events" --> SL
    SL -- "upsert (window_start, station_id)" --> PG
    S3 -- "raw hour + settlements" --> BS
    BS -- "batch/ + meta/ (fixed keys)" --> S3
    BS -- "DELETE+INSERT hour, batch_runs (one transaction)" --> PG
    PG -- "SQL (merged_view …)" --> G
    G --> U
    SH -. imported by .-> P & SL & BS
```

## Data flows

| # | Flow | Format | Notes |
|---|------|--------|-------|
| 1 | producer → WAL | JSON lines | One segment per ingest minute, fsync'd every second. Unclosed segments are recovered after a crash. |
| 2 | producer → Kafka `charge-events`, `charge-settlements` | JSON, key = `station_id` | `acks=all`. Failed sends go to a retry queue and a ledger file (`kafka_failed.log`). |
| 3 | WAL → S3 `raw/…/dt=/hr=/` | Parquet (snappy) | Partitioned by **event time** (settlements by `reported_at`). Fixed key `<instance>-<segment>.parquet`, so a retry overwrites instead of duplicating. |
| 4 | Kafka → speed layer → `speed_view` | | Validate, watermark + `dropDuplicatesWithinWatermark(event_id)`, tariff join, 1-min windows. Output mode `update`, upserted in `foreachBatch`. |
| 5 | S3 → batch layer → `batch_view`, `session_reconciliation`, `batch_runs` | | Recomputes each completed hour whose raw-object fingerprint changed. Writes S3 `batch/` and `meta/` first, then one Postgres transaction. |
| 6 | Postgres → Grafana | SQL | `merged_view` = batch before `MAX(batch_runs.hour_end)`, speed from it onwards. |

## Storage tiers

| Tier | Where | Mutability | Purpose |
|------|-------|------------|---------|
| Write-ahead log | `producer-wal` volume | Deleted once uploaded | Local durability between the Kafka publish and the S3 upload |
| Message log | Kafka `kafka-data` volume | Deleted after 7 days | Speed-path input, replayable |
| **Master dataset** | **S3 `raw/`** | **Append-only** | Source of truth for the batch layer |
| Dead letter | S3 `dead-letter/` | Append-only | Rejected records, kept for inspection |
| Batch output | S3 `batch/`, `meta/` | Overwritten per hour (idempotent) | Authoritative views plus a run manifest |
| Serving | Postgres `pg-data` volume | Derived; can be rebuilt from S3 | Fast dashboard reads |
| Stream state | `spark-checkpoints` volume | Spark-managed | Offsets and window/dedup state (RocksDB) |

## Ports

| Service | Port | Exposure |
|---------|------|----------|
| SSH | 22 | Security Group: team public IP /32 |
| Grafana | 3000 | Security Group: team public IP /32 |
| Kafka (host listener) | 9092 | `127.0.0.1` only (SSH tunnel) |
| Kafka (internal) | 29092, 9093 (controller) | Docker network only |
| PostgreSQL | 5432 | `127.0.0.1` only |
| Spark UI (speed layer) | 4040 | `127.0.0.1` only |
| Local S3 store (`local-s3` profile, dev only) | 9000 / 9001 | `127.0.0.1` only |
| Amazon S3 | 443 outbound | HTTPS to AWS |

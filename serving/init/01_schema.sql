-- ChargePulse serving layer schema (PostgreSQL).
-- Runs automatically on the first start of an empty postgres volume.
-- All timestamps are timestamptz in UTC.

SET timezone = 'UTC';

-- --------------------------------------------------------------------------
-- Reference data: effective-dated tariffs (loaded from config/tariffs.csv)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS station_tariffs (
    station_id          text        NOT NULL,
    tariff_id           text        NOT NULL,
    price_per_kwh       numeric(10,4) NOT NULL,
    service_fee         numeric(10,4) NOT NULL,
    alert_threshold_kwh numeric(10,4) NOT NULL,
    effective_from      timestamptz NOT NULL,
    effective_to        timestamptz NOT NULL,
    PRIMARY KEY (station_id, effective_from)
);

-- --------------------------------------------------------------------------
-- Speed view: 1-minute windows per station, upserted by the streaming job.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS speed_view (
    window_start     timestamptz      NOT NULL,
    window_end       timestamptz      NOT NULL,
    station_id       text             NOT NULL,
    event_count      bigint           NOT NULL,
    session_count    bigint           NOT NULL,
    total_energy_kwh double precision NOT NULL,
    est_energy_cost  double precision,
    avg_power_kw     double precision,
    max_power_kw     double precision,
    updated_at       timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, station_id)
);

-- --------------------------------------------------------------------------
-- Batch view: same grain and columns, recomputed per hour from S3.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_view (
    window_start     timestamptz      NOT NULL,
    window_end       timestamptz      NOT NULL,
    station_id       text             NOT NULL,
    event_count      bigint           NOT NULL,
    session_count    bigint           NOT NULL,
    total_energy_kwh double precision NOT NULL,
    est_energy_cost  double precision,
    avg_power_kw     double precision,
    max_power_kw     double precision,
    computed_at      timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, station_id)
);

-- --------------------------------------------------------------------------
-- Batch metadata: one row per processed event-time hour. MAX(hour_end) is the
-- serving boundary. It is written in the same transaction as batch_view.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_runs (
    hour_start         timestamptz PRIMARY KEY,
    hour_end           timestamptz NOT NULL,
    fingerprint        text        NOT NULL,
    raw_files          integer     NOT NULL,
    raw_rows           bigint      NOT NULL,
    invalid_rows       bigint      NOT NULL,
    dead_letter_rows   bigint      NOT NULL,
    duplicates_removed bigint      NOT NULL,
    unique_events      bigint      NOT NULL,
    late_events        bigint      NOT NULL,
    settlements        integer     NOT NULL,
    aggregate_rows     integer     NOT NULL,
    started_at         timestamptz NOT NULL,
    finished_at        timestamptz NOT NULL,
    run_count          integer     NOT NULL DEFAULT 1
);

-- --------------------------------------------------------------------------
-- Session-level cost reconciliation (calculated vs settlement report).
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS session_reconciliation (
    session_id            text PRIMARY KEY,
    report_id             text NOT NULL,
    station_id            text NOT NULL,
    tariff_id             text,
    session_end_ts        timestamptz NOT NULL,
    reported_at           timestamptz NOT NULL,
    settlement_hour       timestamptz NOT NULL,
    event_count           bigint NOT NULL,
    calc_energy_kwh       double precision NOT NULL,
    calc_amount           double precision,
    billed_energy_kwh     double precision NOT NULL,
    billed_amount         double precision NOT NULL,
    energy_diff_kwh       double precision,
    amount_diff           double precision,
    reconciliation_status text NOT NULL,
    settlement_status     text NOT NULL
);
CREATE INDEX IF NOT EXISTS session_reconciliation_hour_idx
    ON session_reconciliation (settlement_hour);

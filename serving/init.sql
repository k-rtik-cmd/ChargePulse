CREATE TABLE IF NOT EXISTS speed_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    station_id VARCHAR(100),
    total_energy_kwh DOUBLE PRECISION,
    avg_power_kw DOUBLE PRECISION,
    session_count BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    station_id VARCHAR(100),
    total_energy_kwh DOUBLE PRECISION,
    avg_power_kw DOUBLE PRECISION,
    session_count BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_metadata (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
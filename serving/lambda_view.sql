CREATE OR REPLACE VIEW lambda_metrics AS
WITH batch_boundary AS (
    SELECT
        COALESCE(
            MAX(value::timestamptz),
            '1970-01-01'::timestamptz
        ) AS completed_at
    FROM pipeline_metadata
    WHERE key = 'batch_completed_at'
),
combined AS (
    SELECT
        b.window_start,
        b.window_end,
        b.station_id,
        b.total_energy_kwh,
        b.avg_power_kw,
        b.session_count,
        'batch' AS data_source
    FROM batch_metrics b
    CROSS JOIN batch_boundary bb
    WHERE b.window_end <= bb.completed_at

    UNION ALL

    SELECT
        s.window_start,
        s.window_end,
        s.station_id,
        s.total_energy_kwh,
        s.avg_power_kw,
        s.session_count,
        'speed' AS data_source
    FROM speed_metrics s
    CROSS JOIN batch_boundary bb
    WHERE s.window_end > bb.completed_at
)
SELECT * FROM combined;
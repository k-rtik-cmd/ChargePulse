-- ChargePulse serving views: batch, speed and merged.

-- The serving boundary: end of the latest hour the batch layer has completed.
-- Before the first batch run it is -infinity, so all data comes from speed.
CREATE OR REPLACE VIEW serving_boundary AS
SELECT COALESCE(MAX(hour_end), '-infinity'::timestamptz) AS batch_boundary,
       MAX(finished_at)                                  AS last_batch_finished_at
FROM batch_runs;

-- Merged view. Batch rows are authoritative for every window that starts
-- before the boundary. Speed rows only fill the interval from the boundary
-- onwards. The boundary is always on the hour and windows are 1 minute long,
-- so no window falls on both sides and nothing is counted twice.
CREATE OR REPLACE VIEW merged_view AS
SELECT b.window_start, b.window_end, b.station_id, b.event_count, b.session_count,
       b.total_energy_kwh, b.est_energy_cost, b.avg_power_kw, b.max_power_kw,
       'batch'::text AS source
FROM batch_view b
CROSS JOIN serving_boundary sb
WHERE b.window_start < sb.batch_boundary
UNION ALL
SELECT s.window_start, s.window_end, s.station_id, s.event_count, s.session_count,
       s.total_energy_kwh, s.est_energy_cost, s.avg_power_kw, s.max_power_kw,
       'speed'::text AS source
FROM speed_view s
CROSS JOIN serving_boundary sb
WHERE s.window_start >= sb.batch_boundary;

-- Hourly roll-ups of each layer, and how far the speed estimate was from the
-- authoritative batch result for every hour the batch has processed.
CREATE OR REPLACE VIEW batch_hourly AS
SELECT date_trunc('hour', window_start) AS hour_start, station_id,
       SUM(event_count) AS event_count, SUM(total_energy_kwh) AS total_energy_kwh,
       SUM(est_energy_cost) AS est_energy_cost
FROM batch_view GROUP BY 1, 2;

CREATE OR REPLACE VIEW speed_hourly AS
SELECT date_trunc('hour', window_start) AS hour_start, station_id,
       SUM(event_count) AS event_count, SUM(total_energy_kwh) AS total_energy_kwh,
       SUM(est_energy_cost) AS est_energy_cost
FROM speed_view GROUP BY 1, 2;

CREATE OR REPLACE VIEW layer_comparison AS
SELECT r.hour_start,
       COALESCE(SUM(s.event_count), 0)        AS speed_events,
       r.unique_events                        AS batch_events,
       ROUND(COALESCE(SUM(s.total_energy_kwh), 0)::numeric, 3) AS speed_energy_kwh,
       ROUND((SELECT SUM(b.total_energy_kwh) FROM batch_view b
              WHERE b.window_start >= r.hour_start AND b.window_start < r.hour_end)::numeric, 3)
                                              AS batch_energy_kwh,
       r.duplicates_removed, r.late_events, r.invalid_rows, r.dead_letter_rows,
       r.run_count, r.finished_at
FROM batch_runs r
LEFT JOIN speed_hourly s ON s.hour_start = r.hour_start
GROUP BY r.hour_start, r.hour_end, r.unique_events, r.duplicates_removed, r.late_events,
         r.invalid_rows, r.dead_letter_rows, r.run_count, r.finished_at;

-- Threshold alerts: station-minutes in the merged view above the station's
-- per-minute energy threshold from the tariff in force at that time.
CREATE OR REPLACE VIEW station_alerts AS
SELECT m.window_start, m.station_id, ROUND(m.total_energy_kwh::numeric, 3) AS energy_kwh,
       t.alert_threshold_kwh, m.source
FROM merged_view m
JOIN station_tariffs t
  ON t.station_id = m.station_id
 AND m.window_start >= t.effective_from AND m.window_start < t.effective_to
WHERE m.total_energy_kwh > t.alert_threshold_kwh;

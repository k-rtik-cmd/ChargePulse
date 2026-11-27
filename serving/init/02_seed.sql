-- Load the same tariff file the Spark jobs and the producer use.
-- docker-compose mounts config/tariffs.csv at /seed/tariffs.csv.
COPY station_tariffs (station_id, tariff_id, price_per_kwh, service_fee,
                      alert_threshold_kwh, effective_from, effective_to)
FROM '/seed/tariffs.csv' WITH (FORMAT csv, HEADER true);

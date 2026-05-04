-- =================================================================
--  AI Energy Impact Monitor — Raw Schema
--  Run this against the pipeline database after stack is up:
--    docker exec -i pipeline-postgres psql -U pipeline -d pipeline < postgres/schema/01_raw_schema.sql
-- =================================================================

-- -----------------------------------------------------------------
--  Reference table — grid regions
--  Links every time-series row to a named geographic region
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS grid_regions (
    region_id       TEXT        PRIMARY KEY,
    operator        TEXT        NOT NULL,       -- ERCOT | CAISO | PJM | EIA
    display_name    TEXT        NOT NULL,
    state_codes     TEXT[],                     -- e.g. ARRAY['TX']
    lat             NUMERIC(8,5),
    lon             NUMERIC(8,5),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO grid_regions (region_id, operator, display_name, state_codes, lat, lon) VALUES
    ('ERCOT',       'ERCOT',  'Texas',                  ARRAY['TX'],                 31.9686, -99.9018),
    ('CAISO',       'CAISO',  'California',             ARRAY['CA'],                 36.7783, -119.4179),
    ('PJM',         'PJM',    'Mid-Atlantic / Midwest', ARRAY['VA','MD','DC','PA'],  39.9526, -75.1652),
    ('EIA_US',      'EIA',    'Continental US',         NULL,                        39.5,    -98.35)
ON CONFLICT (region_id) DO NOTHING;


-- -----------------------------------------------------------------
--  1. AQI — AirNow (EPA)
--  One row per station reading per timestamp
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aqi_raw (
    time            TIMESTAMPTZ NOT NULL,
    region_id       TEXT        NOT NULL REFERENCES grid_regions(region_id),
    station_id      TEXT        NOT NULL,
    lat             NUMERIC(8,5),
    lon             NUMERIC(8,5),
    parameter       TEXT        NOT NULL,   -- PM2.5 | PM10 | O3 | NO2 | CO | SO2
    aqi             INTEGER,
    concentration   NUMERIC(10,4),
    unit            TEXT,
    category        TEXT,                   -- Good | Moderate | USG | Unhealthy | etc.
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('aqi_raw', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS aqi_raw_region_time  ON aqi_raw (region_id, time DESC);
CREATE INDEX IF NOT EXISTS aqi_raw_station_time ON aqi_raw (station_id, time DESC);

COMMENT ON TABLE aqi_raw IS
    'Raw AQI readings from AirNow (EPA). Updated every 30 minutes per station.';


-- -----------------------------------------------------------------
--  2. Weather — Open-Meteo
--  Hourly forecast/observation per region centroid
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_raw (
    time                TIMESTAMPTZ NOT NULL,
    region_id           TEXT        NOT NULL REFERENCES grid_regions(region_id),
    temperature_2m      NUMERIC(6,2),       -- °C
    apparent_temperature NUMERIC(6,2),      -- feels-like / heat index °C
    relative_humidity   NUMERIC(5,2),       -- %
    precipitation       NUMERIC(6,2),       -- mm
    wind_speed_10m      NUMERIC(6,2),       -- km/h
    wind_direction_10m  NUMERIC(5,1),       -- degrees
    cloud_cover         NUMERIC(5,2),       -- %
    surface_pressure    NUMERIC(8,2),       -- hPa
    is_forecast         BOOLEAN NOT NULL DEFAULT FALSE,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('weather_raw', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS weather_raw_region_time ON weather_raw (region_id, time DESC);

COMMENT ON TABLE weather_raw IS
    'Hourly weather observations and short-range forecasts from Open-Meteo.';


-- -----------------------------------------------------------------
--  3. Grid generation mix — EIA
--  Hourly fuel-type breakdown per region as % of total output
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS grid_generation_raw (
    time            TIMESTAMPTZ NOT NULL,
    region_id       TEXT        NOT NULL REFERENCES grid_regions(region_id),
    -- Generation by fuel type (MWh)
    natural_gas_mwh NUMERIC(12,2),
    coal_mwh        NUMERIC(12,2),
    nuclear_mwh     NUMERIC(12,2),
    wind_mwh        NUMERIC(12,2),
    solar_mwh       NUMERIC(12,2),
    hydro_mwh       NUMERIC(12,2),
    other_mwh       NUMERIC(12,2),
    total_mwh       NUMERIC(12,2),
    -- Derived percentages (stored for query performance)
    nuclear_pct     NUMERIC(5,2),
    fossil_pct      NUMERIC(5,2),
    clean_pct       NUMERIC(5,2),           -- nuclear + wind + solar + hydro
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('grid_generation_raw', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS grid_gen_region_time ON grid_generation_raw (region_id, time DESC);

COMMENT ON TABLE grid_generation_raw IS
    'Hourly generation mix by fuel type from EIA API. Percentages derived at ingest.';


-- -----------------------------------------------------------------
--  4. Grid demand — ERCOT / CAISO / PJM
--  Actual load in MW, updated every 5 minutes
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS grid_demand_raw (
    time            TIMESTAMPTZ NOT NULL,
    region_id       TEXT        NOT NULL REFERENCES grid_regions(region_id),
    demand_mw       NUMERIC(12,2)  NOT NULL,    -- actual instantaneous load
    forecast_mw     NUMERIC(12,2),              -- operator day-ahead forecast
    net_load_mw     NUMERIC(12,2),              -- demand minus renewables
    source          TEXT        NOT NULL,        -- ercot | caiso | pjm
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('grid_demand_raw', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS grid_demand_region_time ON grid_demand_raw (region_id, time DESC);

COMMENT ON TABLE grid_demand_raw IS
    'Real-time grid load in MW from ERCOT, CAISO, and PJM. Updated every 5 minutes.';


-- -----------------------------------------------------------------
--  5. Nuclear reactor status — NRC
--  Daily capacity % per reactor unit
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nuclear_status_raw (
    time            TIMESTAMPTZ NOT NULL,   -- report date (daily)
    region_id       TEXT        REFERENCES grid_regions(region_id),
    unit_name       TEXT        NOT NULL,   -- e.g. "VOGTLE 3"
    power_pct       NUMERIC(5,2),           -- % of licensed capacity
    status_code     TEXT,                   -- NRC status code
    state_code      TEXT,                   -- 2-letter state
    operator        TEXT,                   -- utility name
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('nuclear_status_raw', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS nuclear_unit_time ON nuclear_status_raw (unit_name, time DESC);
CREATE INDEX IF NOT EXISTS nuclear_region_time ON nuclear_status_raw (region_id, time DESC);

COMMENT ON TABLE nuclear_status_raw IS
    'Daily reactor power output as % of licensed capacity, from NRC status report.';


-- -----------------------------------------------------------------
--  6. Seismic — USGS
--  Every M1.0+ event globally, updated every 10 minutes
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS seismic_raw (
    time            TIMESTAMPTZ NOT NULL,
    event_id        TEXT        NOT NULL,       -- USGS event ID (unique)
    magnitude       NUMERIC(4,2) NOT NULL,
    magnitude_type  TEXT,                       -- ml | mw | mb | etc.
    depth_km        NUMERIC(7,2),
    lat             NUMERIC(8,5) NOT NULL,
    lon             NUMERIC(9,5) NOT NULL,
    place           TEXT,                       -- human-readable location
    alert_level     TEXT,                       -- green | yellow | orange | red
    tsunami         BOOLEAN NOT NULL DEFAULT FALSE,
    region_id       TEXT        REFERENCES grid_regions(region_id),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('seismic_raw', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS seismic_event_id ON seismic_raw (event_id, time);
CREATE INDEX IF NOT EXISTS seismic_mag_time ON seismic_raw (magnitude DESC, time DESC);
CREATE INDEX IF NOT EXISTS seismic_region_time ON seismic_raw (region_id, time DESC);

COMMENT ON TABLE seismic_raw IS
    'USGS M1.0+ seismic events. Relevant for nuclear plant proximity monitoring.';


-- -----------------------------------------------------------------
--  Continuous aggregates — pre-roll hourly summaries for dashboard
--  These auto-update as new data arrives (TimescaleDB feature)
-- -----------------------------------------------------------------

-- Hourly AQI average per region
CREATE MATERIALIZED VIEW IF NOT EXISTS aqi_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    region_id,
    parameter,
    AVG(aqi)           AS aqi_avg,
    MAX(aqi)           AS aqi_max,
    MIN(aqi)           AS aqi_min,
    COUNT(*)           AS reading_count
FROM aqi_raw
GROUP BY bucket, region_id, parameter
WITH NO DATA;

SELECT add_continuous_aggregate_policy('aqi_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Hourly grid demand average per region
CREATE MATERIALIZED VIEW IF NOT EXISTS grid_demand_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    region_id,
    AVG(demand_mw)      AS demand_avg_mw,
    MAX(demand_mw)      AS demand_max_mw,
    AVG(forecast_mw)    AS forecast_avg_mw
FROM grid_demand_raw
GROUP BY bucket, region_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('grid_demand_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- =================================================================
--  Schema complete. Verify with:
--    \dt             — list tables
--    \dm             — list materialized views
--    SELECT * FROM timescaledb_information.hypertables;
-- =================================================================


-- =================================================================
--  Compression policies
--  Chunks older than 7 days are compressed (~90-95% size reduction)
--  Queries remain fully transparent — no changes needed in DAGs or dbt
-- =================================================================

ALTER TABLE aqi_raw SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'time DESC',
    timescaledb.compress_segmentby = 'region_id, station_id'
);
SELECT add_compression_policy('aqi_raw', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE weather_raw SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'time DESC',
    timescaledb.compress_segmentby = 'region_id'
);
SELECT add_compression_policy('weather_raw', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE grid_generation_raw SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'time DESC',
    timescaledb.compress_segmentby = 'region_id'
);
SELECT add_compression_policy('grid_generation_raw', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE grid_demand_raw SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'time DESC',
    timescaledb.compress_segmentby = 'region_id, source'
);
SELECT add_compression_policy('grid_demand_raw', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE nuclear_status_raw SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'time DESC',
    timescaledb.compress_segmentby = 'region_id, unit_name'
);
SELECT add_compression_policy('nuclear_status_raw', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE seismic_raw SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'time DESC',
    timescaledb.compress_segmentby = 'region_id'
);
SELECT add_compression_policy('seismic_raw', INTERVAL '7 days', if_not_exists => TRUE);


-- =================================================================
--  Retention policies
--  Raw chunks older than the window are automatically dropped.
--  Long-term trends are preserved in dbt mart tables, not raw rows.
--
--  Window rationale:
--    grid_demand_raw  — 90 days  (high frequency, mart holds summaries)
--    aqi_raw          — 90 days  (30-min cadence, mart holds hourly rollups)
--    weather_raw      — 90 days  (hourly, mart holds daily summaries)
--    grid_generation  — 90 days  (hourly, mart holds daily fuel mix)
--    seismic_raw      — 180 days (lower frequency, events worth keeping longer)
--    nuclear_status   — 365 days (daily only, small footprint, keep a full year)
-- =================================================================

SELECT add_retention_policy('grid_demand_raw',     INTERVAL '90 days',  if_not_exists => TRUE);
SELECT add_retention_policy('aqi_raw',             INTERVAL '90 days',  if_not_exists => TRUE);
SELECT add_retention_policy('weather_raw',         INTERVAL '90 days',  if_not_exists => TRUE);
SELECT add_retention_policy('grid_generation_raw', INTERVAL '90 days',  if_not_exists => TRUE);
SELECT add_retention_policy('seismic_raw',         INTERVAL '180 days', if_not_exists => TRUE);
SELECT add_retention_policy('nuclear_status_raw',  INTERVAL '365 days', if_not_exists => TRUE);


-- =================================================================
--  Verify compression and retention policies with:
--    SELECT * FROM timescaledb_information.compression_settings;
--    SELECT * FROM timescaledb_information.jobs ORDER BY job_id;
--
--  Check current database size with:
--    SELECT pg_size_pretty(pg_database_size('pipeline'));
--
--  Check per-table compressed vs uncompressed size with:
--    SELECT hypertable_name,
--           pg_size_pretty(before_compression_total_bytes) AS before,
--           pg_size_pretty(after_compression_total_bytes)  AS after
--    FROM timescaledb_information.compressed_chunk_stats
--    GROUP BY hypertable_name;
-- =================================================================

{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_regional_hourly_metrics: primary fact table
--
-- One row per (region_id, hour). All signals from int_demand_spikes (which
-- inlines int_regional_hourly) plus:
--   - demand_forecast_delta_mw: actual demand minus operator forecast
--   - peak_period: 'peak' for hours 14–20 UTC, 'off_peak' otherwise
--
-- This is the source for the majority of dashboard charts and the base for
-- most other mart models. Answers questions 3 (regional stress comparison)
-- and 4 (peak vs off-peak clean energy mix).
--
-- Peak period note: hours are UTC. Hours 14–20 UTC ≈ 9am–3pm Eastern /
-- 6am–noon Pacific — slightly earlier than the true afternoon peak in those
-- regions. Acceptable for Phase 4; refine with AT TIME ZONE for Phase 5.
-- ─────────────────────────────────────────────────────────────────────────────

select
    hour,
    region_id,

    -- Demand
    demand_avg_mw,
    demand_max_mw,
    forecast_avg_mw,
    demand_avg_mw - forecast_avg_mw             as demand_forecast_delta_mw,

    -- Spike detection
    demand_baseline_mw,
    demand_stddev_mw,
    demand_anomaly_mw,
    is_demand_spike,

    -- Weather
    temperature_c,
    apparent_temperature_c,
    relative_humidity_pct,
    precipitation_mm,
    wind_speed_kmh,
    cloud_cover_pct,

    -- AQI — PM2.5
    pm25_aqi_avg,
    pm25_aqi_max,
    pm25_station_count,

    -- AQI — Ozone
    ozone_aqi_avg,
    ozone_aqi_max,
    ozone_station_count,

    -- Generation mix — percentages
    nuclear_pct,
    fossil_pct,
    clean_pct,
    total_mwh,

    -- Generation mix — individual fuels
    solar_mwh,
    wind_mwh,
    natural_gas_mwh,
    coal_mwh,

    -- Peak period classification
    case
        when extract(hour from hour) between 14 and 20
            then 'peak'
        else 'off_peak'
    end                                         as peak_period

from {{ ref('int_demand_spikes') }}

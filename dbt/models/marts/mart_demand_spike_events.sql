{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_demand_spike_events: the "2-hour lag" analysis table
--
-- One row per demand spike hour (is_demand_spike = true). Uses LEAD to look
-- 2 rows ahead (= 2 hours) within each region's hourly series to capture the
-- AQI value after the spike.
--
-- pm25_aqi_delta_2h is the key metric: positive = AQI worsened after the spike.
-- This directly quantifies question 1: "within a 2-hour window."
--
-- apparent_temperature_c is included alongside temperature_c so the dashboard
-- can compare which predictor correlates more tightly with demand spikes
-- (question 7 — heat index vs raw temperature sensitivity).
--
-- NULL handling: spike events at the tail of the series have no lookahead rows;
-- pm25_aqi_2h_after and pm25_aqi_delta_2h will be NULL for those rows.
-- ─────────────────────────────────────────────────────────────────────────────

with all_hours as (
    select
        hour,
        region_id,
        demand_avg_mw,
        demand_max_mw,
        demand_baseline_mw,
        demand_anomaly_mw,
        demand_stddev_mw,
        temperature_c,
        apparent_temperature_c,
        pm25_aqi_avg                                as pm25_aqi_at_spike,
        nuclear_pct,
        fossil_pct,
        is_demand_spike,

        lead(pm25_aqi_avg, 2) over (
            partition by region_id
            order by hour
        )                                           as pm25_aqi_2h_after

    from {{ ref('int_demand_spikes') }}
)

select
    hour                                            as spike_hour,
    region_id,
    demand_avg_mw,
    demand_max_mw,
    demand_baseline_mw,
    demand_anomaly_mw,
    temperature_c,
    apparent_temperature_c,
    pm25_aqi_at_spike,
    pm25_aqi_2h_after,
    pm25_aqi_2h_after - pm25_aqi_at_spike          as pm25_aqi_delta_2h,
    nuclear_pct,
    fossil_pct
from all_hours
where
    is_demand_spike = true
order by spike_hour desc

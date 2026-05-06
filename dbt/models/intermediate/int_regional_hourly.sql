{{
  config(materialized='ephemeral')
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- int_regional_hourly: the hourly spine
--
-- One row per (region_id, hour). Joins demand, weather, AQI (PM2.5 and OZONE
-- pivoted into separate columns), and generation mix. All joins are LEFT so
-- hours with partial data still appear in the timeline.
--
-- Demand and AQI are sourced from TimescaleDB continuous aggregates
-- (grid_demand_hourly, aqi_hourly) which pre-aggregate from 5-min / 30-min
-- raw data. Weather and generation are already hourly; time_bucket is applied
-- defensively in case of sub-hourly backfill rows.
-- ─────────────────────────────────────────────────────────────────────────────

with demand_hourly as (
    -- Already one row per (bucket, region_id) — no further aggregation needed.
    select
        bucket                              as hour,
        region_id,
        demand_avg_mw,
        demand_max_mw,
        forecast_avg_mw
    from {{ source('raw', 'grid_demand_hourly') }}
),

weather_hourly as (
    select
        time_bucket('1 hour', time)         as hour,
        region_id,
        avg(temperature_2m)                 as temperature_c,
        avg(apparent_temperature)           as apparent_temperature_c,
        avg(relative_humidity)              as relative_humidity_pct,
        avg(precipitation)                  as precipitation_mm,
        avg(wind_speed_10m)                 as wind_speed_kmh,
        avg(cloud_cover)                    as cloud_cover_pct
    from {{ ref('stg_weather') }}
    group by 1, 2
),

aqi_pm25 as (
    select
        bucket                              as hour,
        region_id,
        aqi_avg                             as pm25_aqi_avg,
        aqi_max                             as pm25_aqi_max,
        reading_count                       as pm25_station_count
    from {{ source('raw', 'aqi_hourly') }}
    where parameter = 'PM2.5'
),

aqi_ozone as (
    select
        bucket                              as hour,
        region_id,
        aqi_avg                             as ozone_aqi_avg,
        aqi_max                             as ozone_aqi_max,
        reading_count                       as ozone_station_count
    from {{ source('raw', 'aqi_hourly') }}
    where parameter = 'OZONE'
),

generation_hourly as (
    select
        time_bucket('1 hour', time)         as hour,
        region_id,
        avg(nuclear_pct)                    as nuclear_pct,
        avg(fossil_pct)                     as fossil_pct,
        avg(clean_pct)                      as clean_pct,
        sum(total_mwh)                      as total_mwh,
        sum(solar_mwh)                      as solar_mwh,
        sum(wind_mwh)                       as wind_mwh,
        sum(natural_gas_mwh)                as natural_gas_mwh,
        sum(coal_mwh)                       as coal_mwh
    from {{ ref('stg_grid_generation') }}
    group by 1, 2
)

select
    d.hour,
    d.region_id,

    -- Demand
    d.demand_avg_mw,
    d.demand_max_mw,
    d.forecast_avg_mw,

    -- Weather
    w.temperature_c,
    w.apparent_temperature_c,
    w.relative_humidity_pct,
    w.precipitation_mm,
    w.wind_speed_kmh,
    w.cloud_cover_pct,

    -- AQI — PM2.5
    p.pm25_aqi_avg,
    p.pm25_aqi_max,
    p.pm25_station_count,

    -- AQI — Ozone
    o.ozone_aqi_avg,
    o.ozone_aqi_max,
    o.ozone_station_count,

    -- Generation mix — percentages
    g.nuclear_pct,
    g.fossil_pct,
    g.clean_pct,
    g.total_mwh,

    -- Generation mix — individual fuels (for renewable backup analysis)
    g.solar_mwh,
    g.wind_mwh,
    g.natural_gas_mwh,
    g.coal_mwh

from demand_hourly          d
left join weather_hourly    w  on d.hour = w.hour  and d.region_id = w.region_id
left join aqi_pm25          p  on d.hour = p.hour  and d.region_id = p.region_id
left join aqi_ozone         o  on d.hour = o.hour  and d.region_id = o.region_id
left join generation_hourly g  on d.hour = g.hour  and d.region_id = g.region_id

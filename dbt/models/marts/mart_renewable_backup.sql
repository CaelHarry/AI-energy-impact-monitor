{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_renewable_backup: renewable intermittency → fossil ramp analysis
--
-- When solar or wind generation drops sharply, gas turbines ramp to compensate.
-- This model surfaces that mechanism by computing hour-over-hour deltas for
-- solar, wind, and natural gas generation per region.
--
-- Key metrics:
--   solar_change_mwh      — solar output change from the previous hour
--   wind_change_mwh       — wind output change from the previous hour
--   renewable_change_mwh  — combined solar + wind change
--   gas_change_mwh        — natural gas change from the previous hour
--
-- A large negative renewable_change_mwh paired with a large positive
-- gas_change_mwh in the same or next hour is the peaker-activation signal.
-- cloud_cover_pct from weather is included as the leading indicator of
-- solar intermittency events before they appear in generation data.
--
-- Only rows with at least one fuel source populated are included.
-- ─────────────────────────────────────────────────────────────────────────────

with hourly as (
    select
        hour,
        region_id,
        solar_mwh,
        wind_mwh,
        natural_gas_mwh,
        coal_mwh,
        clean_pct,
        fossil_pct,
        total_mwh,
        wind_speed_kmh,
        cloud_cover_pct
    from {{ ref('mart_regional_hourly_metrics') }}
    where
        solar_mwh       is not null
        or wind_mwh     is not null
        or natural_gas_mwh is not null
)

select
    hour,
    region_id,
    solar_mwh,
    wind_mwh,
    coalesce(solar_mwh, 0) + coalesce(wind_mwh, 0)     as renewable_mwh,
    natural_gas_mwh,
    coal_mwh,
    total_mwh,
    clean_pct,
    fossil_pct,
    wind_speed_kmh,
    cloud_cover_pct,

    -- Hour-over-hour deltas (positive = generation increased, negative = dropped)
    solar_mwh - lag(solar_mwh) over (
        partition by region_id order by hour
    )                                                   as solar_change_mwh,

    wind_mwh - lag(wind_mwh) over (
        partition by region_id order by hour
    )                                                   as wind_change_mwh,

    (coalesce(solar_mwh, 0) + coalesce(wind_mwh, 0))
    - (coalesce(lag(solar_mwh) over (partition by region_id order by hour), 0)
     + coalesce(lag(wind_mwh)  over (partition by region_id order by hour), 0))
                                                        as renewable_change_mwh,

    natural_gas_mwh - lag(natural_gas_mwh) over (
        partition by region_id order by hour
    )                                                   as gas_change_mwh

from hourly
order by region_id, hour

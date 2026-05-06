{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_nuclear_aqi_impact: nuclear output vs air quality correlation
--
-- Groups hourly observations by nuclear output quartile (NTILE 4) within each
-- region. For each quartile: summary statistics on nuclear_pct, pm25_aqi_avg,
-- fossil_pct, and temperature_c.
--
-- Answers question 2: does higher nuclear output correlate with lower AQI?
-- If yes, avg_pm25_aqi decreases as nuclear_quartile increases (1 → 4).
-- fossil_pct and temperature_c are included as confounders — if high-nuclear
-- hours also happen to be low-temperature hours, the correlation may be
-- explained by demand level rather than the nuclear mix itself.
--
-- Only hours with both nuclear_pct and pm25_aqi_avg populated are included.
-- hour_count exposes how many observations back each quartile — a minimum of
-- ~100 hours per quartile is a reasonable threshold for statistical reliability.
-- ─────────────────────────────────────────────────────────────────────────────

with hours_with_data as (
    select
        hour,
        region_id,
        nuclear_pct,
        pm25_aqi_avg,
        fossil_pct,
        temperature_c,

        ntile(4) over (
            partition by region_id
            order by nuclear_pct asc          -- quartile 1 = lowest nuclear output
        )                                     as nuclear_quartile

    from {{ ref('mart_regional_hourly_metrics') }}
    where
        nuclear_pct  is not null
        and pm25_aqi_avg is not null
)

select
    region_id,
    nuclear_quartile,
    round(min(nuclear_pct)::numeric,   2)     as nuclear_pct_min,
    round(max(nuclear_pct)::numeric,   2)     as nuclear_pct_max,
    round(avg(nuclear_pct)::numeric,   2)     as nuclear_pct_avg,
    round(avg(pm25_aqi_avg)::numeric,  2)     as avg_pm25_aqi,
    round(avg(fossil_pct)::numeric,    2)     as avg_fossil_pct,
    round(avg(temperature_c)::numeric, 2)     as avg_temperature_c,
    count(*)                                  as hour_count
from hours_with_data
group by 1, 2
order by 1, 2

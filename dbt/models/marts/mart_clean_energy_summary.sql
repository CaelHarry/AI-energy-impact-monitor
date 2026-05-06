{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_clean_energy_summary: daily clean energy mix by peak period
--
-- Aggregates the hourly fact table into (day, region_id, peak_period) buckets.
-- Answers question 4: how does the clean energy mix change during peak vs
-- off-peak hours across different regions?
--
-- hour_count exposes data completeness. A full day should have:
--   peak     = 7 hours (hours 14–20 UTC inclusive)
--   off_peak = 17 hours (all other hours)
-- Values below these thresholds indicate pipeline gaps for that day.
-- ─────────────────────────────────────────────────────────────────────────────

select
    date_trunc('day', hour)                     as day,
    region_id,
    peak_period,
    round(avg(clean_pct)::numeric,   2)         as avg_clean_pct,
    round(avg(fossil_pct)::numeric,  2)         as avg_fossil_pct,
    round(avg(nuclear_pct)::numeric, 2)         as avg_nuclear_pct,
    round(sum(total_mwh)::numeric,   2)         as total_mwh,
    count(*)                                    as hour_count
from {{ ref('mart_regional_hourly_metrics') }}
where
    clean_pct is not null
    or fossil_pct is not null
group by 1, 2, 3
order by 1 desc, 2, 3

{{
  config(materialized='ephemeral')
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- int_nuclear_by_region: reactor → region daily rollup
--
-- nuclear_status_raw is per reactor unit (e.g. "VOGTLE 3", "DIABLO CANYON 1").
-- This model aggregates to (region_id, day) — the granularity that joins
-- cleanly with hourly mart models via date truncation.
--
-- Reactors with null region_id (states outside the ERCOT/CAISO/PJM mapping)
-- are excluded. Only the three monitored regions are meaningful for correlation.
-- ─────────────────────────────────────────────────────────────────────────────

select
    date_trunc('day', time)             as day,
    region_id,
    avg(power_pct)                      as avg_nuclear_power_pct,
    min(power_pct)                      as min_nuclear_power_pct,
    max(power_pct)                      as max_nuclear_power_pct,
    count(distinct unit_name)           as reactor_unit_count,
    count(*)                            as reading_count
from {{ ref('stg_nuclear_status') }}
where
    region_id is not null
group by 1, 2

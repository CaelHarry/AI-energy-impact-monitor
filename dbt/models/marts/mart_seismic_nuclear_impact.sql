{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_seismic_nuclear_impact: seismic events → nuclear output change
--
-- For each M3.0+ seismic event near a monitored grid region, compares average
-- nuclear output in the 7 days before vs 7 days after the event.
--
-- Significant seismic events can trigger automatic reactor SCRAM procedures or
-- precautionary output reductions — both observable in nuclear_status_raw over
-- the following days. This is the only model that uses seismic_raw analytically.
--
-- nuclear_pct_delta: positive = output increased after the event (unusual),
-- negative = output dropped (expected response to a significant quake).
--
-- Null values for nuclear_pct_7d_before or _after indicate the 7-day window
-- falls outside the available nuclear data range (early pipeline startup or
-- events near the edge of the retention window).
-- ─────────────────────────────────────────────────────────────────────────────

with seismic_events as (
    select
        time::date          as event_date,
        event_id,
        magnitude,
        region_id,
        place
    from {{ ref('stg_seismic') }}
    where
        magnitude >= 3.0
        and region_id is not null
),

nuclear_daily as (
    select * from {{ ref('int_nuclear_by_region') }}
),

nuclear_before as (
    select
        e.event_id,
        e.region_id,
        e.event_date,
        e.magnitude,
        e.place,
        round(avg(n.avg_nuclear_power_pct)::numeric, 2) as nuclear_pct_7d_before,
        count(n.day)                                    as days_before_count
    from seismic_events e
    join nuclear_daily n
        on  n.region_id = e.region_id
        and n.day >= (e.event_date::timestamp - interval '7 days')
        and n.day <  e.event_date::timestamp
    group by e.event_id, e.region_id, e.event_date, e.magnitude, e.place
),

nuclear_after as (
    select
        e.event_id,
        round(avg(n.avg_nuclear_power_pct)::numeric, 2) as nuclear_pct_7d_after,
        count(n.day)                                    as days_after_count
    from seismic_events e
    join nuclear_daily n
        on  n.region_id = e.region_id
        and n.day >  e.event_date::timestamp
        and n.day <= (e.event_date::timestamp + interval '7 days')
    group by e.event_id
)

select
    b.event_date,
    b.region_id,
    b.event_id,
    b.magnitude,
    b.place,
    b.nuclear_pct_7d_before,
    a.nuclear_pct_7d_after,
    a.nuclear_pct_7d_after - b.nuclear_pct_7d_before   as nuclear_pct_delta,
    b.days_before_count,
    a.days_after_count
from nuclear_before b
left join nuclear_after a on b.event_id = a.event_id
order by b.event_date desc

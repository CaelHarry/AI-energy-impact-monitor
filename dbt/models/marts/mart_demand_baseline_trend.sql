{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- mart_demand_baseline_trend: long-term demand growth by region
--
-- Monthly average and peak demand per region, plus month-over-month delta and
-- percentage change. This is the direct test of the project's core thesis:
-- are data-center-dense regions (ERCOT, PJM) showing sustained upward demand
-- growth that could be attributed to AI infrastructure buildout?
--
-- A single month of data produces one row with null MoM columns (no prior month
-- to compare against). The trend becomes meaningful after 3+ months.
--
-- hour_count exposes data completeness — a fully observed month has ~720 hours
-- per region. Values well below 720 indicate significant pipeline gaps.
-- ─────────────────────────────────────────────────────────────────────────────

with monthly as (
    select
        date_trunc('month', hour)                   as month,
        region_id,
        round(avg(demand_avg_mw)::numeric, 2)       as avg_demand_mw,
        round(max(demand_max_mw)::numeric, 2)       as peak_demand_mw,
        round(avg(demand_baseline_mw)::numeric, 2)  as avg_baseline_mw,
        count(*)                                    as hour_count
    from {{ ref('mart_regional_hourly_metrics') }}
    where demand_avg_mw is not null
    group by 1, 2
)

select
    month,
    region_id,
    avg_demand_mw,
    peak_demand_mw,
    avg_baseline_mw,
    hour_count,

    -- Month-over-month absolute change
    avg_demand_mw - lag(avg_demand_mw) over (
        partition by region_id
        order by month
    )                                               as mom_demand_delta_mw,

    -- Month-over-month percentage change
    round(
        100.0 * (
            avg_demand_mw - lag(avg_demand_mw) over (
                partition by region_id order by month
            )
        ) / nullif(
            lag(avg_demand_mw) over (partition by region_id order by month), 0
        ),
        2
    )                                               as mom_demand_pct_change

from monthly
order by region_id, month

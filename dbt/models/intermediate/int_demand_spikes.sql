{{
  config(materialized='ephemeral')
}}

-- ─────────────────────────────────────────────────────────────────────────────
-- int_demand_spikes: demand anomaly detection
--
-- Adds a 7-day rolling baseline and standard deviation to every row from
-- int_regional_hourly, then flags hours where actual demand is anomalously
-- high relative to the same hour-of-week in the trailing 7 days.
--
-- Window: partitioned by (region_id, day-of-week, hour-of-day) so Monday 3pm
-- is compared against previous Monday 3pm values — controlling for the normal
-- weekly demand cycle rather than comparing against all consecutive hours.
--
-- Spike: demand_anomaly_mw > 2 × demand_stddev_mw (2-sigma threshold).
-- Returns false (not null) for the first 7 days when baseline is unavailable.
-- ─────────────────────────────────────────────────────────────────────────────

with spine as (
    select * from {{ ref('int_regional_hourly') }}
),

with_stats as (
    select
        *,
        avg(demand_avg_mw) over (
            partition by
                region_id,
                extract(dow  from hour),   -- day of week: 0=Sun, 6=Sat
                extract(hour from hour)    -- hour of day: 0–23
            order by hour
            rows between 168 preceding and 1 preceding  -- 7 days × 24 h = 168 rows
        )                                       as demand_baseline_mw,

        stddev_pop(demand_avg_mw) over (
            partition by
                region_id,
                extract(dow  from hour),
                extract(hour from hour)
            order by hour
            rows between 168 preceding and 1 preceding
        )                                       as demand_stddev_mw

    from spine
    where demand_avg_mw is not null
)

select
    *,
    demand_avg_mw - demand_baseline_mw          as demand_anomaly_mw,
    case
        when demand_baseline_mw is null         then false   -- insufficient history
        when demand_stddev_mw   is null         then false
        when demand_stddev_mw   = 0             then false   -- constant baseline edge-case
        when (demand_avg_mw - demand_baseline_mw) > (2.0 * demand_stddev_mw)
                                                then true
        else false
    end                                         as is_demand_spike
from with_stats

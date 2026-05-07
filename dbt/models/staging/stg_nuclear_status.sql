-- Daily per-reactor NRC power output as % of licensed capacity.
-- The NRC occasionally reports values just above 100 due to measurement
-- methodology, so the upper bound is 110. Reactors with no reading are excluded
-- to avoid skewing regional averages.

select
    time,
    region_id,
    unit_name,
    power_pct,
    status_code,
    state_code,
    operator,
    ingested_at
from {{ source('raw', 'nuclear_status_raw') }}
where
    power_pct is not null
    and power_pct between 0 and 110
    and region_id is not null

-- Fails if any reactor power_pct is outside the valid 0–110% range.
-- The staging filter should catch this; this test is a belt-and-suspenders guard.

select *
from {{ ref('stg_nuclear_status') }}
where power_pct < 0 or power_pct > 110

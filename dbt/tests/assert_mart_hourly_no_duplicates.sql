-- Fails if any (hour, region_id) combination appears more than once in the
-- primary fact table. Duplicate rows would corrupt all downstream aggregations.

select hour, region_id, count(*) as n
from {{ ref('mart_regional_hourly_metrics') }}
group by 1, 2
having count(*) > 1

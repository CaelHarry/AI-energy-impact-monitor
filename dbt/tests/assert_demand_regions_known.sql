-- Fails if any demand row has a region_id not present in grid_regions.
-- Guards against DAG bugs that write unknown region codes to grid_demand_raw.

select d.*
from {{ ref('stg_grid_demand') }} d
left join {{ source('raw', 'grid_regions') }} r on d.region_id = r.region_id
where r.region_id is null

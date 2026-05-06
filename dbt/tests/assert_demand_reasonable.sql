-- Fails if any demand reading exceeds 250,000 MW.
-- ERCOT peak ~80 GW, CAISO ~50 GW, PJM ~170 GW. Values above 250,000 MW
-- indicate a unit error (MW vs GW) or a corrupted API response.

select *
from {{ ref('stg_grid_demand') }}
where demand_mw > 250000

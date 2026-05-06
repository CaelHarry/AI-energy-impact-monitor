-- Real-time grid load (5-minute cadence) from ERCOT, CAISO, and PJM.
-- Zero and negative demand values represent sensor/feed errors and are excluded.

select
    time,
    region_id,
    demand_mw,
    forecast_mw,
    net_load_mw,
    source,
    ingested_at
from {{ source('raw', 'grid_demand_raw') }}
where
    demand_mw is not null
    and demand_mw > 0

-- Station-level AQI readings from AirNow (EPA).
-- Filters out null and negative AQI values (AirNow uses -1 as a no-data sentinel).

select
    time,
    region_id,
    station_id,
    lat,
    lon,
    parameter,
    aqi,
    concentration,
    unit,
    category,
    ingested_at
from {{ source('raw', 'aqi_raw') }}
where
    aqi is not null
    and aqi >= 0

-- USGS seismic events M1.0+. The USGS feed is global; region_id is only set
-- when the epicenter falls near a monitored grid region. Both null-region (global)
-- and region-assigned events pass through — mart models filter as needed.

select
    time,
    event_id,
    magnitude,
    magnitude_type,
    depth_km,
    lat,
    lon,
    place,
    alert_level,
    tsunami,
    region_id,
    ingested_at
from {{ source('raw', 'seismic_raw') }}
where
    magnitude >= 1.0

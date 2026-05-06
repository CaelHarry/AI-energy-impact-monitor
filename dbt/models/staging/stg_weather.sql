-- Hourly weather observations and forecasts from Open-Meteo per region centroid.
-- Drops rows missing the primary temperature reading; all other fields may be null.

select
    time,
    region_id,
    temperature_2m,
    apparent_temperature,
    relative_humidity,
    precipitation,
    wind_speed_10m,
    wind_direction_10m,
    cloud_cover,
    surface_pressure,
    is_forecast,
    ingested_at
from {{ source('raw', 'weather_raw') }}
where
    temperature_2m is not null

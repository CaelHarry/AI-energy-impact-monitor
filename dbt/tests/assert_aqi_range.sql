-- Fails if any AQI reading exceeds 500 (the EPA scale maximum).
-- Values above 500 indicate an upstream API bug or data corruption.

select *
from {{ ref('stg_aqi') }}
where aqi > 500

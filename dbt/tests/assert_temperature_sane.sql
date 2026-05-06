-- Fails if any temperature reading is outside the plausible US range.
-- Values below -50°C or above 60°C indicate a sensor error or API bug.

select *
from {{ ref('stg_weather') }}
where temperature_2m < -50 or temperature_2m > 60

-- Hourly EIA generation mix by fuel type per region.
-- Rows with null or zero total_mwh are excluded because the percentage columns
-- (nuclear_pct, fossil_pct, clean_pct) are derived from total_mwh at ingest
-- and would be meaningless without a valid denominator.

select
    time,
    region_id,
    natural_gas_mwh,
    coal_mwh,
    nuclear_mwh,
    wind_mwh,
    solar_mwh,
    hydro_mwh,
    other_mwh,
    total_mwh,
    nuclear_pct,
    fossil_pct,
    clean_pct,
    ingested_at
from {{ source('raw', 'grid_generation_raw') }}
where
    total_mwh is not null
    and total_mwh > 0

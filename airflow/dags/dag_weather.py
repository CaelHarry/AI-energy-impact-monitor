# airflow/dags/dag_weather.py
#
# Fetches hourly weather observations from Open-Meteo for each grid region centroid.
# Schedule: 4 minutes past each hour (staggered to avoid top-of-hour pile-up)
#
# Source: https://api.open-meteo.com — free, no API key required.

import logging
from datetime import datetime, timezone, timedelta

import httpx
from airflow.decorators import dag, task
from pydantic import ValidationError

from pipeline.db import upsert
from pipeline.models import WeatherRow

log = logging.getLogger(__name__)

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# One request per region centroid
REGIONS = {
    "ERCOT": {"lat": 31.9686, "lon": -99.9018},
    "CAISO": {"lat": 36.7783, "lon": -119.4179},
    "PJM":   {"lat": 39.9526, "lon": -75.1652},
}

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
    "surface_pressure",
]


@dag(
    dag_id="dag_weather",
    schedule="4 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["weather", "open-meteo", "hourly"],
    doc_md="""
    ### Open-Meteo weather
    Fetches hourly weather for ERCOT, CAISO, and PJM region centroids.
    Pulls the past 2 hours + next 6 hours to catch any late-arriving observations.
    Written to: `weather_raw`
    """,
)
def dag_weather():

    @task()
    def fetch_weather() -> list[dict]:
        """
        Fetch hourly weather for all regions.
        Returns a list of raw API response dicts, one per region.
        """
        results = []
        now = datetime.now(timezone.utc)
        start = (now - timedelta(hours=2)).strftime("%Y-%m-%d")
        end   = (now + timedelta(hours=6)).strftime("%Y-%m-%d")

        with httpx.Client(timeout=30) as client:
            for region_id, coords in REGIONS.items():
                params = {
                    "latitude":         coords["lat"],
                    "longitude":        coords["lon"],
                    "hourly":           ",".join(HOURLY_VARIABLES),
                    "start_date":       start,
                    "end_date":         end,
                    "timezone":         "UTC",
                    "timeformat":       "iso8601",
                }
                log.info("fetching weather for %s", region_id)
                resp = client.get(BASE_URL, params=params)
                resp.raise_for_status()
                results.append({"region_id": region_id, "data": resp.json()})

        return results

    @task()
    def parse_and_validate(raw_results: list[dict]) -> list[dict]:
        """
        Open-Meteo returns parallel arrays — zip them into row dicts.
        Validate each row with the WeatherRow pydantic model.
        """
        rows = []
        bad  = 0
        now  = datetime.now(timezone.utc)

        for result in raw_results:
            region_id = result["region_id"]
            hourly    = result["data"].get("hourly", {})
            times     = hourly.get("time", [])

            for i, t in enumerate(times):
                row_time = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
                is_forecast = row_time > now

                try:
                    row = WeatherRow(
                        time=row_time,
                        region_id=region_id,
                        temperature_2m=hourly.get("temperature_2m", [None])[i],
                        apparent_temperature=hourly.get("apparent_temperature", [None])[i],
                        relative_humidity=hourly.get("relative_humidity_2m", [None])[i],
                        precipitation=hourly.get("precipitation", [None])[i],
                        wind_speed_10m=hourly.get("wind_speed_10m", [None])[i],
                        wind_direction_10m=hourly.get("wind_direction_10m", [None])[i],
                        cloud_cover=hourly.get("cloud_cover", [None])[i],
                        surface_pressure=hourly.get("surface_pressure", [None])[i],
                        is_forecast=is_forecast,
                    )
                    rows.append(row.to_db())
                except (ValidationError, IndexError, Exception) as e:
                    log.warning("skipping row t=%s region=%s: %s", t, region_id, e)
                    bad += 1

        log.info("parsed %d valid rows, %d skipped", len(rows), bad)
        return rows

    @task()
    def load(rows: list[dict]) -> int:
        """Upsert weather rows — update if forecast becomes observation."""
        if not rows:
            log.warning("no weather rows to load")
            return 0
        return upsert(
            table="weather_raw",
            rows=rows,
            conflict_cols=["time", "region_id"],
        )

    raw   = fetch_weather()
    valid = parse_and_validate(raw)
    load(valid)


dag_weather()

# airflow/dags/dag_airnow.py
#
# Fetches current AQI observations from AirNow (EPA) for each grid region.
# Schedule: every 30 minutes, staggered 2 minutes past the half-hour
#
# Source: https://docs.airnowapi.org
# Requires: AIRNOW_API_KEY in environment

import os
import logging
from datetime import datetime, timezone

import httpx
from airflow.decorators import dag, task
from pydantic import ValidationError

from pipeline.db import upsert
from pipeline.models import AqiRow

log = logging.getLogger(__name__)

BASE_URL = "https://www.airnowapi.org/aq/observation/latLong/current/"

AIRNOW_API_KEY = os.environ.get("AIRNOW_API_KEY", "")

# Bounding boxes per region [min_lat, min_lon, max_lat, max_lon]
REGIONS = {
    "ERCOT": {"lat": 31.9686, "lon": -99.9018, "distance": 200},
    "CAISO": {"lat": 36.7783, "lon": -119.4179, "distance": 200},
    "PJM":   {"lat": 39.9526, "lon": -75.1652,  "distance": 200},
}


@dag(
    dag_id="dag_airnow",
    schedule="2 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["aqi", "airnow", "epa"],
    doc_md="""
    ### AirNow AQI
    Fetches current AQI readings from AirNow (EPA) for monitoring stations
    within 200km of each grid region centroid.
    Parameters: PM2.5, PM10, O3, NO2, CO, SO2
    Written to: `aqi_raw`
    """,
)
def dag_airnow():

    @task()
    def fetch_aqi() -> list[dict]:
        """
        Fetch current AQI observations for all regions.
        AirNow returns all active monitoring stations within `distance` km
        of the given lat/lon.
        """
        if not AIRNOW_API_KEY:
            raise RuntimeError("AIRNOW_API_KEY is not set in the environment")

        results = []

        with httpx.Client(timeout=30) as client:
            for region_id, coords in REGIONS.items():
                params = {
                    "format":       "application/json",
                    "latitude":     coords["lat"],
                    "longitude":    coords["lon"],
                    "distance":     coords["distance"],
                    "API_KEY":      AIRNOW_API_KEY,
                }
                log.info("fetching AQI for %s", region_id)
                resp = client.get(BASE_URL, params=params)
                resp.raise_for_status()
                readings = resp.json()
                log.info("got %d readings for %s", len(readings), region_id)
                results.append({"region_id": region_id, "readings": readings})

        return results

    @task()
    def parse_and_validate(raw_results: list[dict]) -> list[dict]:
        """
        Validate each AQI reading.
        AirNow response fields: DateObserved, HourObserved, LocalTimeZone,
        ReportingArea, StateCode, Latitude, Longitude, ParameterName,
        AQI, Category.Name
        """
        rows = []
        bad  = 0

        for result in raw_results:
            region_id = result["region_id"]

            for reading in result["readings"]:
                # Build ISO timestamp from AirNow date + hour fields
                date_str = reading.get("DateObserved", "").strip()
                hour     = reading.get("HourObserved", 0)
                tz_str   = reading.get("LocalTimeZone", "UTC")

                try:
                    # Parse to UTC — AirNow hours are local time
                    # Simplified: treat as UTC (good enough for trend analysis)
                    time_str = f"{date_str}T{int(hour):02d}:00:00+00:00"
                    station_id = (
                        f"{reading.get('ReportingArea','')}"
                        f"_{reading.get('StateCode','')}"
                        f"_{reading.get('ParameterName','')}"
                    ).replace(" ", "_").upper()

                    row = AqiRow(
                        time=time_str,
                        region_id=region_id,
                        station_id=station_id,
                        lat=reading.get("Latitude"),
                        lon=reading.get("Longitude"),
                        parameter=reading.get("ParameterName", "").strip(),
                        aqi=reading.get("AQI"),
                        category=reading.get("Category", {}).get("Name"),
                    )
                    rows.append(row.to_db())
                except (ValidationError, Exception) as e:
                    log.warning("skipping reading %s: %s", reading, e)
                    bad += 1

        log.info("parsed %d valid rows, %d skipped", len(rows), bad)
        return rows

    @task()
    def load(rows: list[dict]) -> int:
        """Upsert AQI rows — update if a corrected reading arrives."""
        if not rows:
            log.warning("no AQI rows to load")
            return 0
        return upsert(
            table="aqi_raw",
            rows=rows,
            conflict_cols=["time", "station_id", "parameter"],
        )

    raw   = fetch_aqi()
    valid = parse_and_validate(raw)
    load(valid)


dag_airnow()

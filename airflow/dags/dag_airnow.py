# airflow/dags/dag_airnow.py
#
# Fetches hourly AQI observations from the AirNow /aq/data/ API.
# Queries by bounding box for each grid region (ERCOT, CAISO, PJM).
# Schedule: hourly (2 minutes past the hour)
#
# Source: https://www.airnowapi.org/aq/data/
# Requires: AIRNOW_API_KEY environment variable

import logging
import os
from datetime import datetime, timezone

import httpx
from airflow.decorators import dag, task
from pydantic import ValidationError

from pipeline.db import upsert
from pipeline.models import AqiRow

log = logging.getLogger(__name__)

BASE_URL = "https://www.airnowapi.org/aq/data/"
PARAMETERS = "PM25,OZONE,PM10,CO,NO2,SO2"

# Bounding boxes: "minLon,minLat,maxLon,maxLat"
REGION_BBOXES: dict[str, str] = {
    "ERCOT": "-107.0,25.8,-93.5,36.5",    # Texas
    "CAISO": "-124.5,32.5,-114.0,42.0",   # California
    "PJM":   "-92.0,35.0,-74.0,47.0",     # Mid-Atlantic / Midwest
}


@dag(
    dag_id="dag_airnow",
    schedule="2 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["aqi", "airnow", "epa"],
    doc_md="""
    ### AirNow AQI — bounding-box data API
    Fetches hourly AQI observations from the AirNow /aq/data/ endpoint.
    Queries all monitoring stations within bounding boxes for ERCOT, CAISO, and PJM.
    Written to: `aqi_raw`
    """,
)
def dag_airnow():

    @task()
    def fetch_all_regions() -> list[dict]:
        """
        Fetch AQI observations for all three regions via /aq/data/ bounding-box queries.
        One HTTP request per region; individual failures are caught so others still run.
        """
        api_key = os.environ["AIRNOW_API_KEY"]
        now = datetime.now(timezone.utc)
        hour_str = now.strftime("%Y-%m-%dT%H")

        all_rows: list[dict] = []
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            for region_id, bbox in REGION_BBOXES.items():
                params = {
                    "startDate":  hour_str,
                    "endDate":    hour_str,
                    "parameters": PARAMETERS,
                    "BBOX":       bbox,
                    "dataType":   "A",
                    "format":     "application/json",
                    "verbose":    "1",
                    "API_KEY":    api_key,
                }
                try:
                    resp = client.get(BASE_URL, params=params)
                    resp.raise_for_status()
                    data = resp.json()
                    if not isinstance(data, list):
                        log.warning("AirNow %s: unexpected response shape", region_id)
                        continue
                    for item in data:
                        item["_region_id"] = region_id
                    all_rows.extend(data)
                    log.info("AirNow %s: fetched %d observations", region_id, len(data))
                except Exception as e:
                    log.error("AirNow fetch failed for %s: %s", region_id, e)

        log.info("fetched %d total observations across all regions", len(all_rows))
        return all_rows

    @task()
    def validate(raw_rows: list[dict]) -> list[dict]:
        """Validate each observation into AqiRow."""
        rows: list[dict] = []
        bad = 0
        for item in raw_rows:
            try:
                utc_str = item.get("UTC", "")
                obs_time = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)

                category = item.get("Category", {})
                category_name = (
                    category.get("Name") if isinstance(category, dict) else str(category) or None
                )

                aqi_val = item.get("AQI")
                row = AqiRow(
                    time=obs_time,
                    region_id=item.get("_region_id", ""),
                    station_id=item.get("FullAQSCode") or item.get("IntlAQSCode") or "",
                    lat=item.get("Latitude"),
                    lon=item.get("Longitude"),
                    parameter=item.get("Parameter", ""),
                    aqi=int(aqi_val) if aqi_val is not None and int(aqi_val) >= 0 else None,
                    concentration=item.get("RawConcentration") or item.get("Value"),
                    unit=item.get("Unit") or None,
                    category=category_name or None,
                )
                rows.append(row.to_db())
            except (ValidationError, Exception) as e:
                log.warning("skipping row %s: %s", item.get("FullAQSCode", "?"), e)
                bad += 1

        log.info("validated %d rows, %d skipped", len(rows), bad)
        return rows

    @task()
    def load(rows: list[dict]) -> int:
        """Upsert AQI rows — DO UPDATE so corrected readings overwrite stale ones."""
        if not rows:
            log.warning("no AQI rows to load")
            return 0
        return upsert(
            table="aqi_raw",
            rows=rows,
            conflict_cols=["time", "station_id", "parameter"],
        )

    raw   = fetch_all_regions()
    valid = validate(raw)
    load(valid)


dag_airnow()

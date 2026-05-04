# airflow/dags/dag_seismic.py
#
# Fetches M1.0+ seismic events from the USGS GeoJSON feed.
# Schedule: every 10 minutes
#
# Source: https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_hour.geojson
# Free, no authentication required.

import logging
from datetime import datetime, timezone

import httpx
from airflow.decorators import dag, task
from pydantic import ValidationError

from pipeline.db import upsert_ignore
from pipeline.models import SeismicRow

log = logging.getLogger(__name__)

USGS_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_hour.geojson"
)

# Grid regions defined as bounding boxes [min_lon, min_lat, max_lon, max_lat]
REGION_BOUNDS = {
    "ERCOT": (-107.0, 25.8, -93.5, 36.5),
    "CAISO": (-124.5, 32.5, -114.1, 42.0),
    "PJM":   (-84.0,  36.5, -73.5,  42.5),
}


def assign_region(lat: float, lon: float) -> str | None:
    """Return the grid region containing this lat/lon, or None."""
    for region, (min_lon, min_lat, max_lon, max_lat) in REGION_BOUNDS.items():
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            return region
    return None


@dag(
    dag_id="dag_seismic",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["seismic", "usgs"],
    doc_md="""
    ### USGS seismic feed
    Fetches M1.0+ earthquake events from the USGS 1-hour GeoJSON feed.
    Events are assigned to grid regions by bounding box.
    Written to: `seismic_raw`
    """,
)
def dag_seismic():

    @task()
    def fetch_usgs() -> dict:
        """Download the USGS GeoJSON feed and return parsed JSON."""
        log.info("fetching USGS seismic feed")
        with httpx.Client(timeout=20) as client:
            resp = client.get(USGS_URL)
            resp.raise_for_status()
        data = resp.json()
        log.info("fetched %d features", len(data.get("features", [])))
        return data

    @task()
    def parse_and_validate(geojson: dict) -> list[dict]:
        """
        Parse GeoJSON feature collection and validate each event.
        USGS properties: mag, place, time (unix ms), updated, tsunami,
                         magType, depth, alert
        """
        rows = []
        bad  = 0

        for feature in geojson.get("features", []):
            props = feature.get("properties", {})
            geom  = feature.get("geometry", {})
            coords = geom.get("coordinates", [None, None, None])

            lon, lat, depth_km = coords[0], coords[1], coords[2]

            try:
                row = SeismicRow(
                    time=props.get("time"),
                    event_id=feature.get("id"),
                    magnitude=props.get("mag"),
                    magnitude_type=props.get("magType"),
                    depth_km=depth_km,
                    lat=lat,
                    lon=lon,
                    place=props.get("place"),
                    alert_level=props.get("alert"),
                    tsunami=props.get("tsunami", 0),
                    region_id=assign_region(lat, lon) if lat and lon else None,
                )
                rows.append(row.to_db())
            except (ValidationError, Exception) as e:
                log.warning("skipping invalid event %s: %s", feature.get("id"), e)
                bad += 1

        log.info("parsed %d valid events, %d skipped", len(rows), bad)
        return rows

    @task()
    def load(rows: list[dict]) -> int:
        """Upsert events into seismic_raw — skip duplicates."""
        if not rows:
            log.info("no new seismic events")
            return 0
        return upsert_ignore(
            table="seismic_raw",
            rows=rows,
            conflict_cols=["event_id", "time"],
        )

    raw   = fetch_usgs()
    valid = parse_and_validate(raw)
    load(valid)


dag_seismic()

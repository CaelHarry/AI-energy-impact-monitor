# airflow/dags/dag_grid_demand.py
#
# Fetches hourly grid demand (load) for ERCOT, CAISO, and PJM from EIA Form 930.
# Schedule: 7 minutes past each hour (EIA publishes with ~1 hour lag)
#
# Source: https://api.eia.gov/v2/electricity/rto/region-data/data/
# Requires: EIA_API_KEY in environment

import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
from airflow.decorators import dag, task
from pydantic import ValidationError

from pipeline.db import upsert
from pipeline.models import GridDemandRow

log = logging.getLogger(__name__)

EIA_API_KEY = os.environ.get("EIA_API_KEY", "")
BASE_URL    = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

REGION_MAP = {
    "ERCO": "ERCOT",
    "CISO": "CAISO",
    "PJM":  "PJM",
}


@dag(
    dag_id="dag_grid_demand",
    schedule="7 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["grid", "demand", "ercot", "caiso", "pjm", "eia"],
    doc_md="""
    ### Grid demand — ERCOT / CAISO / PJM via EIA Form 930
    Fetches hourly electricity demand (load) in MWh for all three regions
    from the EIA API v2. Pulls a 3-hour window to catch late-arriving corrections.
    EIA publishes with ~1 hour lag; runs at 7 minutes past each hour.
    Written to: `grid_demand_raw`
    """,
)
def dag_grid_demand():

    @task()
    def fetch_demand() -> list[dict]:
        """Fetch the last 3 hours of demand (D) and day-ahead forecast (DF) for all regions."""
        if not EIA_API_KEY:
            raise RuntimeError("EIA_API_KEY is not set in the environment")

        now   = datetime.now(timezone.utc)
        start = (now - timedelta(hours=3)).strftime("%Y-%m-%dT%H")

        params = {
            "api_key":              EIA_API_KEY,
            "frequency":            "hourly",
            "data[0]":              "value",
            "facets[respondent][]": list(REGION_MAP.keys()),
            "facets[type][]":       ["D", "DF"],  # D = Demand, DF = Day-ahead forecast
            "start":                start,
            "sort[0][column]":      "period",
            "sort[0][direction]":   "desc",
            "length":               100,
            "offset":               0,
        }

        log.info("fetching EIA demand + forecast from %s", start)
        with httpx.Client(timeout=30) as client:
            resp = client.get(BASE_URL, params=params)
            resp.raise_for_status()

        rows = resp.json().get("response", {}).get("data", [])
        log.info("received %d EIA rows (D + DF)", len(rows))
        return rows

    @task()
    def validate(raw_rows: list[dict]) -> list[dict]:
        """
        Merge D (actual demand) and DF (forecast) rows by (period, respondent)
        into a single row per (time, region_id) with both demand_mw and forecast_mw.
        """
        demand: dict[tuple, float]   = {}
        forecast: dict[tuple, float] = {}
        bad = 0

        for row in raw_rows:
            respondent = row.get("respondent", "")
            region_id  = REGION_MAP.get(respondent)
            period     = row.get("period", "")
            value      = row.get("value")
            row_type   = row.get("type", "")

            if not region_id or not period or value is None:
                bad += 1
                continue

            key = (period, region_id)
            try:
                if row_type == "D":
                    demand[key] = float(value)
                elif row_type == "DF":
                    forecast[key] = float(value)
            except (ValueError, TypeError) as e:
                log.warning("skipping row %s: %s", row, e)
                bad += 1

        all_keys = demand.keys() | forecast.keys()
        rows = []
        for key in all_keys:
            period, region_id = key
            if key not in demand:
                continue  # skip forecast-only rows with no actual demand
            try:
                validated = GridDemandRow(
                    time=period,
                    region_id=region_id,
                    demand_mw=demand[key],
                    forecast_mw=forecast.get(key),
                    source="eia",
                )
                rows.append(validated.to_db())
            except (ValidationError, Exception) as e:
                log.warning("skipping merged row %s: %s", key, e)
                bad += 1

        log.info("validated %d merged rows, %d skipped", len(rows), bad)
        return rows

    @task()
    def load(rows: list[dict]) -> int:
        """Upsert demand rows into grid_demand_raw."""
        if not rows:
            log.warning("no demand rows to load")
            return 0
        return upsert(
            table="grid_demand_raw",
            rows=rows,
            conflict_cols=["time", "region_id", "source"],
        )

    raw   = fetch_demand()
    valid = validate(raw)
    load(valid)


dag_grid_demand()

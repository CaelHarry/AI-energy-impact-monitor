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
        """Fetch the last 3 hours of demand data for ERCOT, CAISO, and PJM."""
        if not EIA_API_KEY:
            raise RuntimeError("EIA_API_KEY is not set in the environment")

        now   = datetime.now(timezone.utc)
        start = (now - timedelta(hours=3)).strftime("%Y-%m-%dT%H")

        params = {
            "api_key":              EIA_API_KEY,
            "frequency":            "hourly",
            "data[0]":              "value",
            "facets[respondent][]": list(REGION_MAP.keys()),
            "facets[type][]":       ["D"],   # D = Demand
            "start":                start,
            "sort[0][column]":      "period",
            "sort[0][direction]":   "desc",
            "length":               50,
            "offset":               0,
        }

        log.info("fetching EIA demand from %s", start)
        with httpx.Client(timeout=30) as client:
            resp = client.get(BASE_URL, params=params)
            resp.raise_for_status()

        rows = resp.json().get("response", {}).get("data", [])
        log.info("received %d EIA demand rows", len(rows))
        return rows

    @task()
    def validate(raw_rows: list[dict]) -> list[dict]:
        """Parse and validate each EIA demand row."""
        rows = []
        bad  = 0
        for row in raw_rows:
            respondent = row.get("respondent", "")
            region_id  = REGION_MAP.get(respondent)
            period     = row.get("period", "")
            value      = row.get("value")

            if not region_id or not period or value is None:
                bad += 1
                continue

            try:
                raw = {
                    "time":      period,
                    "region_id": region_id,
                    "demand_mw": float(value),
                    "source":    "eia",
                }
                validated = GridDemandRow(**raw)
                rows.append(validated.to_db())
            except (ValidationError, Exception) as e:
                log.warning("skipping demand row %s: %s", row, e)
                bad += 1

        log.info("validated %d rows, %d skipped", len(rows), bad)
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

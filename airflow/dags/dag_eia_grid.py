# airflow/dags/dag_eia_grid.py
#
# Fetches hourly electricity generation by fuel type from the EIA API.
# Schedule: 6 minutes past each hour (staggered)
#
# Source: https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/
# Requires: EIA_API_KEY in environment

import os
import logging
from datetime import datetime, timezone, timedelta

import httpx
from airflow.decorators import dag, task
from pydantic import ValidationError

from pipeline.db import upsert
from pipeline.models import GridGenerationRow

log = logging.getLogger(__name__)

EIA_API_KEY = os.environ.get("EIA_API_KEY", "")
BASE_URL    = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"

# EIA respondent codes that map to our grid regions.
#
# CAISO (CISO) covers most of California but excludes the Los Angeles
# Department of Water and Power (LDWP), which operates its own balancing
# authority and accounts for ~20 % of California's total load. The main
# Silicon Valley data center cluster (PG&E territory) is inside CAISO, so
# the exclusion is acceptable for the AI demand thesis — but it means LA
# Basin heat events won't produce a matching demand spike in this data.
# To add full California coverage, include "LDWP": "LADWP" here and add a
# corresponding entry to the AirNow bounding boxes in dag_airnow.py.
#
# PJM is the most important region for this project: Northern Virginia
# (Ashburn/Loudoun County) hosts ~35 % of global colocation capacity and
# sits squarely inside PJM's balancing authority with no coverage gaps.
REGION_MAP = {
    "ERCO": "ERCOT",   # ERCOT — Texas; covers Austin/Dallas data center corridor
    "CISO": "CAISO",   # CAISO — California (excl. LADWP/LA); covers Silicon Valley
    "PJM":  "PJM",     # PJM — Mid-Atlantic/Midwest; covers Northern Virginia cluster
}

# EIA fuel type codes → our column names
FUEL_MAP = {
    "NG":  "natural_gas_mwh",
    "COL": "coal_mwh",
    "NUC": "nuclear_mwh",
    "WND": "wind_mwh",
    "SUN": "solar_mwh",
    "WAT": "hydro_mwh",
    "OTH": "other_mwh",
}


@dag(
    dag_id="dag_eia_grid",
    schedule="6 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["grid", "eia", "generation", "hourly"],
    doc_md="""
    ### EIA grid generation mix
    Fetches hourly electricity generation by fuel type (coal, gas, nuclear,
    wind, solar, hydro) for ERCOT, CAISO, and PJM from the EIA API v2.
    Derives nuclear_pct, fossil_pct, and clean_pct at ingest time.
    Written to: `grid_generation_raw`
    """,
)
def dag_eia_grid():

    @task()
    def fetch_eia() -> list[dict]:
        """
        Fetch the last 3 hours of generation mix data for all regions.
        EIA publishes data with ~1 hour delay so we pull a 3-hour window
        to catch any late-arriving corrections.
        """
        if not EIA_API_KEY:
            raise RuntimeError("EIA_API_KEY is not set in the environment")

        now   = datetime.now(timezone.utc)
        start = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H")

        params = {
            "api_key":              EIA_API_KEY,
            "frequency":            "hourly",
            "data[0]":              "value",
            "facets[respondent][]": list(REGION_MAP.keys()),
            "start":                start,
            "sort[0][column]":      "period",
            "sort[0][direction]":   "desc",
            "length":               1500,
            "offset":               0,
        }

        log.info("fetching EIA generation mix from %s", start)
        with httpx.Client(timeout=30) as client:
            resp = client.get(BASE_URL, params=params)
            resp.raise_for_status()

        data = resp.json()
        rows = data.get("response", {}).get("data", [])
        log.info("received %d EIA rows", len(rows))
        return rows

    @task()
    def parse_and_validate(raw_rows: list[dict]) -> list[dict]:
        """
        EIA returns one row per (period, respondent, fuel_type).
        Pivot into one row per (period, respondent) with fuel columns.
        Then validate with GridGenerationRow pydantic model.
        """
        # Group by (period, respondent)
        grouped: dict[tuple, dict] = {}

        for row in raw_rows:
            period     = row.get("period", "")
            respondent = row.get("respondent", "")
            fuel_type  = row.get("fueltype", "")
            value      = row.get("value")
            region_id  = REGION_MAP.get(respondent)

            if not region_id or not period:
                continue

            key = (period, region_id)
            if key not in grouped:
                grouped[key] = {"time": period, "region_id": region_id}

            col = FUEL_MAP.get(fuel_type)
            if col and value is not None:
                try:
                    grouped[key][col] = float(value)
                except (ValueError, TypeError):
                    pass

        # Compute totals and validate
        result = []
        bad    = 0

        for (period, region_id), data in grouped.items():
            fuels = [
                data.get("natural_gas_mwh", 0) or 0,
                data.get("coal_mwh", 0) or 0,
                data.get("nuclear_mwh", 0) or 0,
                data.get("wind_mwh", 0) or 0,
                data.get("solar_mwh", 0) or 0,
                data.get("hydro_mwh", 0) or 0,
                data.get("other_mwh", 0) or 0,
            ]
            data["total_mwh"] = sum(fuels)

            try:
                row = GridGenerationRow(**data)
                result.append(row.to_db())
            except (ValidationError, Exception) as e:
                log.warning("skipping row period=%s region=%s: %s", period, region_id, e)
                bad += 1

        log.info("pivoted %d valid rows, %d skipped", len(result), bad)
        return result

    @task()
    def load(rows: list[dict]) -> int:
        """Upsert generation mix rows."""
        if not rows:
            log.warning("no EIA rows to load")
            return 0
        return upsert(
            table="grid_generation_raw",
            rows=rows,
            conflict_cols=["time", "region_id"],
        )

    raw   = fetch_eia()
    valid = parse_and_validate(raw)
    load(valid)


dag_eia_grid()

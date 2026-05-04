# airflow/dags/dag_grid_demand.py
#
# Fetches real-time grid load from ERCOT, CAISO, and PJM.
# Schedule: every 5 minutes
#
# Sources (all public, no auth required):
#   ERCOT: https://www.ercot.com/api/1/services/read/dashboards/current-grid-conditions
#   CAISO: https://oasis.caiso.com/oasisapi/SingleZip (RTLOAD report)
#   PJM:   https://dataminer2.pjm.com/feed/inst_load/export

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
from airflow.decorators import dag, task
from pydantic import ValidationError

from pipeline.db import upsert
from pipeline.models import GridDemandRow

log = logging.getLogger(__name__)


# ── ERCOT ─────────────────────────────────────────────────────────

def fetch_ercot(client: httpx.Client) -> list[dict]:
    """
    ERCOT grid conditions dashboard API.
    Returns current system-wide load in MW.
    """
    url = "https://www.ercot.com/api/1/services/read/dashboards/current-grid-conditions"
    try:
        resp = client.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        # ERCOT response structure varies — extract load from known keys
        load_mw = (
            data.get("currentLoad")
            or data.get("systemLoad")
            or data.get("load")
        )
        if load_mw is None:
            log.warning("ERCOT: could not find load value in response")
            return []

        return [{
            "time":      datetime.now(timezone.utc).replace(second=0, microsecond=0),
            "region_id": "ERCOT",
            "demand_mw": float(load_mw),
            "source":    "ercot",
        }]
    except Exception as e:
        log.error("ERCOT fetch failed: %s", e)
        return []


# ── CAISO ─────────────────────────────────────────────────────────

def fetch_caiso(client: httpx.Client) -> list[dict]:
    """
    CAISO OASIS API — real-time system load (RTLOAD).
    """
    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    start  = now_pt.strftime("%Y%m%dT%H:%M-0000")
    end    = now_pt.strftime("%Y%m%dT%H:%M-0000")

    url = "https://oasis.caiso.com/oasisapi/SingleZip"
    params = {
        "queryname":    "RT_LOAD",
        "startdatetime": start,
        "enddatetime":   end,
        "market_run_id": "RTM",
        "resultformat":  "6",   # JSON
    }
    try:
        resp = client.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("OASISReport", {}).get("MessagePayload", {}).get("RTO", {})
        load_data = rows.get("LOAD_VALUES", {}).get("LOAD", [])

        if isinstance(load_data, dict):
            load_data = [load_data]

        results = []
        for item in load_data:
            try:
                results.append({
                    "time":      datetime.now(timezone.utc).replace(second=0, microsecond=0),
                    "region_id": "CAISO",
                    "demand_mw": float(item.get("MW", 0)),
                    "source":    "caiso",
                })
            except (ValueError, TypeError):
                continue
        return results
    except Exception as e:
        log.error("CAISO fetch failed: %s", e)
        return []


# ── PJM ───────────────────────────────────────────────────────────

def fetch_pjm(client: httpx.Client) -> list[dict]:
    """
    PJM DataMiner2 API — instantaneous load.
    """
    url = "https://dataminer2.pjm.com/feed/inst_load/export"
    params = {"startRow": 1, "endRow": 1}
    try:
        resp = client.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        items = data if isinstance(data, list) else data.get("items", [])
        results = []
        for item in items[:1]:
            try:
                results.append({
                    "time":        datetime.now(timezone.utc).replace(second=0, microsecond=0),
                    "region_id":   "PJM",
                    "demand_mw":   float(item.get("actual_load", item.get("load", 0))),
                    "forecast_mw": float(item.get("forecast_load", 0)) or None,
                    "source":      "pjm",
                })
            except (ValueError, TypeError):
                continue
        return results
    except Exception as e:
        log.error("PJM fetch failed: %s", e)
        return []


# ── DAG ───────────────────────────────────────────────────────────

@dag(
    dag_id="dag_grid_demand",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["grid", "demand", "ercot", "caiso", "pjm"],
    doc_md="""
    ### Grid demand — ERCOT / CAISO / PJM
    Fetches real-time grid load in MW from all three major grid operators.
    Runs every 5 minutes. Each operator is fetched independently so a
    single operator failure does not block the others.
    Written to: `grid_demand_raw`
    """,
)
def dag_grid_demand():

    @task()
    def fetch_all_operators() -> list[dict]:
        """
        Fetch demand from all three operators in a single HTTP session.
        Individual operator failures are caught and logged — they return []
        so the DAG continues with whatever data was successfully fetched.
        """
        rows = []
        with httpx.Client(timeout=20, follow_redirects=True) as client:
            rows += fetch_ercot(client)
            rows += fetch_caiso(client)
            rows += fetch_pjm(client)

        log.info("fetched %d demand rows across all operators", len(rows))
        return rows

    @task()
    def validate(raw_rows: list[dict]) -> list[dict]:
        """Validate each demand row with the GridDemandRow pydantic model."""
        rows = []
        bad  = 0
        for raw in raw_rows:
            try:
                row = GridDemandRow(**raw)
                rows.append(row.to_db())
            except (ValidationError, Exception) as e:
                log.warning("skipping demand row %s: %s", raw, e)
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

    raw   = fetch_all_operators()
    valid = validate(raw)
    load(valid)


dag_grid_demand()

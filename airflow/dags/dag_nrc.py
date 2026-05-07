# airflow/dags/dag_nrc.py
#
# Fetches the NRC daily reactor status report and upserts into nuclear_status_raw.
# Schedule: 08:00 UTC daily (report is published overnight US Eastern time)
#
# Source: https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/
# Format: pipe-delimited text file, one row per reactor unit

import os
import logging
import csv
import io
from datetime import datetime, timezone

import httpx
from airflow.decorators import dag, task
from pydantic import ValidationError

from pipeline.db import upsert
from pipeline.models import NuclearStatusRow

log = logging.getLogger(__name__)

NRC_URL = os.environ.get(
    "NRC_STATUS_URL",
    "https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/"
    "powerreactorstatusforlast365days.txt",
)

# Direct plant-name → region mapping.
# The NRC pipe-delimited file has no state/region column; state must be inferred
# from the plant name. Only plants inside ERCOT, CAISO, and PJM footprints are
# mapped — all others are left as None and excluded from dbt models.
UNIT_TO_REGION: dict[str, str] = {
    # ERCOT — Texas
    "Comanche Peak 1": "ERCOT",
    "Comanche Peak 2": "ERCOT",
    "South Texas 1":   "ERCOT",
    "South Texas 2":   "ERCOT",
    # CAISO — California
    "Diablo Canyon 1": "CAISO",
    "Diablo Canyon 2": "CAISO",
    # PJM — Mid-Atlantic / Midwest (ComEd IL, Dominion VA, FirstEnergy OH/PA, PSEG NJ, AEP MI)
    "Beaver Valley 1":  "PJM",
    "Beaver Valley 2":  "PJM",
    "Braidwood 1":      "PJM",
    "Braidwood 2":      "PJM",
    "Byron 1":          "PJM",
    "Byron 2":          "PJM",
    "Calvert Cliffs 1": "PJM",
    "Calvert Cliffs 2": "PJM",
    "Clinton":          "PJM",
    "D.C. Cook 1":      "PJM",
    "D.C. Cook 2":      "PJM",
    "Davis-Besse":      "PJM",
    "Dresden 2":        "PJM",
    "Dresden 3":        "PJM",
    "Hope Creek 1":     "PJM",
    "LaSalle 1":        "PJM",
    "LaSalle 2":        "PJM",
    "Limerick 1":       "PJM",
    "Limerick 2":       "PJM",
    "North Anna 1":     "PJM",
    "North Anna 2":     "PJM",
    "Peach Bottom 2":   "PJM",
    "Peach Bottom 3":   "PJM",
    "Perry 1":          "PJM",
    "Quad Cities 1":    "PJM",
    "Quad Cities 2":    "PJM",
    "Salem 1":          "PJM",
    "Salem 2":          "PJM",
    "Surry 1":          "PJM",
    "Surry 2":          "PJM",
    "Susquehanna 1":    "PJM",
    "Susquehanna 2":    "PJM",
}


@dag(
    dag_id="dag_nrc_reactor_status",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["nuclear", "nrc", "daily"],
    doc_md="""
    ### NRC reactor status
    Fetches the NRC daily power reactor status report.
    Each row represents one reactor unit's output as a % of licensed capacity.
    Written to: `nuclear_status_raw`
    """,
)
def dag_nrc_reactor_status():

    @task()
    def fetch_nrc() -> str:
        """Download the pipe-delimited NRC status file and return as a string."""
        log.info("fetching NRC reactor status from %s", NRC_URL)
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(NRC_URL)
            resp.raise_for_status()
        log.info("fetched %d bytes", len(resp.content))
        return resp.text

    @task()
    def parse_and_validate(raw_text: str) -> list[dict]:
        """
        Parse the pipe-delimited file and validate each row with pydantic.
        The NRC file uses | as delimiter with a header row.
        Columns: ReportDt | Unit | Power
        (additional columns vary by file version)
        """
        rows = []
        bad  = 0

        reader = csv.DictReader(
            io.StringIO(raw_text),
            delimiter="|",
        )

        for line in reader:
            # Strip whitespace from all keys and values
            line = {k.strip(): v.strip() for k, v in line.items() if k}

            report_dt   = line.get("ReportDt", "").strip()
            unit_name   = line.get("Unit", "").strip()
            power_pct   = line.get("Power", "").strip()
            status_code = line.get("RxType", "").strip() or None
            operator    = line.get("Licensee", "").strip() or None

            region_id  = UNIT_TO_REGION.get(unit_name)

            try:
                row = NuclearStatusRow(
                    time=report_dt,
                    unit_name=unit_name,
                    power_pct=power_pct if power_pct else None,
                    status_code=status_code,
                    state_code=None,
                    operator=operator,
                    region_id=region_id,
                )
                rows.append(row.to_db())
            except (ValidationError, Exception) as e:
                log.warning("skipping invalid row %s: %s", line, e)
                bad += 1

        log.info("parsed %d valid rows, %d skipped", len(rows), bad)
        return rows

    @task()
    def load(rows: list[dict]) -> int:
        """Upsert validated rows into nuclear_status_raw."""
        if not rows:
            log.warning("no rows to load")
            return 0
        return upsert(
            table="nuclear_status_raw",
            rows=rows,
            conflict_cols=["time", "unit_name"],
        )

    raw   = fetch_nrc()
    valid = parse_and_validate(raw)
    load(valid)


dag_nrc_reactor_status()

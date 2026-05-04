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

from pipeline.db import upsert_ignore
from pipeline.models import NuclearStatusRow

log = logging.getLogger(__name__)

NRC_URL = os.environ.get(
    "NRC_STATUS_URL",
    "https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/"
    "powerreactorstatusforlast365days.txt",
)

# Map NRC state codes to grid region IDs
STATE_TO_REGION = {
    "TX": "ERCOT",
    "CA": "CAISO",
    "VA": "PJM", "MD": "PJM", "DC": "PJM", "PA": "PJM",
    "NJ": "PJM", "DE": "PJM", "OH": "PJM", "WV": "PJM",
    "IL": "PJM", "MI": "PJM", "IN": "PJM", "KY": "PJM",
    "NC": "PJM", "TN": "PJM",
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

            # Derive state from unit name suffix where possible
            # NRC names are like "VOGTLE 3" — state lookup via separate mapping
            state_code = line.get("NRCRegion", "").strip() or None
            region_id  = STATE_TO_REGION.get(state_code)

            try:
                row = NuclearStatusRow(
                    time=report_dt,
                    unit_name=unit_name,
                    power_pct=power_pct if power_pct else None,
                    status_code=status_code,
                    state_code=state_code,
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
        return upsert_ignore(
            table="nuclear_status_raw",
            rows=rows,
            conflict_cols=["time", "unit_name"],
        )

    raw   = fetch_nrc()
    valid = parse_and_validate(raw)
    load(valid)


dag_nrc_reactor_status()

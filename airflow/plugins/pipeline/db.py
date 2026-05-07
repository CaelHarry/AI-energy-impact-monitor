import os
import logging
from datetime import datetime, timezone

import psycopg
from psycopg import sql

log = logging.getLogger(__name__)


def _conn_str() -> str:
    return os.environ["PIPELINE_DB_CONN"]


def _adapt(v):
    """Convert ISO datetime strings back to datetime objects for psycopg type safety.

    Airflow XCom serializes datetime objects to ISO strings. This restores them
    so psycopg sends the correct wire type to PostgreSQL TIMESTAMPTZ columns.
    """
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    return v


def upsert(table: str, rows: list[dict], conflict_cols: list[str]) -> int:
    """INSERT ... ON CONFLICT (conflict_cols) DO UPDATE SET all other columns."""
    if not rows:
        return 0

    cols = list(rows[0].keys())
    update_cols = [c for c in cols if c not in conflict_cols]

    if update_cols:
        updates = list(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in update_cols
        ) + [sql.SQL("ingested_at = NOW()")]
        stmt = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
            "ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        ).format(
            table=sql.Identifier(table),
            cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
            placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(cols)),
            conflict=sql.SQL(", ").join(map(sql.Identifier, conflict_cols)),
            updates=sql.SQL(", ").join(updates),
        )
    else:
        stmt = sql.SQL(
            "INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
            "ON CONFLICT ({conflict}) DO NOTHING"
        ).format(
            table=sql.Identifier(table),
            cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
            placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(cols)),
            conflict=sql.SQL(", ").join(map(sql.Identifier, conflict_cols)),
        )

    values = [[_adapt(row[c]) for c in cols] for row in rows]

    with psycopg.connect(_conn_str()) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt, values)

    log.info("upserted %d rows into %s", len(rows), table)
    return len(rows)


def upsert_ignore(table: str, rows: list[dict], conflict_cols: list[str]) -> int:
    """INSERT ... ON CONFLICT (conflict_cols) DO NOTHING."""
    if not rows:
        return 0

    cols = list(rows[0].keys())

    stmt = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
        "ON CONFLICT ({conflict}) DO NOTHING"
    ).format(
        table=sql.Identifier(table),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        placeholders=sql.SQL(", ").join([sql.Placeholder()] * len(cols)),
        conflict=sql.SQL(", ").join(map(sql.Identifier, conflict_cols)),
    )

    values = [[_adapt(row[c]) for c in cols] for row in rows]

    with psycopg.connect(_conn_str()) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt, values)

    log.info("upsert_ignore: %d rows into %s", len(rows), table)
    return len(rows)

#!/bin/bash
# ─────────────────────────────────────────────────────────────────
#  Runs automatically on first container start.
#  Reads AIRFLOW_DB_PASSWORD and PIPELINE_DB_PASSWORD from env.
# ─────────────────────────────────────────────────────────────────
set -e

echo ">>> Creating airflow database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
    CREATE DATABASE airflow
        WITH OWNER $POSTGRES_USER
        ENCODING 'UTF8'
        LC_COLLATE = 'en_US.utf8'
        LC_CTYPE   = 'en_US.utf8';
SQL

echo ">>> Creating pipeline database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
    CREATE DATABASE pipeline
        WITH OWNER $POSTGRES_USER
        ENCODING 'UTF8'
        LC_COLLATE = 'en_US.utf8'
        LC_CTYPE   = 'en_US.utf8';
SQL

echo ">>> Creating airflow user and granting privileges..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="airflow" <<-SQL
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    GRANT ALL ON SCHEMA public TO airflow;
    ALTER SCHEMA public OWNER TO airflow;
SQL

echo ">>> Creating pipeline user and granting privileges..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="pipeline" <<-SQL
    CREATE USER pipeline WITH PASSWORD '${PIPELINE_DB_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE pipeline TO pipeline;
    GRANT ALL ON SCHEMA public TO pipeline;
    ALTER SCHEMA public OWNER TO pipeline;
SQL

echo ">>> Enabling TimescaleDB on pipeline database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname="pipeline" <<-SQL
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    GRANT ALL ON SCHEMA public TO pipeline;
SQL

echo ">>> Init complete."
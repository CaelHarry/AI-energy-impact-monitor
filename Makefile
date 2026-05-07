# =================================================================
#  AI Energy Impact Monitor — Makefile
#  Usage: make <target>
# =================================================================

.PHONY: help up down reset logs schema diagram dbt-run dbt-test dbt-docs dbt-seed

# ── Default ───────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  AI Energy Impact Monitor — available commands"
	@echo ""
	@echo "  Stack"
	@echo "    make up          Start all services (postgres, airflow, pgadmin)"
	@echo "    make down        Stop all services, keep data volumes"
	@echo "    make reset       Full teardown including data volumes (destructive)"
	@echo "    make logs        Tail logs from all containers"
	@echo ""
	@echo "  Database"
	@echo "    make schema      Apply raw schema to pipeline database"
	@echo "    make diagram     Dump live schema and open dbdiagram.io"
	@echo ""
	@echo "  dbt"
	@echo "    make dbt-run     Run all dbt models"
	@echo "    make dbt-test    Run all dbt tests"
	@echo "    make dbt-docs    Generate and serve dbt documentation"
	@echo "    make dbt-seed    Load static seed data (datacenter_facilities)"
	@echo ""

# ── Stack ─────────────────────────────────────────────────────────
up:
	docker compose up -d postgres airflow-webserver airflow-scheduler pgadmin
	@echo ""
	@echo "  Services starting..."
	@echo "  Airflow UI → http://localhost:8080"
	@echo "  pgAdmin    → http://localhost:5050"
	@echo ""

down:
	docker compose down

reset:
	@echo "WARNING: This will delete all data volumes. Press Ctrl+C to cancel."
	@sleep 5
	docker compose down -v
	docker rmi apache/airflow:2.9.1-python3.11 || true

logs:
	docker compose logs -f

# ── Database ──────────────────────────────────────────────────────
schema:
	docker exec -i pipeline-postgres psql -U pipeline -d pipeline \
		< postgres/schema/01_raw_schema.sql
	@echo "Schema applied."

diagram:
	@echo "Dumping live schema to schema_live.sql..."
	docker exec pipeline-postgres pg_dump \
		--schema-only \
		--no-owner \
		--no-privileges \
		-U pipeline \
		-d pipeline \
		-t grid_regions \
		-t aqi_raw \
		-t weather_raw \
		-t grid_generation_raw \
		-t grid_demand_raw \
		-t nuclear_status_raw \
		-t seismic_raw \
		> schema_live.sql
	@echo "Live schema saved to schema_live.sql"
	@echo ""
	@echo "Opening dbdiagram.io — paste the contents of schema.dbml to render the ERD."
	open https://dbdiagram.io/d || xdg-open https://dbdiagram.io/d || \
		echo "Visit https://dbdiagram.io/d and paste schema.dbml"

# ── dbt ──────────────────────────────────────────────────────────
dbt-run:
	docker compose run --rm dbt run

dbt-test:
	docker compose run --rm dbt test

dbt-docs:
	docker compose run --rm -p 8081:8080 --entrypoint sh dbt \
		-c "dbt docs generate && dbt docs serve --port 8080"
	@echo "dbt docs → http://localhost:8081"

dbt-seed:
	docker compose run --rm dbt seed
	@echo "Seed data loaded into marts.datacenter_facilities"

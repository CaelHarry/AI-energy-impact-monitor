# AI Energy Impact Monitor

A real-time data pipeline that tracks how AI data center energy demand stresses the US power grid — and how that stress ripples into air quality, nuclear output, and regional climate signals.

The core insight: when temperatures spike in data center–dense regions like Northern Virginia or Texas, grid demand surges, gas peaker plants spin up, nuclear output stays flat, and air quality degrades — all within a 2-hour window. Nobody has cleanly assembled this correlation into an open, time-series pipeline. This project does.

## What it does

Pulls live data from six public sources, stores it in a time-series PostgreSQL database, runs dbt transformations to detect spikes and correlations, and (in the final phase) serves a live heatmap dashboard showing AI energy pressure on the US grid in real time.

## Data sources

| Source | Data | Update frequency | Key needed |
|---|---|---|---|
| [AirNow (EPA)](https://docs.airnowapi.org) | Regional AQI by location | Every 30 min | Yes (free) |
| [Open-Meteo](https://open-meteo.com) | Temperature, heat index, wind | Hourly | No |
| [EIA](https://www.eia.gov/opendata) | Grid generation mix (coal/gas/nuclear/solar %) | Hourly | Yes (free) |
| [ERCOT](https://www.ercot.com/gridinfo) / [CAISO](https://oasis.caiso.com) / [PJM](https://dataminer2.pjm.com) | Regional grid demand load | Every 5 min | No |
| [NRC](https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/) | Nuclear reactor capacity % | Daily | No |
| [USGS](https://earthquake.usgs.gov/earthquakes/feed/) | Seismic activity M1.0+ | Every 10 min | No |

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Storage | PostgreSQL 16 + TimescaleDB |
| Transformation | dbt (dbt-postgres) |
| Containerisation | Docker + Docker Compose |
| DB browser | pgAdmin 4 |
| Language | Python 3.11 |

## Architecture

```
Public APIs
    │
    ▼
Airflow DAGs          ← one DAG per source, scheduled independently
    │
    ▼
PostgreSQL            ← raw hypertables, time-partitioned by TimescaleDB
(TimescaleDB)
    │
    ▼
dbt models            ← staging → intermediate → mart layers
    │                    spike detection, cross-source correlation
    ▼
Dashboard             ← live heatmap, spike alerts, trend charts
(Phase 5)
```

## Key questions this answers

- When temperatures spike in a data-center-dense region, how quickly does grid demand surge and AQI degrade?
- Does higher nuclear output measurably reduce air quality impact during demand peaks?
- Which grid regions (ERCOT / CAISO / PJM) show the strongest correlation between AI infrastructure growth and grid stress?
- How does the clean energy mix change during peak vs off-peak hours across different regions?

## Schema

The full database schema is defined in [`schema.dbml`](schema.dbml) using [DBML](https://dbml.dbdiagram.io/docs/) format. To render an interactive ERD:

1. Open [dbdiagram.io/d](https://dbdiagram.io/d)
2. Paste the contents of `schema.dbml`

Or run `make diagram` to dump the live schema from your running database and open dbdiagram.io in one command.

Six raw hypertables (time-partitioned by TimescaleDB) all reference a central `grid_regions` table:

| Table | Source | Update frequency |
|---|---|---|
| `aqi_raw` | AirNow (EPA) | Every 30 min |
| `weather_raw` | Open-Meteo | Hourly |
| `grid_generation_raw` | EIA | Hourly |
| `grid_demand_raw` | ERCOT / CAISO / PJM | Every 5 min |
| `nuclear_status_raw` | NRC | Daily |
| `seismic_raw` | USGS | Every 10 min |

---

## Getting started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + Compose plugin)
- Python 3.11+ (for generating the Fernet key)

### 1. Clone and configure

```bash
git clone https://github.com/your-username/ai-energy-monitor.git
cd ai-energy-monitor
cp .env.example .env
```

Edit `.env` and fill in your values. Generate the Airflow Fernet key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Get your free API keys:
- AirNow: https://docs.airnowapi.org/account/request/
- EIA: https://www.eia.gov/opendata/register.php

All other sources require no authentication.

### 2. Create local directories

```bash
mkdir -p airflow/dags airflow/plugins airflow/logs
```

### 3. Start the stack

```bash
# Initialise Airflow DB and create admin user (run once)
docker compose up airflow-init

# Once init exits with code 0, start all services
docker compose up -d postgres airflow-webserver airflow-scheduler pgadmin
```

### 4. Verify

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / (from .env) |
| pgAdmin | http://localhost:5050 | admin@pipeline.dev / (from .env) |
| PostgreSQL | localhost:5432 | pipeline / (from .env) |

Confirm TimescaleDB is active:

```bash
docker exec -it pipeline-postgres psql -U pipeline -d pipeline -c "\dx"
```

## Project structure

```
.
├── docker-compose.yml
├── .env.example
├── .gitignore
├── .gitattributes
├── Makefile
├── schema.dbml
├── LICENSE
├── README.md
│
├── postgres/
│   ├── init/
│   │   └── 01_init_databases.sh
│   └── schema/
│       └── 01_raw_schema.sql
│
├── airflow/
│   ├── requirements.txt
│   ├── dags/
│   └── plugins/
│
└── dbt/
    ├── profiles.yml
    ├── models/
    │   ├── staging/
    │   ├── intermediate/
    │   └── marts/
    └── tests/
```

## Build phases

- [x] Phase 1 — Docker Compose infrastructure
- [x] Phase 2 — TimescaleDB schema (raw hypertables)
- [ ] Phase 3 — Airflow DAGs (one per source)
- [ ] Phase 4 — dbt transformation models
- [ ] Phase 5 — Live heatmap dashboard

## Useful commands

```bash
# View all container logs
docker compose logs -f

# Restart a single service
docker compose restart airflow-scheduler

# Run dbt manually
docker compose run --rm dbt dbt run

# Run dbt tests
docker compose run --rm dbt dbt test

# Stop all services (keeps data volumes)
docker compose down

# Full reset including all data (destructive)
docker compose down -v
```

## Contributing

Contributions are welcome. Please open an issue before submitting a pull request for anything beyond small fixes, so we can discuss the approach first.

## License

MIT — see [LICENSE](LICENSE) for details.

---

## Setting up on a new machine

After cloning the repo on a new device, a few things need to be recreated manually because they are excluded from version control by `.gitignore`.

### What is not in the repo

| Missing | Why | How to recreate |
|---|---|---|
| `.env` | Contains secrets — never committed | `cp .env.example .env` then fill in values |
| `postgres-data/` | Docker volume — machine-specific | Created automatically on first `docker compose up` |
| `pgadmin-data/` | Docker volume — machine-specific | Created automatically on first `docker compose up` |
| `airflow/logs/` | Log output — not useful in git | `mkdir -p airflow/logs` |
| Docker images | Too large for git | Pulled automatically by Docker on first run |

### Full checklist

```bash
# 1. Install Docker Desktop if not already on this machine
#    https://www.docker.com/products/docker-desktop/

# 2. Clone the repo
git clone https://github.com/your-username/ai-energy-monitor.git
cd ai-energy-monitor

# 3. Create the missing log directory
mkdir -p airflow/logs

# 4. Create your .env file
cp .env.example .env
# Open .env and fill in all passwords and API keys

# 5. Generate a Fernet key (no local Python needed)
docker run --rm python:3.11 python -c \
  "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste the output into .env as AIRFLOW_FERNET_KEY

# 6. Initialise Airflow (run once)
docker compose up airflow-init

# 7. Start all services
docker compose up -d postgres airflow-webserver airflow-scheduler pgadmin

# 8. Apply the database schema
make schema
```

### Fernet key — same or new?

If you want both machines to share the same Airflow instance (same encrypted connections and variables), use the **same** `AIRFLOW_FERNET_KEY` in both `.env` files. If this is a completely independent environment, generate a fresh key — it makes no difference at this stage since no encrypted variables have been saved yet.

### API keys

Your `AIRNOW_API_KEY` and `EIA_API_KEY` are the same keys you registered for on your first machine — just copy them into the new `.env`. No new registration needed.

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
| pgAdmin | http://localhost:5050 | admin@pipeline.local / (from .env) |
| PostgreSQL | localhost:5432 | pipeline / (from .env) |

Confirm TimescaleDB is active:

```bash
docker exec -it pipeline-postgres psql -U pipeline -d pipeline -c "\dx"
```

## Project structure

```
.
├── docker-compose.yml          # all services
├── .env.example                # copy to .env
├── LICENSE
├── README.md
│
├── postgres/
│   └── init/
│       └── 01_init_databases.sql   # creates airflow + pipeline DBs
│
├── airflow/
│   ├── requirements.txt            # DAG dependencies
│   ├── dags/                       # one DAG per data source
│   └── plugins/                    # shared utilities
│
└── dbt/
    ├── profiles.yml                # DB connection config
    ├── models/
    │   ├── staging/                # stg_* — clean raw tables
    │   ├── intermediate/           # int_* — cross-source joins
    │   └── marts/                  # mart_* — spike events, trends
    └── tests/
```

## Build phases

- [x] Phase 1 — Docker Compose infrastructure
- [ ] Phase 2 — TimescaleDB schema (raw hypertables)
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

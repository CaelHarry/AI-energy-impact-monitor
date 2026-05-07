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
| [EIA Form 930](https://www.eia.gov/opendata) | Regional grid demand (ERCOT / CAISO / PJM) | Hourly | Yes (free, same key as above) |
| [NRC](https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/) | Nuclear reactor capacity % | Daily | No |
| [USGS](https://earthquake.usgs.gov/earthquakes/feed/) | Seismic activity M1.0+ | Every 10 min | No |

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Storage | PostgreSQL 16 + TimescaleDB |
| Transformation | dbt (dbt-postgres) |
| Monitoring | Grafana (provisioned dashboards) |
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
Grafana               ← pipeline health monitoring (auto-provisioned)
                         live heatmap dashboard (Phase 5)
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
| `grid_demand_raw` | EIA Form 930 (ERCOT / CAISO / PJM) | Hourly |
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
make up
```

On first run this initialises the Airflow database and creates the admin user automatically before starting all services.

### 4. Verify

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / (from .env) |
| Grafana | http://localhost:3000 | admin / (from .env) |
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
│   ├── dags/              ← one DAG per source
│   └── plugins/
│       └── pipeline/      ← shared helpers (models.py, db.py)
│
├── dbt/
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
│
└── grafana/
    └── provisioning/
        ├── datasources/   ← pipeline DB connection (auto-provisioned)
        └── dashboards/    ← pipeline health dashboard JSON
```

## Pipeline health monitoring

Grafana is included in the stack and auto-provisions a **Pipeline Health** dashboard at [http://localhost:3000](http://localhost:3000) (login: `admin` / value of `GRAFANA_PASSWORD` in `.env`).

The dashboard has three sections:

| Section | Panels | What it shows |
|---|---|---|
| Data Freshness | 6 stat panels (one per table) | Minutes/hours since the last row landed — green → yellow → red thresholds per source cadence |
| Ingestion Volume | Bar chart + EIA lag stat | Rows inserted in the last 24 h per table; EIA data publish lag in hours (~14 h expected) |
| Row count over time | Time-series (1 h buckets) | Per-table ingestion rate across the selected time range |

<!-- Add dashboard screenshot here -->

The dashboard and datasource are fully provisioned from files in `grafana/provisioning/` — no manual setup required after `make up`.

## Build phases

- [x] Phase 1 — Docker Compose infrastructure
- [x] Phase 2 — TimescaleDB schema (raw hypertables)
- [x] Phase 3 — Airflow DAGs (one per source)
- [x] Phase 4 — dbt transformation layer (16 models, 65 tests)
- [x] Phase 4.5 — Grafana pipeline health monitoring dashboard
- [ ] Phase 5 — Live AI energy impact heatmap dashboard

## Useful commands

```bash
make up          # Start all services
make down        # Stop all services (data volumes preserved)
make reset       # Full teardown including data — destructive
make logs        # Tail all container logs
make dbt-run     # Run all dbt models
make dbt-test    # Run dbt data quality tests
```

Apply or re-apply the database schema:

```bash
make schema      # Linux / Mac
```

```powershell
# Windows — make schema uses shell redirection which fails on PowerShell
docker cp postgres/schema/01_raw_schema.sql pipeline-postgres:/tmp/schema.sql
docker exec pipeline-postgres psql -U pipeline -d pipeline -f /tmp/schema.sql
```

Other common operations:

```bash
# Restart a single service after changing plugin code
docker compose restart airflow-scheduler

# Direct DB access
docker exec -it pipeline-postgres psql -U pipeline -d pipeline
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
# Linux/Mac:
make schema
# Windows (PowerShell):
docker cp postgres/schema/01_raw_schema.sql pipeline-postgres:/tmp/schema.sql
docker exec pipeline-postgres psql -U pipeline -d pipeline -f /tmp/schema.sql
```

### Fernet key — same or new?

If you want both machines to share the same Airflow instance (same encrypted connections and variables), use the **same** `AIRFLOW_FERNET_KEY` in both `.env` files. If this is a completely independent environment, generate a fresh key — it makes no difference at this stage since no encrypted variables have been saved yet.

### API keys

Your `AIRNOW_API_KEY` and `EIA_API_KEY` are the same keys you registered for on your first machine — just copy them into the new `.env`. No new registration needed.

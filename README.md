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

## Project thesis and analytical scope

The central claim this project tests: **AI data centers act as a persistent, weather-insensitive baseload on the grid**. Unlike residential demand (which spikes in summer heat) or industrial demand (which tracks business hours), data centers run continuously at high utilisation. When a region hosts a large concentration of data center capacity, its grid floor — the minimum demand even at 3 am on a mild night — rises, squeezing out the slack that operators use to absorb renewable variability. During temperature spikes, that already-stressed grid has less room to absorb the residential/commercial surge, forcing gas peaker plants online and degrading air quality within hours.

Nuclear is the one clean source that provides a meaningful structural buffer: it is dispatchable on a slow timescale, runs at near-100 % capacity factor, and displaces the gas peakers that would otherwise set the marginal emission rate. This pipeline tracks that offset directly — comparing AQI degradation during demand spikes in high-nuclear vs low-nuclear grid hours.

### Region selection rationale

| Region | Data center relevance | Nuclear presence | Grid coverage |
|---|---|---|---|
| **PJM** | Northern Virginia (Ashburn/Loudoun County) is the world's largest data center cluster — ~35 % of global colocation capacity. Strongest signal for the AI demand thesis. | Large fleet (~35 GW), ~20 % of generation mix. Clear AQI offset signal expected. | Full — PJM is a single balancing authority with clean accounting. |
| **ERCOT** | Texas (Austin, Dallas, San Antonio corridor) is one of the fastest-growing US data center markets. | ~5 GW (Comanche Peak + South Texas Project). Smaller buffer than PJM. | Full — ERCOT covers essentially all of Texas. |
| **CAISO** | Silicon Valley (Santa Clara, San Jose) is the main California data center cluster. CAISO covers this via PG&E. | Diablo Canyon Units 1 & 2 (~2.3 GW) — the only operating California reactors. | Partial — see limitations below. |

### Known data coverage limitations

**CAISO excludes the Los Angeles grid (LADWP)**

The Los Angeles Department of Water and Power operates its own balancing authority (EIA code: `LDWP`) and is not part of CAISO. LADWP serves roughly 4 million customers — about 20 % of California's total load — and is **not included** in the `grid_generation_raw` or `grid_demand_hourly` tables.

For the data center thesis this matters less than it might seem: the Silicon Valley hyperscale cluster (Google, Meta, Apple campus infrastructure) sits in PG&E territory, which *is* part of CAISO. LA has data centers but is not the hyperscale hub.

**AQI bounding box covers more territory than the demand data**

The AirNow DAG queries a bounding box that covers all of California, including the LA Basin. The demand and generation data covers CAISO only. This creates a mismatch:

- An LA Basin heat wave will degrade PM2.5 AQI within the CAISO bounding box without producing a matching CAISO demand spike (because LADWP absorbs that load separately).
- Any correlation model trained on CAISO demand vs CAISO-region AQI will see unexplained AQI exceedances during LA heat events. This weakens California correlations and could produce false negatives.

If extending this project to fully cover California, the fix is to add `LDWP` to the `REGION_MAP` in `dag_eia_grid.py` and either track it as a separate region or merge it into a combined California aggregate.

**EIA generation data has a 14–24 hour publish lag**

EIA validates and publishes generation mix data with roughly a one-day delay. Grid demand data (EIA Form 930) has a similar lag. AirNow AQI is near-real-time (< 1 hour delay). The practical effect is that cross-source correlations in the dbt mart layer are always looking at yesterday's generation vs today's AQI — acceptable for statistical analysis over weeks of data, but not suitable for same-hour event detection without using the raw source tables directly.

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

## AI Energy Impact dashboard

The main project dashboard auto-provisions at [http://localhost:3000](http://localhost:3000) under the title **AI Energy Impact Monitor**. It refreshes every 5 minutes and defaults to a 24-hour window.

The dashboard has four sections:

| Section | Panel type | Data source | What it shows |
|---|---|---|---|
| Current Grid Mix | 3 stat cards (one per region) | `grid_generation_raw` | 24-hour average clean energy % for ERCOT, CAISO, and PJM. Color-coded: red below 25 %, yellow 25–45 %, green 45 %+. |
| Energy Mix Breakdown | Grouped bar chart | `grid_generation_raw` | 7-day average of Clean %, Fossil %, and Nuclear % side-by-side for each region. Clean = nuclear + wind + solar + hydro. Fossil = gas + coal. |
| Live Grid Demand | Time series | `grid_demand_hourly` | Hourly demand in MW per region across the selected time window. PJM typically peaks around 80 GW, ERCOT around 60 GW, CAISO around 25 GW. |
| Air Quality — PM2.5 AQI | Time series | `aqi_hourly` | Hourly PM2.5 AQI per region. Threshold lines at 51 (Moderate), 101 (Unhealthy for Sensitive Groups), and 151 (Unhealthy). |

### Reading the clean energy percentages

The stat cards reflect a real split in how decarbonised each grid is:

- **CAISO (California)** consistently reads near or above 90 % in spring and autumn. By 2025–26, utility-scale batteries shift daytime solar surplus into the overnight hours, so even at midnight gas usage is only ~2,000 MW on a 25,000 MW grid. The green card is expected.
- **PJM (Mid-Atlantic / Midwest)** sits in the 40–50 % range. Its large nuclear fleet (~35 GW) accounts for most of the clean share; coal and gas fill the remainder.
- **ERCOT (Texas)** typically reads 30–45 %. Texas has the largest wind fleet in the US and growing solar capacity, but natural gas remains the dominant balancing fuel.

### Data freshness notes

- **Grid generation** (EIA) has a 14–24 h publish lag — the 24-hour and 7-day averages pull the most recent data EIA has released, which may be a day behind real time.
- **Grid demand** (EIA Form 930) has a similar lag; demand time series data may stop a day short of "now."
- **AQI** (AirNow) updates hourly with less than a 1-hour lag — the AQI panel reflects near-real-time air quality.

All panels query TimescaleDB source tables and continuous aggregates directly, so they remain live regardless of whether the dbt transformation layer has been re-run.

## Build phases

- [x] Phase 1 — Docker Compose infrastructure
- [x] Phase 2 — TimescaleDB schema (raw hypertables)
- [x] Phase 3 — Airflow DAGs (one per source)
- [x] Phase 4 — dbt transformation layer (16 models, 65 tests)
- [x] Phase 4.5 — Grafana pipeline health monitoring dashboard
- [x] Phase 5 — AI Energy Impact Monitor dashboard (grid mix, demand, AQI)

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

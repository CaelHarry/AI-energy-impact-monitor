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

**AQI bounding box: scoped to Northern California only**

The LA Basin creates a geographic mismatch: monitoring stations there are served by a mix of LADWP (not in CAISO) and SCE (in CAISO), with no clean boundary that follows the utility service territories. Including the full state would blend LA Basin AQI readings — which have no matching demand signal — into the CAISO average, producing spurious correlation failures during LA heat events.

To avoid this, the AirNow bounding box for CAISO is intentionally limited to Northern California (`lat >= 36.5`), covering the Bay Area and Silicon Valley — the region that is both the primary California data center cluster and cleanly inside CAISO's grid boundary. This means:

- AQI readings reflect air quality in and around the Silicon Valley data center concentration.
- Demand spikes in CAISO data and AQI spikes in the bounding box come from the same geographic and grid territory.
- San Diego (SDG&E, also part of CAISO) is outside this box and excluded. It could be added as a separate named region if SoCal coverage is needed.

If extending coverage to include full California, the recommended approach is to add `LDWP` to `REGION_MAP` in `dag_eia_grid.py` as a separate region and add a matching LA Basin bounding box in `dag_airnow.py`, rather than merging it into CAISO.

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

A separate static reference table, `marts.datacenter_facilities`, maps known hyperscale and colocation data center campuses to their grid region. This is loaded once via `make dbt-seed` rather than a live DAG, since facility locations don't change on a pipeline cadence.

| Column | Type | Description |
|---|---|---|
| `name` | text | Facility name |
| `operator` | text | Operating company (Google, Amazon, Equinix, etc.) |
| `facility_type` | text | `hyperscale`, `colocation`, or `enterprise` |
| `lat` / `lon` | float8 | Approximate facility coordinates |
| `region_id` | text | Grid region — ERCOT, CAISO, or PJM |
| `capacity_mw_est` | float8 | Estimated MW capacity — not always publicly disclosed; treat as order-of-magnitude |
| `year_opened` | integer | Approximate year the facility came online |
| `source` | text | Public source the entry is based on |

The seed covers 16 facilities across the three regions: 5 in ERCOT (Texas corridor), 4 in CAISO (Bay Area / Silicon Valley), and 7 in PJM (Northern Virginia cluster). To add more facilities, edit [`dbt/seeds/datacenter_facilities.csv`](dbt/seeds/datacenter_facilities.csv) and re-run `make dbt-seed`.

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
| Air Quality — PM2.5 Concentration | Time series | `aqi_raw` | Hourly average PM2.5 concentration (µg/m³) per region. Threshold lines at 12 µg/m³ (Moderate), 35.4 µg/m³ (Unhealthy for Sensitive Groups), and 55.4 µg/m³ (Unhealthy). |

### Reading the clean energy percentages

The stat cards reflect a real split in how decarbonised each grid is:

- **CAISO (California)** consistently reads near or above 90 % in spring and autumn. By 2025–26, utility-scale batteries shift daytime solar surplus into the overnight hours, so even at midnight gas usage is only ~2,000 MW on a 25,000 MW grid. The green card is expected.
- **PJM (Mid-Atlantic / Midwest)** sits in the 40–50 % range. Its large nuclear fleet (~35 GW) accounts for most of the clean share; coal and gas fill the remainder.
- **ERCOT (Texas)** typically reads 30–45 %. Texas has the largest wind fleet in the US and growing solar capacity, but natural gas remains the dominant balancing fuel.

### Data freshness notes

- **Grid generation** (EIA) has a 14–24 h publish lag — the 24-hour and 7-day averages pull the most recent data EIA has released, which may be a day behind real time.
- **Grid demand** (EIA Form 930) has a similar lag; demand time series data may stop a day short of "now."
- **PM2.5 concentration** (AirNow) updates hourly with less than a 1-hour lag. The panel queries `aqi_raw` directly using raw concentration (µg/m³) rather than the AQI index. AirNow only computes a valid AQI index for ~18 % of station readings (requiring sufficient recent samples); raw concentration is available for ~80 % of readings and is more suitable for continuous correlation analysis.

All panels query TimescaleDB source tables and continuous aggregates directly, so they remain live regardless of whether the dbt transformation layer has been re-run.

## Nuclear Fleet Status dashboard

A second dashboard auto-provisions at [http://localhost:3000](http://localhost:3000) under the title **Nuclear Fleet Status**. It refreshes hourly and defaults to a 30-day window.

Nuclear output is the structural variable in the clean-energy story — it is baseload, runs at near-100 % capacity factor, and is the primary factor displacing gas peakers during demand spikes. This dashboard tracks the fleet continuously so any unplanned outages that would reduce the clean buffer are immediately visible.

The dashboard has three sections:

| Section | Panel type | Data source | What it shows |
|---|---|---|---|
| Regional Average Capacity | 3 stat cards (one per region) | `nuclear_status_raw` | 48-hour average capacity factor per region. Color-coded: red below 70 %, yellow 70–90 %, green 90 %+. A red card means unplanned outages are reducing the clean buffer. |
| Reactor Status Table | Table | `nuclear_status_raw` | Latest capacity % per reactor unit, sorted by region then capacity descending. Shows operator, report date, and color-coded capacity column. |
| 30-Day Capacity Trend | Time series | `nuclear_status_raw` | Daily average nuclear capacity % for ERCOT, CAISO, and PJM over the selected window. Used to spot sustained derating events vs. brief maintenance outages. |

### Why nuclear capacity matters for this project

When a reactor partially derates or goes offline, gas peakers spin up to compensate — directly increasing the fossil % of the grid and raising the marginal emission rate. During a concurrent temperature spike (which drives AI data center cooling load), the combination produces the worst-case AQI outcome this pipeline is designed to detect. The Nuclear Fleet dashboard makes that risk visible in near-real time (NRC data lags by ~1 day).

Notable reactors tracked:

- **ERCOT**: South Texas Project (2 × ~1.35 GW), Comanche Peak (2 × ~1.2 GW)
- **CAISO**: Diablo Canyon (2 × ~1.15 GW) — the only operating reactor in California; license extended to ~2030
- **PJM**: ~35 GW fleet across Pennsylvania, New Jersey, Illinois, and Virginia — the largest nuclear fleet of any US grid region

## Build phases

- [x] Phase 1 — Docker Compose infrastructure
- [x] Phase 2 — TimescaleDB schema (raw hypertables)
- [x] Phase 3 — Airflow DAGs (one per source)
- [x] Phase 4 — dbt transformation layer (16 models, 65 tests)
- [x] Phase 4.5 — Grafana pipeline health monitoring dashboard
- [x] Phase 5 — AI Energy Impact Monitor dashboard (grid mix, demand, PM2.5 concentration)
- [x] Phase 5.5 — Nuclear Fleet Status dashboard (capacity factors, reactor table, 30-day trend)

## Useful commands

```bash
make up          # Start all services
make down        # Stop all services (data volumes preserved)
make reset       # Full teardown including data — destructive
make logs        # Tail all container logs
make dbt-run     # Run all dbt models
make dbt-test    # Run dbt data quality tests
make dbt-seed    # Load static reference data (datacenter_facilities)
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

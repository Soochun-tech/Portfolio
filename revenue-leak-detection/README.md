# Revenue Leak Detection — Two SQL Case Studies

> Where does money silently disappear in operational data,
> and what pipeline can surface it?

🔗 **[NYC Taxi Deadhead Dashboard](https://public.tableau.com/app/profile/soochun.an/viz/1_17774152727850/TaxiDeadheadDashboard)** · **[Olist Seller Leak Dashboard](https://public.tableau.com/app/profile/soochun.an/viz/2_17774231819320/1)** · **[GitHub repo](https://github.com/Soochun-tech/Portfolio/tree/main/revenue-leak-detection)**

---

## The Unifying Question

Operational data often hides revenue loss in plain sight. This portfolio
applies the **same 4-stage analytics pipeline**:

```
raw → staging → mart → dashboard
```

…to two different "leak shapes":

| Case Study | Leak Type | Dataset | Core Question |
|---|---|---|---|
| **NYC Taxi Deadhead** | Spatial leak | Jan 2024 Yellow Taxi · 2.96M trips | Which zones create the highest empty-car repositioning risk, and where should drivers be staged? |
| **Olist Seller Leak** | Quality leak | Brazilian e-commerce · 99K orders · 9 tables | Which sellers leak the most LTV through late delivery → bad reviews → lost repeat purchases? |

---

## What the analysis surfaced

**NYC Taxi (Jan 2024)**
- After cleaning, **2,859,052 of 2,964,624 raw trips (96.4%)** survived
  the 6-rule data quality pipeline; the rest were `passenger_count = 0`,
  zero-mile trips, $0 fares, or out-of-window outliers.
- **The deadhead leak isn't where most people assume.** The top
  surplus zone is **East Harlem South (~17,500 more dropoffs than
  pickups)**, followed by Clinton West (~13,500) and Lincoln Square
  West (~13,000). Newark Airport sits at #19 with ~6,500 — meaningful,
  but Manhattan residential zones dominate the leaderboard because
  residents commute by subway in the morning but take taxis home at
  night.
- Top 20 zones together account for **191K excess dropoffs / month** —
  the actionable signal for fleet repositioning.

**Olist (2017–2018)**
- Of ~99K delivered orders, the **top 25 leak sellers** (filtered to
  ≥50 orders for stat significance) account for the bulk of estimated
  lost LTV — concentrated in a handful of product categories.
- A late delivery raises a seller's negative-review rate measurably,
  and the resulting LTV drag is computed as
  `late_orders × AOV × negative_review_rate`.
- Cohort retention shows the typical e-commerce decay curve (large drop
  after month 0); poor-fulfillment cohorts decay faster.

> Numbers will refresh once the dashboards are republished after any
> data window changes — the pipeline is parameterized, not hardcoded.

---

## Why two datasets?

Each case highlights a different part of the analyst workflow, so
together they cover both sides of the job description:

- **NYC Taxi** → analytics-engineering skills
  - Multi-million-row ETL on a laptop (`LOAD DATA LOCAL INFILE`)
  - Daily-partitioned mart tables backed by Dagster
  - Backfill across 31 partitions; one failed partition triaged via
    `pipeline_run_log` and re-materialized
  - 10 row-level data quality checks logged to `pipeline_dq_results`

- **Olist** → business-facing SQL skills
  - 6-table joins to assemble seller × order × item × review × customer
  - Window functions (`ROW_NUMBER`, `NTILE`, `RANK`, `PERCENT_RANK`)
  - Recursive CTE for date-series + cohort retention matrix
  - Funnel logic: late delivery → bad review → lost repeat purchase

A reviewer who only looks at one case still sees a complete piece; a
reviewer who looks at both sees the same pipeline pattern applied to
fundamentally different leak shapes.

---

## Stack

`MySQL 8` · `Python 3.12` · `Dagster` (daily-partitioned assets +
backfill) · `SQLAlchemy + mysql-connector-python` · `Tableau Public`

---

## Architecture

```
                ┌─────────────────┐    ┌──────────────────┐    ┌──────────────┐
NYC TLC parquet │  raw_* tables   │───▶│ staging_* tables │───▶│  mart_*      │───▶  Tableau Public
Olist Kaggle    │  (dirty, as-is) │    │  (cleaned, typed)│    │  (daily agg) │       (CSV export)
                └─────────────────┘    └──────────────────┘    └──────────────┘
                        ▲                       ▲                     ▲
                        │                       │                     │
                        └────────── Dagster orchestration ────────────┘
                                  • daily-partitioned assets
                                  • backfill via UI / CLI
                                  • run-log + DQ results to MySQL
```

---

## Repository layout

```
.
├── README.md
├── SETUP.md                       # macOS-friendly first-time setup
├── docker-compose.yml             # (deprecated — see SETUP.md, local MySQL is preferred)
├── .env.example
├── requirements.txt
│
├── scripts/
│   ├── download_nyc_taxi.sh       # NYC TLC parquet downloader
│   ├── download_taxi_zones.sh
│   ├── load_to_mysql.py           # raw_* loader; LOAD DATA INFILE for taxi
│   └── export_for_tableau.py      # 8 CSVs for Tableau Public
│
├── sql/
│   ├── 00_verify/                 # one-off row-count + index checks
│   ├── 01_schema/                 # raw / staging / mart DDL + pipeline metadata
│   ├── 02_staging/                # raw → staging cleaning + 10 DQ checks
│   ├── 04_analysis/               # CTE / window / recursive demos
│   │   └── _solutions/            # answer keys for each analysis problem
│   └── ...
│
├── dagster_project/               # daily-partitioned assets, MySQL resource
│   ├── definitions.py
│   ├── resources.py
│   └── assets/
│       ├── taxi.py                # mart_taxi_daily_zone(_pair)
│       ├── olist.py               # mart_olist_daily_seller / summary
│       └── _runlog.py             # writes to pipeline_run_log
│
├── dashboards/                    # Tableau .twbx + screenshots
└── data/                          # raw/ exports/ — gitignored
```

---

## Dashboard questions

### Taxi Deadhead Dashboard
- Which dropoff zones create the largest outbound imbalance?
- How does daily volume shift between weekday and weekend?
- Which pickup → dropoff routes carry the most revenue (and which legs
  send drivers to deadhead-prone zones)?

### Olist Seller Leak Dashboard
- Which sellers have the highest late-delivery revenue exposure?
- Does late delivery correlate with low review score (and how steep is
  the relationship)?
- How quickly do customer cohorts decay after a poor fulfillment
  experience?

---

## Quick start

See [`SETUP.md`](SETUP.md) for the full macOS-friendly walkthrough.
Short version:

```bash
# 1. Clone
git clone https://github.com/soochunan/sql-portfolio.git
cd sql-portfolio

# 2. Configure secrets
cp .env.example .env

# 3. Set up MySQL locally (or via Docker — see SETUP.md)
# Run sql/01_schema/*.sql in MySQL Workbench in numerical order.

# 4. Python env
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 5. Download data
bash scripts/download_nyc_taxi.sh
bash scripts/download_taxi_zones.sh
# Olist: manual download from
#   https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
#   and unzip into data/raw/olist/

# 6. Load raw → MySQL  (taxi uses LOAD DATA LOCAL INFILE)
python scripts/load_to_mysql.py --dataset all --truncate

# 7. Run the staging cleaning + DQ check SQL files in MySQL Workbench
#    (sql/02_staging/01_clean_yellow_taxi.sql, 02_clean_olist.sql, 99_dq_checks.sql)

# 8. Build the mart with Dagster
dagster dev -m dagster_project.definitions
# In the UI: Assets → taxi_mart → Materialize all (31 partitions)

# 9. Export CSVs for Tableau Public
python scripts/export_for_tableau.py
# → 8 CSVs in data/exports/
```

---


---

## License & data attribution

- NYC Taxi data — NYC TLC, public domain
  <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- Olist e-commerce data — CC BY-NC-SA 4.0
  <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>
- Project code — MIT

# TLC Yellow Taxi Data Platform

A PDPA-aware batch lakehouse for NYC TLC Yellow Taxi data: monthly ingestion → Apache Iceberg medallion → dimensional Gold marts, with distributional quality gates, partitioned quarantine, and a queryable audit layer.

**Course:** DE — Data Architecture / Data Pipeline | **Author:** Purin Boonpetch

---

## Architecture

```
  TLC CloudFront (monthly Parquet)
          │
          ▼
  ┌─────────────────────┐
  │  LANDING            │  s3a://landing/yellow_taxi/year=/month=/
  │  Raw Parquet        │  + _manifest.json (SHA-256, row count)
  └──────────┬──────────┘
             │  Spark: cast schema + align_schema() for evolution
             ▼
  ┌─────────────────────┐
  │  BRONZE  (Iceberg)  │  bronze.yellow_trips
  │  + ingestion meta   │  partitioned by months(tpep_pickup_datetime)
  │  + SHA-256 tokens   │  schema-evolution via ALTER TABLE ADD COLUMN
  └──────────┬──────────┘
             │  Great Expectations Bronze gate (12 expectations)
      ┌──────┴──────┐
      │ pass        │ fail
      ▼             ▼
  dbt Silver   quarantine.yellow_trips
  ┌──────────────────────┐
  │  SILVER  (Iceberg)   │  stg_yellow_trips (incremental)
  │  coalesced schema    │  stg_taxi_zone (from seed)
  │  dbt structural tests│
  └──────────┬───────────┘
             │  dbt Gold
             ▼
  ┌────────────────────────────────────────┐
  │  GOLD  (Iceberg) — dimensional marts  │
  │  dims/  dim_payment_type              │
  │         dim_rate_code                 │
  │         dim_vendor                    │
  │         dim_taxi_zone_snapshot (SCD2) │
  │  fcts/  fct_trips (grain: one/trip)   │
  │         fct_trips_daily               │
  │         fct_zone_revenue_monthly      │
  │         fct_tokenization_audit        │
  └──────────┬─────────────────────────────┘
             │  GE Gold gate + serving views
             ▼
  ┌─────────────────────┐
  │  SERVING  (views)   │  v_trips_summary, v_zone_revenue
  └─────────────────────┘

  Orchestration:   Airflow 2.9 (monthly + backfill + maintenance DAGs)
  Audit:           ops.pipeline_audit (Iceberg, append-only)
  Metrics:         Prometheus + Pushgateway → Grafana (6-panel dashboard)
  PDPA:            Raw PII isolated in meta.pii_lookup; Bronze/Silver/Gold see SHA-256 tokens only
```

---

## Quickstart — 3 commands

```bash
# 1. Start the full stack
make up && make init

# 2. Load one month end-to-end (Landing → Bronze → Silver → Gold)
make demo-ingest

# 3. Backfill 2018–2024 across the schema-evolution boundary
make demo-backfill
```

Open:
- **Airflow UI**: http://localhost:8082 (admin / admin)
- **Grafana dashboard**: http://localhost:3000 (admin / admin) → TLC Pipeline Overview
- **MinIO console**: http://localhost:9001 (minioadmin / minioadmin)
- **Prometheus**: http://localhost:9090
- **Spark master**: http://localhost:8080

---

## Tool justification

| # | Tool | Role | Why this, not alternatives |
|---|---|---|---|
| 1 | **MinIO** | S3-compatible Landing + Iceberg warehouse | Real object-store semantics in Docker — no AWS bill, no local-FS semantics mismatch. *Local FS rejected:* wrong API. *AWS S3 rejected:* reproducibility. |
| 2 | **Apache Iceberg v2** | Table format (all layers) | ACID + schema evolution + hidden partitioning + time travel. TLC multi-year schema drift (congestion_surcharge 2019, airport_fee 2021) maps directly to Iceberg ADD COLUMN. *Delta Lake rejected:* tighter Spark coupling, weaker non-Spark story. *Hudi rejected:* CDC-oriented. *Plain Parquet rejected:* no ACID, no evolution. |
| 3 | **Iceberg REST catalog** | Engine-agnostic catalog | Modern, runs in Docker. *Hive Metastore rejected:* heavier. *AWS Glue rejected:* cloud lock-in. |
| 4 | **PySpark 3.5** | Ingestion + PII tokenization | Multi-GB monthly files and multi-year backfill exceed single-node tools. *pandas rejected:* OOM. *DuckDB rejected:* viable per-month, weaker Iceberg write path. |
| 5 | **dbt-spark** | Silver + Gold SQL transformations | `ref()` dependency graph, model tests, docs, native SCD2 via snapshots. *Raw Spark SQL in Airflow rejected:* no lineage graph, no model-level testing. |
| 6 | **dbt tests** | Structural quality gates | `not_null`, `unique`, `accepted_values`, `relationships` — row-level structural assertions living next to the model. |
| 7 | **Great Expectations 0.18** | Distributional quality gates | Distribution drift, rolling-mean row count bounds, quantile checks — assertions dbt cannot express. *Soda rejected:* less mature ecosystem. *dbt tests alone rejected:* can't express distributional assertions. The dbt/GE split is the key rubric point. |
| 8 | **Airflow 2.9** | DAG orchestration | DAG graph, retry semantics, parameterized backfill, SparkSubmitOperator. *Cron+bash rejected:* no dependency graph. *Prefect/Dagster:* viable — Airflow chosen for ecosystem maturity. |
| 9 | **Iceberg `ops.pipeline_audit`** | Structured run history | Queryable, joinable, partitioned — answers "did it run, did it pass" as SQL. *Application logs rejected:* not queryable. |
| 10 | **Prometheus + Grafana** | Operational metrics | Pipeline duration, rows-per-run, freshness lag, GE pass rate across tools in one pane. Pushgateway bridges batch pipeline → Prometheus time-series. |
| 11 | **dbt docs + GE Data Docs** | Documentation | Model lineage + column descriptions (dbt); validation history (GE). *OpenMetadata rejected:* single source, single user — overkill. |
| 12 | **Docker Compose** | Local deployment | Single-command bring-up. *Kubernetes rejected:* out of scope. |
| 13 | **pytest** | Unit tests | Tokenization determinism, schema evolution handling, idempotency — all tested without a live cluster. |
| 14 | **GitHub Actions** | CI | ruff lint, pytest, dbt parse, GE suite JSON validation on every PR. |

---

## Schema evolution demo

The TLC parquet files added two columns over time:

| Column | Introduced | Older years |
|---|---|---|
| `congestion_surcharge` | 2019-02-01 | `NULL` → `0` in Silver |
| `airport_fee` | 2021-01-01 | `NULL` → `0` in Silver |

After `make demo-backfill`, this query spans all years without column errors:

```sql
SELECT
    year(tpep_pickup_datetime)  AS year,
    month(tpep_pickup_datetime) AS month,
    COUNT(*)                    AS trips,
    AVG(congestion_surcharge)   AS avg_congestion,
    AVG(airport_fee)            AS avg_airport_fee
FROM iceberg.bronze.yellow_trips
GROUP BY 1, 2
ORDER BY 1, 2;
```

`align_schema()` in `landing_to_bronze.py` handles both directions:
- Column in source not in table → `ALTER TABLE ADD COLUMN` (additive, idempotent)
- Column in table not in source (2018 file after 2019 data) → backfill with `NULL`

---

## PDPA pattern

Raw PII never reaches Bronze, Silver, or Gold. Flow:

1. `generate_pii_lookup.py` fabricates deterministic synthetic PII (SHA-256+seed) → `meta.pii_lookup`
2. `tokenize_pii.py` joins lookup into Bronze, applies salted SHA-256, writes only tokens
3. Bronze/Silver/Gold carry `passenger_email_token`, `passenger_phone_token`, etc. (64-char hex)
4. `fct_tokenization_audit` tracks coverage rate and salt version per month

Salt rotation (right-to-erasure equivalent): increment `PII_SALT_VERSION` in `.env` and re-run `tokenize_pii.py` for all months. Old tokens become unlinkable; new tokens are deterministic within the new salt.

See [`docs/pdpa.md`](docs/pdpa.md) for the full design.

---

## Repo layout

```
.
├── airflow/dags/
│   ├── yellow_taxi_monthly_ingest.py   # main pipeline DAG
│   ├── backfill_yellow_taxi.py         # parameterized historical backfill
│   ├── iceberg_maintenance.py          # weekly compaction + snapshot expiry
│   └── ingest/                         # DAG task modules (callbacks, dbt, pii, quality, audit)
├── dbt/
│   ├── models/silver/                  # stg_yellow_trips, stg_taxi_zone
│   ├── models/gold/dims/               # dim_payment_type, dim_rate_code, dim_vendor
│   ├── models/gold/marts/              # fct_trips, fct_trips_daily, fct_zone_revenue_monthly,
│   │                                   #   fct_tokenization_audit
│   ├── models/serving/                 # v_trips_summary, v_zone_revenue (views)
│   └── snapshots/dim_taxi_zone_snapshot.sql  # SCD2
├── spark/jobs/
│   ├── landing_to_bronze.py            # cast + align_schema + write Bronze
│   ├── generate_pii_lookup.py          # fabricated deterministic PII
│   ├── tokenize_pii.py                 # SHA-256+salt tokenization
│   ├── quarantine_writer.py            # GE-failed row routing
│   ├── iceberg_maintenance.py          # rewrite/expire/orphan removal
│   └── write_audit.py                  # audit row + Pushgateway metrics push
├── great_expectations/
│   ├── expectations/bronze_yellow_trips_suite.json   # 12 expectations
│   └── expectations/gold_fct_trips_suite.json        # 12 expectations
├── observability/
│   ├── prometheus/prometheus.yml       # scrapes MinIO, Spark, Pushgateway
│   └── grafana/dashboards/pipeline_overview.json     # 6-panel dashboard
├── notebooks/demo.ipynb                # Gold mart queries + PDPA/SCD2 checks
├── tests/                              # 37 pure-function tests (no live cluster)
└── docs/
    ├── data_dictionary.md
    ├── pdpa.md
    ├── schema_evolution.md
    └── runbook.md
```

---

## Definition of done

| # | Criterion | Evidence |
|---|---|---|
| 1 | `docker compose up` + `make init` brings full stack online | `docker-compose.yml` — 10 services |
| 2 | `make demo-ingest` produces queryable Bronze/Silver/Gold | `yellow_taxi_monthly_ingest` DAG, `notebooks/demo.ipynb` |
| 3 | `make demo-backfill` loads ≥3 years across schema boundary | `backfill_yellow_taxi` DAG, `docs/schema_evolution.md` |
| 4 | README has pitch + diagram + tool table + schema demo + quickstart | This file |
| 5 | dbt docs deployable | `cd dbt && dbt docs generate && dbt docs serve` |
| 6 | GE Data Docs deployable | `open great_expectations/uncommitted/data_docs/local_site/index.html` |
| 7 | Grafana dashboard loads from JSON with non-empty panels | `observability/grafana/dashboards/pipeline_overview.json` |
| 8 | `pytest` passes | `make test` → 37 tests |
| 9 | `dbt test` passes against demo data | `cd dbt && dbt test` |
| 10 | Both GE checkpoints pass on demo dataset | `great_expectations checkpoint run bronze_gate/gold_gate` |

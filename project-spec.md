# NYC Yellow Taxi Data Platform — Project Specification

> A PDPA-aware batch lakehouse for NYC TLC Yellow Taxi data: monthly ingestion → Iceberg medallion → dimensional Gold marts, with distributional quality gates, partitioned quarantine, and a queryable audit layer.

**Author:** Purin Boonpetch
**Course:** DE — Data Architecture / Data Pipeline project
**Grading criterion:** appropriate tool selection for the project
**Status:** spec, pre-build

---

## 1. One-line pitch

A monthly-batch lakehouse on NYC Yellow Taxi data that demonstrates the core data-platform stack — object storage, table format, distributed compute, modular transformation, distributional quality gates, dimensional modeling, and operational observability — with every tool choice defensible against alternatives.

---

## 2. Goals and non-goals

### Goals

1. Build a 4-layer medallion lakehouse (Landing → Bronze → Silver → Gold) on Iceberg with idempotent monthly ingestion.
2. Demonstrate **appropriate tool selection** — every component in the stack has a written justification and a rejected alternative.
3. Demonstrate the **dbt-vs-Great-Expectations split** (structural vs distributional checks) instead of overlapping the two.
4. Demonstrate **dimensional modeling on Gold** — conformed dims, SCD2, documented grain.
5. Demonstrate **PDPA-style PII handling** via deterministic tokenization on a fabricated PII layer.
6. Demonstrate **schema evolution** — load multiple TLC years that span real schema changes (`congestion_surcharge` 2019, `airport_fee` 2021) and query across them without rewriting downstream models.
7. Provide a **queryable audit layer** answering "did the pipeline run, when, with what input, and did it pass quality."

### Non-goals (explicit — scope discipline counts toward the rubric)

- Streaming, CDC, sub-daily latency
- Real-time serving or low-latency APIs
- Multi-cloud, Kubernetes, or production deployment
- Dedicated data catalog tool (OpenMetadata/DataHub) — overkill for a single source; documentation via dbt docs + GE Data Docs + `docs/data_dictionary.md`
- Centralized log aggregation (Loki/ELK) — `docker logs` + audit table covers the realistic debug surface for this scope
- BI tool integration beyond one demonstration dashboard
- Cost modeling, lineage tooling beyond dbt's native lineage

---

## 3. Data source

**Primary:** NYC TLC Yellow Taxi Trip Records
**URL pattern:** `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
**Format:** Parquet, one file per month
**Cadence:** monthly, ~2-month publication lag
**Schema:** evolves over time (use this — it's free signal for the rubric)

**Secondary (small, static):** TLC taxi zone lookup CSV → `dim_taxi_zone` (with SCD2 demo)

**Fabricated layer:** synthetic PII columns generated and joined per `VendorID + pickup_datetime` to simulate a PDPA-relevant context — `passenger_email`, `passenger_phone`, `payment_card_last4`, `passenger_id`. Generated once into a deterministic lookup table so re-ingesting a month produces the same PII (preserves idempotency).

### Why this source against the rubric

- **Monthly cadence** satisfies the "data that updates" requirement
- **Real schema drift** justifies Iceberg over plain Parquet
- **Multi-GB per year** justifies Spark over pandas/DuckDB
- **Public, no auth** keeps the project reproducible

---

## 4. Architecture

```
                ┌──────────────────────────────────────────────────────────┐
                │  TLC CloudFront (monthly Parquet)  +  TLC zone lookup    │
                └────────────────────────┬─────────────────────────────────┘
                                         │
                                         ▼
            ┌──────────────────────────────────────────────┐
            │  LANDING                                     │
            │  s3a://landing/yellow_taxi/year=/month=/     │
            │  Raw Parquet + _manifest.json                │
            └────────────────────────┬─────────────────────┘
                                     │  Spark: type cast, attach metadata,
                                     │  tokenize fabricated PII
                                     ▼
            ┌──────────────────────────────────────────────┐
            │  BRONZE  (Iceberg, partitioned by month)     │
            │  bronze.yellow_trips                         │
            │  + ingestion_metadata columns                │
            │  + tokenized PII columns                     │
            └────────────────────────┬─────────────────────┘
                                     │  GE Bronze gate
                ┌────────────────────┴─────────────────────┐
                │ pass                                fail │
                ▼                                          ▼
            dbt Silver                       quarantine.yellow_trips
            ┌──────────────────────┐         (partitioned by
            │  SILVER (Iceberg)    │          ingestion_date,
            │  stg_yellow_trips    │          failure_reason)
            │  stg_taxi_zone       │
            │  + dbt structural    │
            │    tests             │
            └──────────┬───────────┘
                       │  dbt Gold
                       ▼
            ┌──────────────────────────────────────────────┐
            │  GOLD (Iceberg) — dimensional marts          │
            │   dims/  fct_*/                              │
            │   + dbt tests                                │
            └──────────┬───────────────────────────────────┘
                       │  GE Gold gate
                       ▼
            ┌──────────────────────────────────────────────┐
            │  SERVING                                     │
            │  - dbt-exposed Gold views                    │
            │  - one demo dashboard (Superset or notebook) │
            └──────────────────────────────────────────────┘

  Orchestration (everything above): Airflow
  Audit (every step writes a row):  ops.pipeline_audit (Iceberg)
  Metrics:                          Prometheus → Grafana
  Documentation:                    dbt docs + GE Data Docs + docs/
```

---

## 5. Tech stack — the rubric answer

| # | Tool | What it does here | Why this, why not alternatives |
|---|---|---|---|
| 1 | **MinIO** | S3-compatible object store (Landing zone + Iceberg warehouse) | Real object-store semantics (prefix partitioning, multipart upload, eventual consistency) in Docker without an AWS bill. *Local FS rejected:* wrong semantics, no exposure to S3-API quirks. *Real AWS S3 rejected:* coursework reproducibility, no account required. |
| 2 | **Apache Iceberg v2** | Table format for Bronze/Silver/Gold/quarantine/audit | ACID + schema evolution + hidden partitioning + time travel on object storage. TLC's multi-year schema drift maps directly to schema evolution. *Delta Lake rejected:* tighter Spark/Databricks coupling, weaker non-Spark engine story. *Hudi rejected:* CDC-oriented, overkill for monthly immutable batches. *Plain Parquet rejected:* no ACID, no schema evolution metadata, no snapshot isolation. |
| 3 | **Iceberg REST catalog** | Catalog backend for Iceberg | Engine-agnostic, runs in Docker, modern Iceberg-native choice. *Hive Metastore rejected:* heavier deployment, legacy. *AWS Glue rejected:* cloud lock-in for a local project. |
| 4 | **PySpark 3.5** | Landing→Bronze ingestion, PII tokenization, Iceberg writes | Multi-GB monthly files and multi-year backfill exceed single-node tools. Spark provides predicate pushdown, partition-aware writes, and the Iceberg write path. *pandas rejected:* OOM on backfill. *DuckDB rejected:* viable per-month, loses on multi-year + weaker Iceberg write story. |
| 5 | **dbt-spark** | Silver and Gold SQL transformations, tests, docs, snapshots | `ref()` dependency graph, tests-as-code, model docs, native SCD2 via snapshots. *Raw Spark SQL in Airflow rejected:* loses lineage graph, lineage docs, model-level testing, and developer ergonomics. |
| 6 | **dbt tests** | Structural data quality: `not_null`, `unique`, `accepted_values`, `relationships` | Lives next to the model. Owns row-level structural assertions. |
| 7 | **Great Expectations** | Distributional + business-rule quality gates at Bronze and Gold | Distribution drift, rolling-mean row count bounds, quantile checks, multi-column invariants — assertions dbt tests cannot express. *Soda rejected:* lighter weight but less mature ecosystem. *dbt tests alone rejected:* cannot express distributional assertions. |
| 8 | **Airflow 2.9** | DAG orchestration, retries, parameterized backfill, monthly schedule | DAG graph, sensor/operator ecosystem, backfill UI, mature provider for Spark + dbt + GE. *Cron + bash rejected:* no dependency graph, no backfill, no retry semantics. *Prefect/Dagster:* viable alternatives, Airflow chosen for ecosystem maturity and instructor familiarity. |
| 9 | **Iceberg `ops.pipeline_audit` table** | Structured pipeline run history (status, row counts, timings, GE pass rate) | Queryable, joinable, partitioned, survives forever. The right tool for the "did it run, did it pass" questions. *Application logs rejected for this purpose:* not queryable, not joinable. |
| 10 | **Prometheus + Grafana** | Operational metrics dashboards | Pipeline duration trend, rows-per-run, freshness lag, GE pass rate, file size distribution. Grafana also reads `ops.pipeline_audit` via Spark Thrift / Trino for unified dashboards. *Airflow built-in metrics rejected:* can't cross-reference Spark/dbt/GE in one pane. |
| 11 | **dbt docs + GE Data Docs + `docs/`** | Documentation surface | Model lineage and column descriptions in dbt docs; validation history in GE Data Docs; data dictionary and runbook in `docs/`. *OpenMetadata / DataHub rejected:* single data source, single user — catalog tooling is unjustified overhead. |
| 12 | **Docker Compose** | Local deployment of all services | Single-command bring-up, reproducible across machines. *Kubernetes rejected:* out of scope, no operational story to tell. |
| 13 | **pytest** | Unit tests for Spark jobs and PII tokenization logic | Critical paths (tokenization determinism, schema evolution handling) deserve test coverage. |
| 14 | **GitHub Actions** (optional, lightweight) | Lint dbt models, run pytest on PR | Cheap CI signal. Skip if scope-constrained. |

**Tools deliberately not in the stack** (and the rubric justification for omitting them):
- **Kafka / streaming** — monthly batch source; streaming is the wrong shape.
- **Loki / Promtail / ELK** — single-host, single-developer, low-volume; `docker logs` + audit table is right-sized.
- **OpenMetadata / DataHub** — one data source, no cross-team discovery problem.
- **Trino as the primary query engine** — Spark already provides query path; Trino only optionally added if Grafana → Iceberg connectivity needs it.
- **Hive Metastore** — Iceberg REST catalog is the modern equivalent.

---

## 6. Design decisions and rationale

### 6.1 Idempotency: partition overwrite, not MERGE INTO

**Decision:** all Bronze writes use `INSERT OVERWRITE` on the monthly partition. Re-running a month is an atomic metadata swap.

**Rationale:** TLC publishes immutable monthly files; occasional republished corrections replace the whole month. This matches partition-overwrite semantics exactly. MERGE INTO in Iceberg copy-on-write rewrites every data file containing matched rows (O(matched_files × file_size)); merge-on-read writes delete files and pays the cost at read time. MERGE is the right tool for sparse updates scattered across partitions or true row-level CDC — neither applies here.

**Interview-defensible sentence:** "Partition overwrite gives atomic metadata swap with O(new_data) write cost and no read-side merge penalty, matches the source's republish semantics, and is trivially idempotent. MERGE would earn its complexity only for sparse row-level reconciliation, which the source doesn't require."

### 6.2 Medallion layering: four layers, not eight

**Decision:** Landing → Bronze → Silver → Gold. Quality gates are *processes* between layers, not layers themselves.

**Rationale:** "Quality Gate" and "Ingest" are verbs, not zones. Naming them as layers inflates the diagram without adding contract clarity. Each of the four layers has a written contract (Section 4 + Section 7).

### 6.3 dbt vs Great Expectations split

**Decision:**
- **dbt tests** own structural assertions: `not_null`, `unique`, `accepted_values`, `relationships`, referential integrity between dims and facts.
- **Great Expectations** owns distributional and business-rule assertions: row count bounds vs rolling mean, distribution drift, quantile bounds, multi-column invariants.

**Rationale:** the rubric punishes overlapping tools. The split is the industry-standard division of labor and is explicitly defensible.

### 6.4 PDPA handling: deterministic salted tokenization

**Decision:** fabricated PII columns are tokenized at the Landing→Bronze boundary using salted SHA-256. Salt is stored in a separate config (env var in dev, secrets manager in any future deployment). Tokens are deterministic (joinable across runs) and irreversible.

**Right-to-erasure pattern:** salt rotation invalidates all prior tokens; documented in `docs/pdpa.md` even if not exercised.

**Rationale:** demonstrates the pattern transferable to real Thai bank data (the actual portfolio target). Honest framing: this is fabricated PII on US-public data, used to exercise the engineering pattern.

### 6.5 SCD Type 2 on `dim_taxi_zone`

**Decision:** SCD2 implemented via dbt snapshots on the TLC zone lookup CSV.

**Rationale:** the zone lookup is the only honestly-slowly-changing dimension in the source. Changes are rare but real (zone renames, service zone reclassifications). Honest framing in the README: "SCD Type 2 implemented to demonstrate the pattern; changes in practice are infrequent."

Other "dimensions" (`dim_payment_type`, `dim_rate_code`) are static lookups — SCD0/SCD1 only.

### 6.6 Schema evolution: deliberate demo across years

**Decision:** load 2018, 2019, 2021, and 2024 monthly files. Bronze schema evolves via Iceberg `ALTER TABLE ... ADD COLUMN` as new columns appear (`congestion_surcharge` in 2019, `airport_fee` in 2021). Silver coalesces missing columns to NULL with documented semantics.

**Deliverable:** one section in the README showing `SELECT … FROM bronze.yellow_trips WHERE pickup_date BETWEEN '2018-01-01' AND '2024-12-31'` works across the boundary, with snapshot history visible via `SELECT * FROM bronze.yellow_trips.history`.

### 6.7 File size targets

**Decision:** Bronze and Silver target **128–512 MB** per data file. Monthly partitions are compacted to this range via the weekly `iceberg_maintenance` DAG (`rewrite_data_files`).

**Rationale:** small-file problem is a senior-DE concern. Iceberg defaults are fine for writes but compaction is not free — the maintenance DAG demonstrates awareness.

### 6.8 Quarantine semantics

**Decision:** `quarantine.yellow_trips` Iceberg table, partitioned by `(ingestion_date, failure_reason)`. Failed rows from the GE Bronze gate are written here with full provenance (run_id, source_url, expectation_name, expectation_kwargs).

**Rationale:** partition by `failure_reason` enables cheap "all schema violations last month" queries. Quarantine is queryable, not a dead letter dump.

### 6.9 Audit table, not centralized logging

**Decision:** every Airflow task writes one row to `ops.pipeline_audit`. Stack traces remain in `docker logs`. GE details remain in GE Data Docs. dbt details remain in `run_results.json`.

**Rationale:** the realistic debug surface for this project — pipeline status, ingestion metadata, quality outcomes — is structured-event-shaped, not narrative-shaped. Audit table is the appropriate tool. Centralized log aggregation (Loki/ELK) earns its place at higher scale or with on-call rotations; neither applies here.

---

## 7. Layer contracts

| Layer | Location | Schema guarantees | Write semantics | Consumers |
|---|---|---|---|---|
| **Landing** | `s3a://landing/yellow_taxi/year=YYYY/month=MM/` | None — as received from TLC | Immutable; written once per month with sibling `_manifest.json` (URL, sha256, byte size, row count, ingested_at) | Bronze loader only |
| **Bronze** | `bronze.yellow_trips` (Iceberg) | Typed columns, PII tokenized, ingestion metadata columns (`_run_id`, `_source_url`, `_source_sha256`, `_ingested_at`, `_schema_version`) | Partition overwrite by month; idempotent | GE Bronze gate, Silver dbt models |
| **Quarantine** | `quarantine.yellow_trips` (Iceberg) | Bronze schema + `_failure_reason`, `_expectation_kwargs`, `_run_id` | Append-only | Ops investigation, GE Data Docs |
| **Silver** | `silver.stg_yellow_trips`, `silver.stg_taxi_zone`, etc. (Iceberg) | Cleaned, deduped, conformed, joined with zone lookup; structural dbt tests pass | dbt incremental (partition strategy) | Gold dbt models |
| **Gold** | `gold.dim_*`, `gold.fct_*` (Iceberg) | Dimensional model, surrogate keys, documented grain, conformed dims, SCD2 on zone; GE Gold gate passes | dbt incremental or full refresh per model | Serving views, dashboards, notebooks |
| **Serving** | `serving.*` views | Pre-joined, dashboard-shaped | dbt views | Superset / notebook |
| **Audit** | `ops.pipeline_audit` (Iceberg) | One row per Airflow task execution | Append-only, partitioned by `days(started_at)` | Grafana, ops investigation |

---

## 8. Data model — Gold marts

### Conformed dimensions

| Dim | Type | Grain | Notes |
|---|---|---|---|
| `dim_taxi_zone` | **SCD2** | one row per (zone, valid period) | Surrogate key `zone_sk`; natural key `LocationID` |
| `dim_date` | SCD0 | one row per calendar date | Generated, not sourced; includes day-of-week, month, quarter, is_weekend |
| `dim_payment_type` | SCD1 | one row per payment type | Static lookup |
| `dim_rate_code` | SCD1 | one row per rate code | Static lookup |
| `dim_vendor` | SCD1 | one row per vendor | Static lookup |

### Fact tables

| Fact | Grain | Foreign keys | Measures |
|---|---|---|---|
| `fct_trips` | one row per trip | `pickup_zone_sk`, `dropoff_zone_sk`, `pickup_date_sk`, `dropoff_date_sk`, `payment_type_sk`, `rate_code_sk`, `vendor_sk` | fare_amount, tip_amount, total_amount, trip_distance, trip_duration_sec, passenger_count |
| `fct_trips_daily` | one row per (date, pickup_zone, payment_type) | same as above except dropoff | trip_count, total_fare, total_tip, avg_distance, avg_duration |
| `fct_zone_revenue_monthly` | one row per (year_month, zone) | `zone_sk`, `date_sk` (first of month) | trip_count, gross_revenue, tip_pct, avg_fare |
| `fct_tokenization_audit` | one row per ingestion run | `date_sk` | rows_tokenized, distinct_tokens, salt_version |

**Modeling rules:**
- Surrogate keys generated via `dbt_utils.generate_surrogate_key`
- Every fact's `_schema.yml` description begins with `"Grain: one row per …"` — this is the tell that you understand dimensional modeling
- SCD2 implemented via `dbt snapshot`, not hand-rolled MERGE
- No fact joins another fact — facts join dims only

---

## 9. Data quality — explicit expectation lists

### Bronze gate — `bronze_yellow_trips_suite`

Distributional and structural-but-not-trivial assertions:

1. `expect_table_row_count_to_be_between` — bounds derived from 14-month rolling mean ± 2σ of historical row counts
2. `expect_column_mean_to_be_between` on `fare_amount` — rolling-window bounds
3. `expect_column_mean_to_be_between` on `trip_distance` — rolling-window bounds
4. `expect_column_quantile_values_to_be_between` on `trip_distance` p99 — catches sensor-error 200-mile trips
5. `expect_column_quantile_values_to_be_between` on `fare_amount` p99
6. `expect_column_values_to_be_between` on `passenger_count` (0, 9)
7. `expect_column_pair_values_A_to_be_greater_than_B` on (`tpep_dropoff_datetime`, `tpep_pickup_datetime`)
8. `expect_column_values_to_be_between` on `PULocationID` (1, 265)
9. `expect_column_values_to_be_between` on `DOLocationID` (1, 265)
10. `expect_column_value_lengths_to_equal` on tokenized PII columns (SHA-256 hex = 64)
11. `expect_column_kl_divergence_to_be_less_than` on `payment_type` distribution vs reference month
12. `expect_column_proportion_of_unique_values_to_be_between` on `passenger_id` token — sanity check on tokenization

### Gold gate — per-mart suites

- `fct_trips_daily`: row count reconciles to `fct_trips` aggregate within tolerance; no null FKs; date partition completeness
- `fct_zone_revenue_monthly`: tip_pct ∈ [0, 50]; gross_revenue > 0; zone coverage matches `dim_taxi_zone` active rows for the month

### dbt tests (structural — not in GE)

- `not_null` on all FKs in fact tables
- `unique` on surrogate keys in dims
- `relationships` from each fact FK to its dim PK
- `accepted_values` on `payment_type`, `rate_code`, `vendor`
- `dbt_utils.expression_is_true` for `trip_duration_sec > 0`

---

## 10. Airflow DAGs

### `yellow_taxi_monthly_ingest` (monthly, 5th at 02:00)

```
check_tlc_for_new_month
  → download_to_landing
  → validate_manifest (sha256, row count sanity)
  → landing_to_bronze (Spark, partition overwrite)
  → tokenize_pii (Spark, deterministic SHA-256 with salt)
  → ge_checkpoint_bronze
       ├── pass → dbt_run_silver → dbt_test_silver
       │            → dbt_run_gold → dbt_test_gold
       │            → ge_checkpoint_gold
       │            → publish_serving_views
       │            → emit_metrics
       │            → write_audit_row(SUCCESS)
       └── fail → write_to_quarantine
                  → write_audit_row(QUARANTINED)
                  → alert_slack
```

### `iceberg_maintenance` (weekly, Sunday 03:00)

```
rewrite_data_files (target 128–512 MB)
  → expire_snapshots (>30 days)
  → remove_orphan_files
  → write_audit_row
```

### `backfill_yellow_taxi` (manual, parameterized)

```
params: start_month, end_month, force=false
for each month in [start_month, end_month]:
  same task graph as monthly_ingest, but force=true bypasses
  "already loaded" short-circuit
```

**Idempotency contract:** running `backfill_yellow_taxi` for `[2023-01, 2023-12]` produces byte-identical Bronze partitions to running `monthly_ingest` 12 times in order (modulo `_run_id` and `_ingested_at` metadata).

---

## 11. Audit table schema

```sql
CREATE TABLE ops.pipeline_audit (
  run_id                  STRING,
  dag_id                  STRING,
  task_id                 STRING,
  layer                   STRING,           -- 'landing' | 'bronze' | 'silver' | 'gold' | 'maintenance'
  table_name              STRING,
  source_url              STRING,
  source_sha256           STRING,
  schema_version          INT,
  rows_in                 BIGINT,
  rows_out                BIGINT,
  rows_quarantined        BIGINT,
  ge_suite_name           STRING,
  ge_pass_rate            DOUBLE,
  ge_failed_expectations  ARRAY<STRING>,
  started_at              TIMESTAMP,
  finished_at             TIMESTAMP,
  duration_sec            DOUBLE,
  status                  STRING,           -- 'SUCCESS' | 'FAILED' | 'QUARANTINED'
  error_message           STRING,
  triggered_by            STRING            -- 'scheduled' | 'backfill' | 'manual'
)
USING iceberg
PARTITIONED BY (days(started_at));
```

Queries this enables (each becomes a Grafana panel):

- Pipeline duration trend per DAG over time
- Rows ingested per month, with anomaly band
- Freshness lag: `now() - max(finished_at)` per table
- GE pass rate per suite over time
- Quarantine rate per ingestion
- Failed-run leaderboard last 30 days

---

## 12. Observability — Grafana dashboard

Single dashboard, 6 panels:

1. **Pipeline status timeline** — heatmap of DAG runs per day, colored by status (green/yellow/red)
2. **Ingestion duration trend** — line chart, last 12 runs
3. **Rows ingested per run** — bar chart with ±2σ band
4. **GE pass rate by suite** — multi-line, last 30 days
5. **Freshness lag per table** — gauge per Gold mart
6. **File size distribution (Bronze)** — histogram per partition, last 3 months

Backed by `ops.pipeline_audit` (via Spark Thrift or Trino data source) + Prometheus for system-level metrics (container CPU/mem).

---

## 13. Repo layout

```
nyc-yellow-platform/
├── README.md                       # one-line pitch, architecture diagram, tool justification table
├── docker-compose.yml              # MinIO, Spark, Iceberg REST, Airflow, Postgres, Prometheus, Grafana
├── .env.example
├── Makefile                        # up, down, init, demo-ingest, demo-backfill
├── airflow/
│   ├── dags/
│   │   ├── yellow_taxi_monthly_ingest.py
│   │   ├── iceberg_maintenance.py
│   │   └── backfill_yellow_taxi.py
│   └── plugins/
├── spark/
│   ├── jobs/
│   │   ├── landing_to_bronze.py
│   │   ├── tokenize_pii.py
│   │   └── write_audit.py
│   └── conf/
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_yellow_trips.sql
│   │   │   ├── stg_taxi_zone.sql
│   │   │   └── _schema.yml
│   │   └── gold/
│   │       ├── dims/
│   │       │   ├── dim_taxi_zone.sql        # references snapshot
│   │       │   ├── dim_date.sql
│   │       │   ├── dim_payment_type.sql
│   │       │   ├── dim_rate_code.sql
│   │       │   └── dim_vendor.sql
│   │       ├── marts/
│   │       │   ├── trip_analytics/
│   │       │   │   ├── fct_trips.sql
│   │       │   │   ├── fct_trips_daily.sql
│   │       │   │   └── _schema.yml
│   │       │   ├── zone_revenue/
│   │       │   │   ├── fct_zone_revenue_monthly.sql
│   │       │   │   └── _schema.yml
│   │       │   └── governance/
│   │       │       ├── fct_tokenization_audit.sql
│   │       │       └── _schema.yml
│   ├── snapshots/
│   │   └── dim_taxi_zone_snapshot.sql
│   └── tests/                                # singular tests
├── great_expectations/
│   ├── great_expectations.yml
│   ├── expectations/
│   │   ├── bronze_yellow_trips_suite.json
│   │   ├── gold_fct_trips_daily_suite.json
│   │   └── gold_fct_zone_revenue_suite.json
│   ├── checkpoints/
│   │   ├── bronze_gate.yml
│   │   └── gold_gate.yml
│   └── plugins/
├── observability/
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       ├── dashboards/
│       │   └── pipeline_overview.json
│       └── datasources/
│           └── datasources.yml
├── tests/
│   ├── test_tokenize_pii.py                  # determinism, length, salt rotation
│   ├── test_landing_to_bronze.py             # schema evolution handling
│   └── test_audit_writer.py
├── docs/
│   ├── architecture.png
│   ├── tool_justification.md                 # mirror of Section 5
│   ├── data_dictionary.md
│   ├── pdpa.md                               # tokenization design, salt rotation, right-to-erasure
│   ├── schema_evolution.md                   # year-by-year TLC schema changes
│   └── runbook.md                            # how to operate, common failures, recovery
└── .github/
    └── workflows/
        └── ci.yml                            # pytest + dbt parse + GE config validation
```

---

## 14. Build plan — suggested order

Build in slices end-to-end, not layer-by-layer. Each milestone is demoable.

**Milestone 1 — End-to-end thin slice (1 month of data, no quality, no marts)**
- Docker Compose: MinIO, Spark, Iceberg REST, Airflow, Postgres
- Download one month → Landing → Bronze (no PII, no quarantine, no GE)
- One trivial dbt Silver model
- Audit table writing
- *Demo:* DAG runs, Bronze table queryable, audit row exists

**Milestone 2 — PII + quality gates**
- PII fabrication + tokenization
- GE Bronze suite + quarantine table
- GE Gold suite
- Slack alert on failure

**Milestone 3 — Dimensional Gold**
- All dims including SCD2 zone snapshot
- `fct_trips`, `fct_trips_daily`, `fct_zone_revenue_monthly`
- dbt tests, dbt docs

**Milestone 4 — Schema evolution + backfill**
- Load 2018, 2019, 2021, 2024
- Backfill DAG
- Schema evolution demo section in README

**Milestone 5 — Observability + maintenance**
- Prometheus + Grafana with 6 panels
- `iceberg_maintenance` DAG
- File size telemetry

**Milestone 6 — Polish**
- One Superset dashboard or notebook hitting Gold marts
- Full README with architecture diagram, tool justification table, schema evolution demo
- Runbook
- CI (pytest + dbt parse)

---

## 15. Definition of done

A graded submission ships with:

1. `docker compose up` brings the entire stack online with no manual steps beyond `make init`
2. `make demo-ingest` runs the monthly DAG end-to-end for one month and produces queryable Bronze/Silver/Gold tables
3. `make demo-backfill` loads ≥3 years across the schema-evolution boundary
4. README contains: one-line pitch, architecture diagram, full tool justification table (Section 5), schema evolution demo, and "how to reproduce"
5. dbt docs deploys to a browsable location
6. GE Data Docs deploys to a browsable location
7. Grafana dashboard imports from JSON and shows non-empty panels
8. `pytest` passes
9. `dbt test` passes
10. Both GE checkpoints pass on the demo dataset

---

## 16. Risks and open questions

| Risk | Mitigation |
|---|---|
| TLC schedule slip / URL changes | Pin URLs in config; document manual fallback (download + place in Landing) |
| Iceberg REST catalog instability on M-series Mac | Test early in Milestone 1; fall back to Hadoop catalog if needed |
| Schema evolution edge cases (column renames vs add/drop) | Restrict demo to additive changes (the historical reality); document non-additive cases as out of scope |
| Spark memory pressure on 16 GB Mac during full backfill | Backfill month-by-month with explicit checkpointing; don't load all years simultaneously |
| GE rolling-mean bootstrap requires history | Bootstrap with first 14 months hard-coded, then switch to rolling once history exists; document the cutover |

---

## 17. Stretch goals (post-grade, for portfolio uplift)

Not in scope for the graded submission. Reasonable v2 additions if the project becomes portfolio-leading:

- OpenLineage emission from Spark + dbt + Airflow → Marquez for true cross-tool column-level lineage
- Second data source (NYC 311 service requests, daily Socrata) to justify a real data catalog
- Trino as an analytics query engine alongside Spark
- Cost model: per-table estimated AWS-equivalent storage + compute cost
- Notification routing tiers (Slack for warnings, PagerDuty-equivalent for errors)

# TLC Yellow Taxi Data Platform

> A PDPA-aware batch lakehouse for NYC Yellow Taxi data — monthly ingestion through a four-layer Apache Iceberg medallion, dimensional Gold marts, distributional quality gates, partitioned quarantine, and a fully queryable audit layer. Every tool choice is documented with a rejected alternative.

**Course:** DE — Data Architecture / Data Pipeline &nbsp;|&nbsp; **Author:** Purin Boonpetch &nbsp;|&nbsp; ![tests](https://img.shields.io/badge/tests-46%20passing-brightgreen) ![python](https://img.shields.io/badge/python-3.11-blue) ![ruff](https://img.shields.io/badge/lint-ruff-purple)

---

## Table of Contents

1. [Architecture](#architecture)
2. [Quickstart](#quickstart)
3. [Services](#services)
4. [Layer Contracts](#layer-contracts)
5. [Gold Data Model](#gold-data-model)
6. [Quality Gates — dbt vs Great Expectations](#quality-gates)
7. [PDPA Tokenization Pattern](#pdpa-tokenization-pattern)
8. [Schema Evolution](#schema-evolution)
9. [Airflow DAGs](#airflow-dags)
10. [Observability](#observability)
11. [Tool Justification](#tool-justification)
12. [Repo Layout](#repo-layout)
13. [Definition of Done](#definition-of-done)

---

## Architecture

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  TLC CloudFront  (monthly Parquet, ~2-month lag)                    │
  │  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_   │
  └───────────────────────────────┬─────────────────────────────────────┘
                                  │  download_to_landing.py
                                  ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  LANDING   s3a://landing/yellow_taxi/year=YYYY/month=MM/            │
  │  Raw Parquet (as received) + _manifest.json (SHA-256, row count)    │
  └───────────────────────────────┬─────────────────────────────────────┘
                                  │  landing_to_bronze.py
                                  │  • cast_source_schema() — type casts
                                  │  • align_schema()       — Iceberg ALTER TABLE
                                  │  • generate_pii_lookup.py → meta.pii_lookup
                                  │  • tokenize_pii.py      — salted SHA-256
                                  ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  BRONZE   iceberg.bronze.yellow_trips                               │
  │  Partitioned by months(tpep_pickup_datetime)                        │
  │  29 columns: 19 source + 5 ingestion metadata + 4 PII tokens + salt│
  │  Schema evolution: ALTER TABLE ADD COLUMN (idempotent)              │
  └───────────────────────┬─────────────────────────────────────────────┘
                          │  Great Expectations — bronze_gate (12 expectations)
               ┌──────────┴──────────┐
               │ PASS                │ FAIL
               ▼                     ▼
  ┌────────────────────┐   ┌─────────────────────────────────────────────┐
  │  dbt Silver        │   │  iceberg.quarantine.yellow_trips            │
  │  stg_yellow_trips  │   │  Bronze schema + _failure_reason            │
  │  (incremental,     │   │  + _expectation_kwargs + _run_id            │
  │   insert_overwrite)│   │  Partitioned by days(tpep_pickup_datetime)  │
  │  stg_taxi_zone     │   │  and _failure_reason — queryable quarantine │
  │  dbt structural    │   └─────────────────────────────────────────────┘
  │  tests pass here   │
  └──────────┬─────────┘
             │  dbt Gold (dims + facts)
             ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  GOLD   iceberg.gold.*                                              │
  │                                                                     │
  │  Dimensions                   Facts                                 │
  │  ─────────────                ─────────────────────────────────     │
  │  dim_date          (SCD0)     fct_trips          grain: 1/trip      │
  │  dim_vendor        (SCD1)     fct_trips_daily    grain: date×zone×pay│
  │  dim_payment_type  (SCD1)     fct_zone_revenue_monthly  grain: mo×zone│
  │  dim_rate_code     (SCD1)     fct_tokenization_audit    grain: mo×salt│
  │  dim_taxi_zone     (SCD2 →)                                         │
  │  dim_taxi_zone_snapshot                                             │
  └──────────┬──────────────────────────────────────────────────────────┘
             │  Great Expectations — gold_gate (12 expectations)
             ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  SERVING   iceberg.serving.*  (dbt views)                           │
  │  v_trips_summary  ·  v_zone_revenue                                 │
  │  Pre-joined, label-enriched, dashboard-ready                        │
  └─────────────────────────────────────────────────────────────────────┘

  Orchestration:  Airflow 2.9 — 3 DAGs (monthly ingest, backfill, maintenance)
  Audit:          iceberg.ops.pipeline_audit — one row per task, append-only
  Metrics:        Spark/Airflow → Pushgateway → Prometheus → Grafana (6 panels)
  PII:            Clear text lives only in meta.pii_lookup; all downstream layers
                  carry only 64-char salted SHA-256 tokens
```

---

## Quickstart

```bash
# 1. Bring the full stack online (MinIO, Spark, Airflow, Iceberg REST, Grafana, …)
make up && make init

# 2. Run one month end-to-end: Landing → Bronze → Silver → Gold
make demo-ingest          # triggers yellow_taxi_monthly_ingest for 2023-01

# 3. Backfill 2018–2024 across the schema-evolution boundary
make demo-backfill        # triggers backfill DAG for Jan+Jul of 2018/2019/2021/2024
```

`make init` waits for MinIO readiness, creates the two S3 buckets (`landing`, `warehouse`), and creates all Iceberg namespaces (`bronze`, `silver`, `gold`, `quarantine`, `ops`, `meta`).

### Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8082 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Prometheus | http://localhost:9090 | — |
| Spark master | http://localhost:8080 | — |
| Iceberg REST catalog | http://localhost:8181 | — |

### Browse the results

```bash
# After make demo-ingest, open the demo notebook:
jupyter notebook notebooks/demo.ipynb

# dbt lineage + column docs:
cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
# → http://localhost:8080

# GE validation history:
open great_expectations/uncommitted/data_docs/local_site/index.html
```

---

## Services

Ten containers defined in `docker-compose.yml`:

| Container | Image | Purpose |
|---|---|---|
| `minio` | `minio/minio` | S3-compatible object store — Landing zone + Iceberg warehouse |
| `minio-init` | `minio/mc` | One-shot: creates buckets and IAM aliases on first `make init` |
| `iceberg-rest` | `tabulario/iceberg-rest` | Iceberg REST catalog (engine-agnostic, no Hive Metastore) |
| `spark-master` | `bitnami/spark:3.5` | Spark master (also used as SQL entrypoint) |
| `spark-worker` | `bitnami/spark:3.5` | Spark worker |
| `postgres` | `postgres:16` | Airflow metadata database |
| `airflow-webserver` | `apache/airflow:2.9.3-python3.11` | Airflow UI + REST API |
| `airflow-scheduler` | `apache/airflow:2.9.3-python3.11` | DAG scheduling + task dispatch |
| `prometheus` | `prom/prometheus` | Scrapes Pushgateway, MinIO, Spark |
| `pushgateway` | `prom/pushgateway` | Accepts batch-push metrics from Spark jobs |
| `grafana` | `grafana/grafana` | Dashboards — reads from Prometheus |

---

## Layer Contracts

Each layer has a written contract specifying what it guarantees, how it is written, and who reads it.

| Layer | Location | Schema guarantee | Write semantics | Consumers |
|---|---|---|---|---|
| **Landing** | `s3a://landing/yellow_taxi/year=YYYY/month=MM/` | None — byte-for-byte as received from TLC | Immutable; written once per month with a sibling `_manifest.json` (URL, SHA-256, byte size, row count, `ingested_at`) | Bronze loader only |
| **Bronze** | `iceberg.bronze.yellow_trips` | 29 typed columns; PII tokenized; 5 ingestion metadata columns; partition key `months(tpep_pickup_datetime)` | `INSERT OVERWRITE` on the monthly partition — atomic metadata swap, idempotent re-runs | GE Bronze gate, Silver dbt models |
| **Quarantine** | `iceberg.quarantine.yellow_trips` | Full Bronze schema + `_failure_reason STRING`, `_expectation_kwargs STRING`, `_run_id STRING` | Append-only | Ops investigation, GE Data Docs |
| **Silver** | `iceberg.silver.stg_yellow_trips`, `stg_taxi_zone` | Cleaned, conformed, `NULL`→`0` coalescing for evolved columns; dbt structural tests pass | dbt incremental, `insert_overwrite` partitioned by `pickup_month` | Gold dbt models |
| **Gold** | `iceberg.gold.*` | Dimensional model; surrogate keys via `dbt_utils.generate_surrogate_key`; grain documented per model; SCD2 on zones; GE Gold gate passes | dbt table (dims full-refresh) or incremental (facts) | Serving views, Grafana, notebooks |
| **Serving** | `iceberg.serving.*` | Pre-joined, label-enriched, dashboard-shaped | dbt views (no physical write — logical only) | Grafana, Jupyter |
| **Audit** | `iceberg.ops.pipeline_audit` | One row per Airflow task execution; 20 columns covering timing, row counts, GE pass rate, status | Append-only; partitioned by `days(started_at)` | Grafana, ops investigation |

### Why `INSERT OVERWRITE` (not `MERGE INTO`)

TLC publishes immutable monthly files. Occasional republished corrections replace the whole month — which maps exactly to partition overwrite semantics:

- **OVERWRITE** — O(new data) write cost, atomic metadata swap, trivially idempotent.
- **MERGE INTO** — rewrites every data file containing matched rows (copy-on-write) or accumulates delete files (merge-on-read). Correct tool for sparse row-level updates, not bulk monthly replacement.

---

## Gold Data Model

### Dimensions

| Model | Type | Grain | Key | Notes |
|---|---|---|---|---|
| `dim_date` | SCD0 (generated) | One row per calendar day, 2018-01-01 → 2026-12-31 | `date_day`, `date_key` (INT yyyyMMdd) | Columns: year, quarter, month, week_of_year, day_of_week, day_name, month_name, is_weekend. No source table — generated via SQL `SEQUENCE`. |
| `dim_vendor` | SCD1 | One row per vendor ID | `vendor_id` | 1 = Creative Mobile Technologies, 2 = VeriFone |
| `dim_payment_type` | SCD1 | One row per payment type code | `payment_type_id` | Includes code `0 = 'Unknown'` (undocumented TLC rows) |
| `dim_rate_code` | SCD1 | One row per rate code | `rate_code_id` | Includes code `99 = 'Unknown'` |
| `dim_taxi_zone` | View over SCD2 | One row per **active** zone (265 rows) | `location_id`, `zone_sk` | Thin model over `dim_taxi_zone_snapshot WHERE dbt_valid_to IS NULL` — callers join this, not the snapshot |
| `dim_taxi_zone_snapshot` | SCD2 | One row per (zone, valid period) | `dbt_scd_id` (surrogate), `location_id` (natural) | dbt snapshot on `borough`, `zone_name`, `service_zone`; `invalidate_hard_deletes=True` |

### Facts

| Model | Grain | Key columns | Measures |
|---|---|---|---|
| `fct_trips` | One row per trip (`vendor_id` + `pickup_at`) | `trip_id` (MD5 surrogate), `pu_location_id`, `do_location_id`, `payment_type`, `rate_code_id` | `fare_amount`, `tip_amount`, `total_amount`, `trip_distance`, `duration_min`, `tip_pct` |
| `fct_trips_daily` | One row per (`pickup_date`, `pu_location_id`, `payment_type`) | `daily_id` (MD5 surrogate) | `trip_count`, `total_passengers`, `total_distance_mi`, `total_fare`, `total_tip`, `total_revenue`, `avg_tip_pct`, `avg_duration_min` |
| `fct_zone_revenue_monthly` | One row per (`pickup_month`, `pu_location_id`) | `zone_month_id` (MD5 surrogate) | `trip_count`, `total_revenue`, `avg_revenue_per_trip`, `total_distance_mi`, `total_tips`, `avg_tip_pct` |
| `fct_tokenization_audit` | One row per (`pickup_month`, `salt_version`) | `audit_id` (MD5 surrogate) | `total_trips`, `tokenized_count`, `token_coverage_rate`, `distinct_passengers` |

### Modeling rules

- Every fact's `_schema.yml` description begins with `"Grain: one row per …"` — making the grain explicit and testable.
- Surrogate keys use `dbt_utils.generate_surrogate_key` — deterministic MD5, consistent across runs.
- Facts join dims only — no fact-to-fact joins.
- SCD2 implemented via `dbt snapshot` with `strategy: check`, not hand-rolled `MERGE`.
- `dim_taxi_zone` cleanly separates the access pattern (current record) from the snapshot internals.

---

## Quality Gates

The dbt/GE split is a deliberate design choice: each tool handles the assertions it is best suited for.

```
dbt tests          Great Expectations
──────────────     ──────────────────────────────────────────
Structural         Distributional + business-rule
not_null           row count vs rolling mean ± 2σ
unique             column quantile bounds (p99)
accepted_values    KL divergence on payment_type distribution
relationships      multi-column pair invariant
                   token length = 64 (tokenization tripwire)
```

**Why not dbt tests alone?** dbt cannot express distributional assertions or quantile checks.
**Why not GE alone?** GE has no `ref()` graph, no model-level lineage, and slower structural test iteration.

### GE Bronze gate — `bronze_yellow_trips_suite` (12 expectations)

Bounds are empirically grounded from inspection of `yellow_tripdata_{2018,2019,2021,2023}-01.parquet` (see [`docs/data_dictionary.md`](docs/data_dictionary.md)):

| # | Expectation | Threshold / rationale |
|---|---|---|
| 1 | `expect_table_row_count_to_be_between` | 500k – 12M (anchored to 2023-01 = 3,066,766 rows) |
| 2 | `expect_column_mean_to_be_between(fare_amount)` | $10 – $30 |
| 3 | `expect_column_mean_to_be_between(trip_distance)` | 1 – 8 miles |
| 4 | `expect_column_quantile_values_to_be_between(trip_distance, p99)` | 15 – 30 mi (guards 258,928-mile sensor error observed in raw data) |
| 5 | `expect_column_quantile_values_to_be_between(fare_amount, p99)` | $50 – $150 |
| 6 | `expect_column_values_to_be_between(passenger_count)` | 0 – 9 (51,164 zero-passenger rows are valid; 9 is the cab legal max) |
| 7 | `expect_column_pair_values_A_to_be_greater_than_B(tpep_dropoff_datetime, tpep_pickup_datetime)` | Strict — catches 3 negative-duration rows in 2023-01 |
| 8 | `expect_column_values_to_be_between(PULocationID)` | 1 – 265 (taxi zone range) |
| 9 | `expect_column_values_to_be_between(DOLocationID)` | 1 – 265 |
| 10 | `expect_column_value_lengths_to_equal(passenger_email_token)` | 64 — SHA-256 hex output length, tokenization tripwire |
| 11 | `expect_column_value_lengths_to_equal(passenger_id_token)` | 64 |
| 12 | `expect_column_values_to_be_in_set(payment_type)` | {0, 1, 2, 3, 4} |

### GE Gold gate — `gold_fct_trips_suite` (12 expectations)

Covers `fct_trips`: row count reconciliation with Silver, surrogate key uniqueness, no null FKs, `trip_duration_sec > 0`, `tip_pct ∈ [0, 200]`, date partition completeness, and token length.

### dbt structural tests

Defined in `_schema.yml` files next to each model:

- `not_null` on all FK columns and surrogate keys in facts
- `unique` on surrogate keys in all dims and facts
- `relationships` from each fact FK to its dimension PK
- `accepted_values` on `payment_type`, `rate_code_id`, `vendor_id`

---

## PDPA Tokenization Pattern

> **Honest framing:** TLC data contains no real PII — identifying fields are stripped before publication. This project fabricates synthetic PII to exercise the engineering pattern end-to-end; the design is directly transferable to real PDPA-sensitive sources (e.g., Thai financial transaction data).

### Data flow

```
spark/jobs/generate_pii_lookup.py
  │  Reads bronze.yellow_trips for the month
  │  Generates deterministic synthetic PII per (VendorID, tpep_pickup_datetime):
  │    passenger_email, passenger_phone, payment_card_last4, passenger_id
  └→ writes iceberg.meta.pii_lookup (INSERT OVERWRITE, idempotent)

spark/jobs/tokenize_pii.py
  │  Reads bronze.yellow_trips + meta.pii_lookup
  │  Joins on (VendorID, tpep_pickup_datetime)
  │  Applies tokenize(value, salt) to each PII column in-flight
  │  Appends token columns + _salt_version; drops nothing
  └→ overwrites the Bronze monthly partition (token columns added)

Result: Bronze carries only
  passenger_email_token    (64-char SHA-256 hex)
  passenger_phone_token    (64-char SHA-256 hex)
  payment_card_last4_token (64-char SHA-256 hex)
  passenger_id_token       (64-char SHA-256 hex)
  _salt_version            (INT — tracks which salt was used)

Raw PII exists ONLY in iceberg.meta.pii_lookup (isolated namespace, never
referenced by dbt, never exposed in Serving).
```

### Tokenization algorithm

```python
import hashlib

def tokenize(value: str | None, salt: str) -> str | None:
    if value is None:
        return None
    return hashlib.sha256((value + salt).encode()).hexdigest()
    # → always 64 hex characters, e.g. "a3f8c1d2e4…"
```

Properties: **deterministic** (tokens are joinable across runs), **irreversible** (SHA-256 is one-way), **salt-sensitive** (different salt → completely different tokens), **length-stable** (always 64 chars — GE expectations #10/#11 verify this as a regression tripwire).

### Salt management

| Environment | Salt storage |
|---|---|
| Development | `PII_SALT` + `PII_SALT_VERSION` env vars in `.env` (git-ignored) |
| Production | Secrets manager (AWS Secrets Manager, HashiCorp Vault, etc.) |

### Salt rotation — right-to-erasure equivalent

Rotating the salt makes all prior tokens cryptographically unresolvable, effectively erasing linkability between tokens and identities.

```bash
# 1. Update PII_SALT and increment PII_SALT_VERSION in .env
# 2. Re-tokenize all affected months
for ym in 2023-01 2023-02 ...; do
  year=${ym%-*}; month=${ym#*-}
  spark-submit spark/jobs/tokenize_pii.py --year $year --month $month
done
# 3. dbt re-materializes Silver + Gold on next run — downstream token columns update automatically
```

The `_salt_version` column in Bronze and `fct_tokenization_audit.salt_version` show which version each month used, so coverage gaps after rotation are visible as SQL.

See [`docs/pdpa.md`](docs/pdpa.md) for the full design rationale.

---

## Schema Evolution

The TLC CloudFront files retroactively unified the column list across all years — every file exposes the same 19 columns. "Schema evolution" here is **semantic** (null density changes at a known date), not physical. The challenge is loading older months after newer months without breaking queries.

| Column | Semantically introduced | Prior months |
|---|---|---|
| `congestion_surcharge` | 2019-02-01 | 100% null for 2018 and 2019-01 |
| `airport_fee` | 2021-01-01 | 100% null for all prior years |

### How `align_schema()` handles it

`spark/jobs/landing_to_bronze.py:align_schema()` runs before every Bronze write:

1. **Column in source, absent from Iceberg table** → `ALTER TABLE … ADD COLUMN` (additive, idempotent — guards against re-running the same month).
2. **Column in Iceberg table, absent from source DataFrame** → `df.withColumn(col, lit(None))` — preserves source fidelity, Bronze never coalesces.

Silver then coalesces: `COALESCE(congestion_surcharge, 0)`, `COALESCE(airport_fee, 0)`, with a comment explaining the introduction year.

### Cross-year query (after `make demo-backfill`)

```sql
-- Spans 2018 → 2024 without column errors
SELECT
    year(tpep_pickup_datetime)   AS year,
    month(tpep_pickup_datetime)  AS month,
    COUNT(*)                     AS trips,
    AVG(congestion_surcharge)    AS avg_congestion,
    AVG(airport_fee)             AS avg_airport_fee
FROM iceberg.bronze.yellow_trips
GROUP BY 1, 2
ORDER BY 1, 2;
```

Expected output (sample):

| year | month | trips | avg_congestion | avg_airport_fee |
|---:|---:|---:|---:|---:|
| 2018 | 1 | ~8.7M | NULL | NULL |
| 2019 | 1 | ~7.8M | 0.64 | NULL |
| 2021 | 1 | ~1.0M | 0.45 | 0.02 |
| 2024 | 1 | ~3.1M | 0.57 | 0.09 |

*(NULL in Bronze = column present in schema but semantically absent for that year. Silver coalesces to 0.)*

### Iceberg time travel

```sql
-- Every Bronze write creates a snapshot — queryable as history
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg.bronze.yellow_trips.history
ORDER BY committed_at DESC LIMIT 10;
```

See [`docs/schema_evolution.md`](docs/schema_evolution.md) for the full analysis.

---

## Airflow DAGs

All task logic lives in `airflow/dags/shared/` — factory functions returning configured operators. Each DAG file is thin wiring only (~30–40 lines).

### `yellow_taxi_monthly_ingest` — runs 5th of each month at 02:00 UTC

```
download_to_landing          (SparkSubmitOperator — downloads Parquet to MinIO)
  → landing_to_bronze        (Spark — cast, align_schema, write Bronze partition)
  → generate_pii_lookup      (Spark — fabricate deterministic synthetic PII)
  → tokenize_pii             (Spark — salted SHA-256, overwrite Bronze partition)
  → ge_checkpoint_bronze     (BashOperator — runs GE suite, captures exit code)
       │
       ├── [exit 0] write_audit_success
       │              → dbt run silver  → dbt test silver
       │              → dbt run gold    → dbt test gold
       │              → ge_checkpoint_gold
       │
       └── [exit ≠0] write_to_quarantine
                      → write_audit_quarantined
                      → alert_slack  (logging stub — extend for real alerting)
```

Trigger manually for a specific month:
```bash
airflow dags trigger yellow_taxi_monthly_ingest \
  --conf '{"year": 2023, "month": 6}'
```

### `backfill_yellow_taxi` — manual, parameterized

Processes months sequentially (not fanned out) to bound Spark memory. Each month runs the identical step graph as the monthly ingest DAG.

```bash
airflow dags trigger backfill_yellow_taxi \
  --conf '{"start_month": "2018-01", "end_month": "2018-12"}'
```

**Idempotency contract:** running the backfill for `[2023-01, 2023-12]` produces byte-identical Bronze partitions to running the monthly ingest 12 times in order (modulo `_run_id` and `_ingested_at` metadata columns).

### `iceberg_maintenance` — every Sunday at 03:00 UTC

```
rewrite_data_files  (targets 128–512 MB per file via CALL iceberg.system.rewrite_data_files)
  → expire_snapshots  (drops snapshots older than 30 days)
  → remove_orphan_files  (removes files not referenced by any snapshot)
```

Each step writes an audit row to `ops.pipeline_audit`. A failure in one table does not block other tables — each table is processed independently within the step.

---

## Observability

### Grafana dashboard (6 panels)

Imported automatically from `observability/grafana/dashboards/pipeline_overview.json`:

| Panel | Type | Query |
|---|---|---|
| 1. Pipeline run status | State timeline | `pipeline_run_status` gauge per dag_id |
| 2. Ingestion duration trend | Time series | `pipeline_run_duration_seconds` last 12 runs |
| 3. Rows ingested per run | Bar chart | `pipeline_rows_written` per task |
| 4. GE pass rate by suite | Multi-line | `pipeline_ge_pass_rate` per layer |
| 5. Freshness lag per table | Gauge | `time() - pipeline_last_finished_ts` |
| 6. Rows quarantined | Bar chart | `pipeline_rows_quarantined` per run |

### Metric push flow

Batch pipelines cannot be scraped directly by Prometheus (they are short-lived). Instead, each Spark job calls `push_metrics()` in `write_audit.py` after writing its audit row:

```
Spark job finishes
  → write_audit_row(record, spark)    # Iceberg append to ops.pipeline_audit
  → push_metrics(record)              # HTTP POST to Pushgateway (best-effort)
       ↑
  Prometheus scrapes Pushgateway every 15s
       ↑
  Grafana reads from Prometheus
```

Six metrics are pushed: `pipeline_run_status`, `pipeline_run_duration_seconds`, `pipeline_rows_written`, `pipeline_rows_quarantined`, `pipeline_ge_pass_rate`, `pipeline_last_finished_ts`.

### Audit table queries

```sql
-- End-to-end run history
SELECT dag_id, task_id, status, rows_out, duration_sec, started_at
FROM iceberg.ops.pipeline_audit
ORDER BY started_at DESC LIMIT 20;

-- Freshness: when was each Gold table last written?
SELECT table_name, MAX(finished_at) AS last_run, status
FROM iceberg.ops.pipeline_audit
WHERE layer = 'gold'
GROUP BY table_name, status;

-- Quarantine rate over time
SELECT DATE_TRUNC('month', started_at) AS month,
       SUM(rows_quarantined) AS quarantined,
       SUM(rows_out) AS total
FROM iceberg.ops.pipeline_audit
GROUP BY 1 ORDER BY 1;
```

---

## Tool Justification

| # | Tool | Role | Why this — not alternatives |
|---|---|---|---|
| 1 | **MinIO** | S3-compatible Landing + Iceberg warehouse | Real object-store semantics (prefix partitioning, multipart upload, path-style access) in Docker with no AWS bill. _Local FS rejected:_ wrong API semantics. _AWS S3 rejected:_ reproducibility — no account required. |
| 2 | **Apache Iceberg v2** | Table format for all layers | ACID + schema evolution + hidden partitioning + time travel on object storage. TLC's multi-year semantic schema drift (two columns introduced across 2019–2021) maps directly to Iceberg `ADD COLUMN`. _Delta Lake rejected:_ tighter Spark/Databricks coupling, weaker non-Spark engine story. _Hudi rejected:_ CDC-oriented, overkill for monthly immutable batches. _Plain Parquet rejected:_ no ACID, no schema evolution metadata, no snapshot isolation. |
| 3 | **Iceberg REST catalog** | Engine-agnostic catalog | Modern, Docker-native, no Thrift server. _Hive Metastore rejected:_ heavier deployment, legacy Thrift protocol. _AWS Glue rejected:_ cloud lock-in for a local-first project. |
| 4 | **PySpark 3.5** | Ingestion + PII tokenization + maintenance | Multi-GB monthly files and multi-year backfill exceed single-node tools. Predicate pushdown, partition-aware writes, the Iceberg write path. _pandas rejected:_ OOM on multi-year backfill. _DuckDB rejected:_ viable per-month, but weaker Iceberg write path and no distributed execution. |
| 5 | **dbt-spark** | Silver + Gold SQL transformations | `ref()` dependency graph, tests-as-code, model lineage docs, native SCD2 via snapshots, `insert_overwrite` incremental strategy for Iceberg. _Raw Spark SQL in Airflow rejected:_ no lineage graph, no model-level tests, no column docs. |
| 6 | **dbt tests** | Structural quality gates | `not_null`, `unique`, `accepted_values`, `relationships` — row-level structural assertions living next to the model, version-controlled with the transformation. |
| 7 | **Great Expectations 0.18** | Distributional quality gates | Distribution drift detection, rolling-mean row count bounds, quantile checks, multi-column invariants — assertions dbt cannot express. _Soda rejected:_ less mature ecosystem at time of selection. _dbt tests alone rejected:_ fundamentally cannot express distributional assertions. **The dbt/GE split is the key architectural rubric point.** |
| 8 | **Airflow 2.9** | DAG orchestration | DAG dependency graph, retry semantics, `BranchPythonOperator` for GE routing, parameterized backfill via `Param`, `SparkSubmitOperator`. _Cron + bash rejected:_ no dependency graph, no retry, no backfill. _Prefect/Dagster:_ viable — Airflow chosen for ecosystem maturity and instructor familiarity. |
| 9 | **`ops.pipeline_audit` (Iceberg)** | Structured run history | Queryable as SQL, joinable to Bronze/Gold data, append-only, partitioned by `days(started_at)`. Answers "did it run, when, with what inputs, did it pass quality" without log scraping. _Application logs rejected:_ not queryable, not joinable. |
| 10 | **Prometheus + Pushgateway + Grafana** | Operational metrics | Unified dashboard across pipeline duration, row counts, freshness lag, GE pass rate. Pushgateway bridges batch Spark jobs → Prometheus time-series (batch jobs cannot be scraped directly). |
| 11 | **dbt docs + GE Data Docs** | Documentation surface | Model lineage graph + column descriptions (dbt); validation history with expectation detail (GE). _OpenMetadata/DataHub rejected:_ single data source, single user — catalog tooling is unjustified overhead. |
| 12 | **Docker Compose** | Local deployment | Single-command bring-up of 10 services, reproducible on any machine with Docker. _Kubernetes rejected:_ out of scope; no operational story to tell at this scale. |
| 13 | **pytest** | Unit tests | Tokenization determinism, schema evolution handling, `align_schema` idempotency, `AuditRecord` field defaults, `push_metrics` encoding — all tested without a live cluster. 46 tests, pure functions only. |
| 14 | **GitHub Actions** | CI | `ruff` lint, `pytest`, `dbt parse` (import validation), GE suite JSON schema validation on every PR. No live cluster required — all checks pass with the dev venv. |

**Tools deliberately excluded:**
- **Kafka/streaming** — monthly batch source; streaming is the wrong shape.
- **Loki/ELK** — single host, low volume; `docker logs` + audit table is right-sized.
- **OpenMetadata/DataHub** — one data source, no cross-team discovery problem.
- **Trino** — Spark already provides the query path for this project.
- **Hive Metastore** — Iceberg REST catalog is the modern replacement.

---

## Repo Layout

```
.
├── airflow/
│   ├── dags/
│   │   ├── yellow_taxi_monthly_ingest.py   # thin wiring (~40 lines)
│   │   ├── backfill_yellow_taxi.py         # thin wiring (~35 lines)
│   │   ├── iceberg_maintenance.py          # thin wiring (~30 lines)
│   │   └── shared/                         # factory functions shared across all 3 DAGs
│   │       ├── callbacks.py                # on_failure callback with layer detection
│   │       ├── tasks_ingest.py             # make_download_task, make_landing_to_bronze_task
│   │       ├── tasks_pii.py                # make_generate_pii_task, make_tokenize_task
│   │       ├── tasks_quality.py            # make_ge_gate_task, make_branch_task
│   │       ├── tasks_audit.py              # make_quarantine_task, write_audit_success, …
│   │       ├── tasks_dbt.py                # make_dbt_silver_run/test, make_dbt_gold_run/test
│   │       ├── tasks_backfill.py           # run_backfill, summarise
│   │       └── tasks_maintenance.py        # rewrite_data_files, expire_snapshots, …
│   └── plugins/__init__.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── seeds/taxi_zone_lookup.csv          # 265 TLC zones (source for dim_taxi_zone)
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_yellow_trips.sql        # incremental, insert_overwrite, COALESCE evolved cols
│   │   │   ├── stg_taxi_zone.sql           # from seed
│   │   │   └── _schema.yml / _sources.yml
│   │   ├── gold/
│   │   │   ├── dims/
│   │   │   │   ├── dim_date.sql            # generated calendar spine 2018–2026
│   │   │   │   ├── dim_taxi_zone.sql       # current SCD2 record (dbt_valid_to IS NULL)
│   │   │   │   ├── dim_vendor.sql
│   │   │   │   ├── dim_payment_type.sql    # includes code 0='Unknown'
│   │   │   │   ├── dim_rate_code.sql       # includes code 99='Unknown'
│   │   │   │   └── _schema.yml
│   │   │   └── marts/
│   │   │       ├── trip_analytics/fct_trips.sql
│   │   │       ├── trip_analytics/fct_trips_daily.sql
│   │   │       ├── zone_revenue/fct_zone_revenue_monthly.sql
│   │   │       ├── governance/fct_tokenization_audit.sql
│   │   │       └── _schema.yml
│   │   └── serving/
│   │       ├── v_trips_summary.sql
│   │       └── v_zone_revenue.sql
│   └── snapshots/
│       └── dim_taxi_zone_snapshot.sql      # SCD2 via dbt snapshot, check strategy
├── spark/
│   ├── jobs/
│   │   ├── config.py                       # Settings (pydantic-settings), Iceberg + S3 conf
│   │   ├── download_to_landing.py          # HTTP download + SHA-256 + _manifest.json
│   │   ├── landing_to_bronze.py            # cast_source_schema, align_schema, Bronze write
│   │   ├── generate_pii_lookup.py          # deterministic fabricated PII → meta.pii_lookup
│   │   ├── tokenize_pii.py                 # salted SHA-256 tokenization, Bronze overwrite
│   │   ├── quarantine_writer.py            # GE-failed row routing → quarantine table
│   │   ├── iceberg_maintenance.py          # rewrite/expire/orphan stored procedures
│   │   └── write_audit.py                  # AuditRecord, write_audit_row, push_metrics
│   └── conf/spark-defaults.conf
├── great_expectations/
│   ├── great_expectations.yml
│   ├── expectations/
│   │   ├── bronze_yellow_trips_suite.json  # 12 distributional expectations
│   │   └── gold_fct_trips_suite.json       # 12 expectations (row count reconciliation, FKs, …)
│   └── checkpoints/
│       ├── bronze_gate.yml
│       └── gold_gate.yml
├── observability/
│   ├── prometheus/prometheus.yml           # scrapes MinIO, Spark, Pushgateway
│   └── grafana/
│       ├── dashboards/pipeline_overview.json  # 6-panel dashboard (auto-provisioned)
│       └── datasources/datasources.yml
├── tests/                                  # 46 pure-function tests — no live cluster
│   ├── test_download_to_landing.py         # URL builder, landing prefix logic
│   ├── test_landing_to_bronze.py           # cast_source_schema, add_ingestion_metadata
│   ├── test_schema_evolution.py            # align_schema — 13 tests incl. idempotency
│   ├── test_tokenize_pii.py                # tokenize() determinism, salt sensitivity, UDF
│   └── test_audit_writer.py               # AuditRecord defaults, push_metrics encoding
├── notebooks/demo.ipynb                   # Bronze row count, PDPA check, Gold mart queries,
│                                          # SCD2 verification, audit table, time travel
├── docs/
│   ├── data_dictionary.md                 # Empirical column analysis of 4 sample months
│   ├── pdpa.md                            # Tokenization design, salt rotation, erasure pattern
│   ├── schema_evolution.md                # Year-by-year null density, cross-year query demo
│   └── runbook.md                         # Common failures and recovery procedures
├── docker-compose.yml                     # 10 services
├── Makefile                               # up, down, init, lint, test, demo-ingest, demo-backfill
├── pyproject.toml                         # ruff, mypy, pytest config
└── .env.example                           # Required env vars (copy to .env, never commit)
```

---

## Definition of Done

| # | Criterion | How to verify |
|---|---|---|
| 1 | `docker compose up` + `make init` brings the full 10-service stack online | `docker compose ps` — all services healthy |
| 2 | `make demo-ingest` produces queryable Bronze, Silver, and Gold tables | `notebooks/demo.ipynb` — all cells run without error |
| 3 | `make demo-backfill` loads ≥3 years across the schema-evolution boundary | Cross-year query in schema evolution section above returns rows for 2018–2024 |
| 4 | README: pitch, diagram, tool table, schema demo, PDPA, quickstart | This file |
| 5 | dbt docs deployable and browsable | `cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .` |
| 6 | GE Data Docs deployable | `open great_expectations/uncommitted/data_docs/local_site/index.html` |
| 7 | Grafana dashboard loads with non-empty panels after a demo run | http://localhost:3000 → TLC Pipeline Overview |
| 8 | `pytest` passes | `make test` → **46 tests**, 0 failures |
| 9 | `dbt test` passes against demo data | `cd dbt && dbt test --profiles-dir .` |
| 10 | Both GE checkpoints pass on the demo dataset | `great_expectations checkpoint run bronze_gate` and `gold_gate` |

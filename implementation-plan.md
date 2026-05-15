# Implementation Plan — NYC Yellow Taxi Data Platform

> Companion to `project-spec.md`. The spec answers *what* and *why*; this plan answers *how* and *in what order*, with clean-code and best-practice standards applied at each step.

Build in **vertical slices**, not horizontal layers. Each milestone produces a demoable, working pipeline; later milestones deepen capability without rebuilding earlier work.

---

## Engineering standards (apply to every step)

- **Source control:** one feature branch per milestone; conventional-commit messages (`feat:`, `fix:`, `refactor:`, `docs:`, `test:`); PR per milestone with checklist.
- **Configuration over hardcoding:** every path, URL, salt, bucket, and catalog name lives in `.env` (read via `pydantic-settings` or `os.getenv` with explicit defaults). No literal `s3a://...` strings inside job code.
- **Idempotency by default:** every task is safe to re-run. Writes use partition overwrite; audit rows include `run_id`.
- **Typed Python:** PEP 604 type hints on every public function; `mypy --strict` clean on `spark/jobs/` and `airflow/plugins/`.
- **Linting/formatting:** `ruff` (lint + format) and `sqlfluff` (dbt SQL) wired as a pre-commit hook.
- **Tests live next to code paths they protect:** `tests/test_<module>.py` mirrors `spark/jobs/<module>.py`.
- **No silent failures:** every `except` clause names the exception and writes a `FAILED` audit row before re-raising.
- **Small composable functions:** Spark jobs are *thin* orchestration calling pure transformation functions that take/return DataFrames — those pure functions are what pytest exercises.
- **Documentation as you go:** update `docs/` in the same PR as the feature; never defer.

---

## Milestone 0 — Repo & environment bootstrap

**Goal:** an empty but correctly shaped repository.

1. `git init`; add `.gitignore` (Python, Spark, dbt `target/`, `logs/`, `.env`, MinIO data dirs).
2. Create the directory tree from spec §13 with empty `__init__.py` / `README.md` placeholders so layout is reviewable from commit 1.
3. Add `.env.example` enumerating every variable (MinIO keys, salt, Iceberg warehouse path, Airflow Fernet key).
4. Add `pyproject.toml` with `ruff`, `mypy`, `pytest`, `pytest-spark`, `pydantic-settings` as dev deps.
5. Add `.pre-commit-config.yaml` (ruff, sqlfluff, end-of-file-fixer, trailing-whitespace).
6. Write top-level `README.md` skeleton (pitch, architecture placeholder, "how to run" stub).
7. Add `Makefile` targets: `init`, `up`, `down`, `lint`, `test`, `demo-ingest`, `demo-backfill` (stubs returning `echo "TODO"`).

**Done when:** `make lint` and `make test` exit 0 against an empty codebase; `tree` output matches spec §13.

---

## Milestone 1 — End-to-end thin slice (one month, no quality, no marts)

**Goal:** prove the seams. One real month flows Landing → Bronze and an audit row appears.

1. **Docker Compose** — `docker-compose.yml` for MinIO, Iceberg REST catalog, Spark master+worker, Airflow (webserver + scheduler + standalone metadata Postgres). Pin every image tag; never use `latest`.
2. **MinIO bootstrap** — one-shot `mc` container creates buckets `landing`, `warehouse`, `quarantine` on first boot.
3. **Iceberg catalog config** — `spark-defaults.conf` wires the REST catalog and `s3a` filesystem against MinIO. Confirmed by a smoke test: `CREATE NAMESPACE bronze` from `spark-sql`.
4. **Settings module** — `spark/jobs/config.py` exposes typed `Settings` (pydantic) so jobs never read env vars directly.
5. **Landing loader** — `spark/jobs/download_to_landing.py`:
   - Pure function `build_landing_path(year, month) -> str`.
   - Streams TLC parquet to `s3a://landing/yellow_taxi/year=Y/month=M/`.
   - Writes sibling `_manifest.json` (URL, sha256, byte_size, row_count, ingested_at) atomically (`.tmp` → rename).
6. **Bronze writer** — `spark/jobs/landing_to_bronze.py`:
   - Reads landing parquet, casts types explicitly (no schema inference in prod path), adds ingestion metadata columns.
   - Writes Iceberg `bronze.yellow_trips` with `INSERT OVERWRITE` on the monthly partition.
7. **Audit writer** — `spark/jobs/write_audit.py` exposing one function `write_audit_row(record: AuditRecord)`. Uses an Iceberg `INSERT` against `ops.pipeline_audit`. `AuditRecord` is a `@dataclass`.
8. **Airflow DAG** — `airflow/dags/yellow_taxi_monthly_ingest.py`, three tasks: `download_to_landing → landing_to_bronze → write_audit_row`. Uses `SparkSubmitOperator`. DAG-level `default_args` set retries=2, retry_delay=5m.
9. **pytest baseline** — `tests/test_landing_to_bronze.py` exercises the pure cast/metadata function with a tiny in-memory DataFrame; no Iceberg, no MinIO.

**Done when:**
- `make up && make demo-ingest` succeeds end-to-end for `2023-01`.
- `SELECT COUNT(*) FROM bronze.yellow_trips` returns rows.
- `SELECT * FROM ops.pipeline_audit` shows one `SUCCESS` row per task.

---

## Milestone 2 — PII tokenization & quality gates

**Goal:** the dbt-vs-GE split is real; failed rows have a home.

1. **PII fabrication** — `spark/jobs/generate_pii_lookup.py` produces a deterministic lookup keyed by `(VendorID, tpep_pickup_datetime)`. Stored once in `meta.pii_lookup` (Iceberg). Re-runs are no-ops.
2. **Tokenization** — `spark/jobs/tokenize_pii.py`:
   - Pure function `tokenize(value: str, salt: str) -> str` (SHA-256 hex). Unit-tested for determinism and salt sensitivity.
   - Joins PII lookup into Bronze on the natural key; replaces raw PII with tokenized columns; raw PII never lands on disk in any layer.
3. **Salt management** — salt read from `Settings.pii_salt`, version stamped into every Bronze row (`_salt_version`). Document rotation procedure in `docs/pdpa.md`.
4. **GE Bronze suite** — `great_expectations/expectations/bronze_yellow_trips_suite.json` with the 12 expectations from spec §9. Bootstrap rolling-window bounds from a fixed 14-month seed.
5. **Quarantine writer** — `spark/jobs/quarantine_writer.py`: on GE failure, writes failed rows + `_failure_reason`, `_expectation_kwargs`, `_run_id` to `quarantine.yellow_trips` partitioned by `(ingestion_date, failure_reason)`.
6. **Gate orchestration** — `ge_checkpoint_bronze` Airflow task uses `GreatExpectationsOperator`; on failure, `BranchPythonOperator` routes to `write_to_quarantine → write_audit_row(QUARANTINED) → alert_slack`. Success path continues downstream (added in M3).
7. **Tests** — `tests/test_tokenize_pii.py`: determinism, length=64, salt rotation invalidates prior tokens, NULL inputs handled.

**Done when:**
- Forcing a deliberately bad month (manually corrupted parquet) routes rows to quarantine with full provenance.
- `pytest tests/test_tokenize_pii.py` passes.
- No raw PII column appears in `SHOW COLUMNS FROM bronze.yellow_trips`.

---

## Milestone 3 — Silver + dimensional Gold

**Goal:** the dbt graph is real; facts join only dims; SCD2 works.

1. **dbt project bootstrap** — `dbt_project.yml`, `profiles.yml` against Spark Thrift / `dbt-spark` over the Iceberg session. Add `dbt_utils` and `dbt_expectations` to `packages.yml`.
2. **Silver staging** — `models/silver/stg_yellow_trips.sql` (incremental, partitioned by month) and `stg_taxi_zone.sql`. Materialization: `incremental` with `insert_overwrite`. Every column has a description in `_schema.yml`. **Every Silver/Gold model adds `current_timestamp() as _transformed_at`** so end-to-end latency (`_transformed_at - _ingested_at`) is queryable per row.
3. **dbt structural tests** — `not_null`, `unique`, `accepted_values`, `relationships` per spec §9.
4. **Gold dimensions** — one file per dim under `models/gold/dims/`. Surrogate keys via `dbt_utils.generate_surrogate_key`. `dim_taxi_zone` referenced from a `snapshots/dim_taxi_zone_snapshot.sql` (SCD2 with `check_cols`).
5. **Gold facts** — `fct_trips`, `fct_trips_daily`, `fct_zone_revenue_monthly`, `fct_tokenization_audit`. Each `_schema.yml` description starts with `"Grain: one row per …"`. Carry `_ingested_at` forward from Bronze and stamp a fresh `_transformed_at` at this layer (so latency can be measured Landing→Bronze and Bronze→Gold separately).
6. **GE Gold suite** — per-mart suites from spec §9 (row-count reconciliation, tip_pct bounds, zone coverage).
7. **Airflow integration** — extend monthly DAG: `dbt_run_silver → dbt_test_silver → dbt_run_gold → dbt_test_gold → ge_checkpoint_gold → publish_serving_views`. Use `BashOperator` with `--select` flags; never `dbt run` the whole project.
8. **Serving views** — `models/serving/*.sql` as `materialized='view'`; thin pre-joined shapes for dashboarding.
9. **dbt docs** — `dbt docs generate && dbt docs serve` served from a Compose sidecar on a known port.

**Done when:**
- `dbt test` passes against a real month.
- `dim_taxi_zone` shows multiple rows for any zone with `valid_to IS NULL` exactly once.
- dbt docs lineage graph shows the full Silver→Gold→Serving DAG.

---

## Milestone 4 — Schema evolution & backfill

**Goal:** queries span 2018–2024 across additive schema changes; backfill is byte-equivalent to monthly.

1. **Bronze schema-evolution handling** — `landing_to_bronze` detects new columns vs current Iceberg schema; emits `ALTER TABLE … ADD COLUMN` before write. Wrapped in a tested helper `align_schema(df, table_name)`.
2. **Silver column coalescing** — `stg_yellow_trips.sql` uses `coalesce(congestion_surcharge, 0)` / `coalesce(airport_fee, 0)` with comments referencing the year columns appeared. Documented in `docs/schema_evolution.md`.
3. **Backfill DAG** — `backfill_yellow_taxi.py`, parameterized `start_month`, `end_month`, `force`. Iterates month-by-month (not a fan-out) to control memory. Reuses the same task functions as monthly DAG via shared callables.
4. **Idempotency proof** — `tests/test_idempotency.py`: run the same month twice, hash Bronze partition data files (excluding metadata columns), assert equality.
5. **Demo data** — load Jan/Jul of 2018, 2019, 2021, 2024 (eight months) for the schema-evolution demo. Document the command in `docs/schema_evolution.md`.

**Done when:**
- `SELECT … FROM bronze.yellow_trips WHERE pickup_date BETWEEN '2018-01-01' AND '2024-12-31'` runs without column errors.
- `SELECT * FROM bronze.yellow_trips.history` shows snapshots from each backfill month.
- `make demo-backfill START=2018-01 END=2024-12` completes.

---

## Milestone 5 — Observability & maintenance

**Goal:** a single Grafana board answers "is the pipeline healthy."

1. **Prometheus** — scrape Airflow's StatsD exporter, MinIO `/minio/v2/metrics/cluster`, Spark history server. `prometheus.yml` lives in `observability/prometheus/`.
2. **Grafana provisioning** — datasources (Prometheus + Spark Thrift/Trino for `ops.pipeline_audit`) and dashboard JSON committed to `observability/grafana/`. Auto-loaded on container start.
3. **Six-panel dashboard** — exactly the panels in spec §12; no extras.
4. **`iceberg_maintenance` DAG** — weekly: `rewrite_data_files` (target 128–512 MB), `expire_snapshots` (>30d), `remove_orphan_files`. Each task writes an audit row.
5. **File-size telemetry** — small Spark job after Bronze write reports file-count and size histogram into `ops.pipeline_audit` via the `rows_out` + a JSON `metadata` column (add column in this milestone with a documented schema migration).

**Done when:**
- Grafana board imports from JSON and shows non-empty data for all six panels.
- Manually running `iceberg_maintenance` reduces Bronze file count for a fragmented month.

---

## Milestone 6 — Polish, CI, demo

1. **CI** — `.github/workflows/ci.yml`: `ruff check`, `mypy`, `pytest`, `dbt parse`, `great_expectations checkpoint list` (config validation). Runs on PR.
2. **One demo dashboard** — Superset container in compose OR a checked-in `notebooks/demo.ipynb` hitting Gold marts via Spark. Pick one; do not build both.
3. **README finalization** — pitch, architecture diagram (export `docs/architecture.png`), full tool justification table (mirror of spec §5), schema evolution demo with copy-pasteable queries, "how to reproduce in 3 commands".
4. **Runbook** — `docs/runbook.md`: common failure modes (TLC URL change, GE bootstrap, MinIO disk full), recovery commands.
5. **Definition-of-done sweep** — walk spec §15 line by line; each item linked to evidence (file path / make target / screenshot).

**Done when:** all ten items in spec §15 are checked off and demonstrated by a single `make demo-ingest` + `make demo-backfill` run.

---

## Cross-cutting deliverables (touched in every milestone)

| Concern | Where it lives | Owner milestone |
|---|---|---|
| Audit table writes | every Airflow task | M1, extended M2/M5 |
| `docs/` updates | per-feature PR | every milestone |
| pytest coverage of pure functions | `tests/` mirrors `spark/jobs/` | every milestone |
| dbt model descriptions | `_schema.yml` adjacent to model | M3 onward |
| Pre-commit hooks pass | `.pre-commit-config.yaml` | M0 onward |

---

## Out of scope (do not build, even if tempting)

Mirrors spec §2 non-goals. If a milestone is running long, cut **breadth of months loaded** or **dashboard panels**, never cut: PII tokenization, GE/dbt split, SCD2, audit table, or schema-evolution demo. Those are the rubric.

# Runbook — TLC Yellow Taxi Pipeline

## Quick diagnostics

```bash
docker compose ps                          # all services running?
docker compose logs airflow-scheduler --tail 50
docker compose logs spark-master --tail 30
```

---

## Failure: TLC CloudFront URL returns 404

**Symptom:** `download_to_landing` task fails with HTTP 404 or connection error.

**Cause:** TLC occasionally republishes files at new paths or delays a month's file.

**Fix:**
1. Check the TLC data page for the updated URL.
2. Download the file manually and place it in Landing:
   ```bash
   mc cp yellow_tripdata_YYYY-MM.parquet \
     local/landing/yellow_taxi/year=YYYY/month=MM/data.parquet
   ```
3. Re-trigger the DAG, skipping `download_to_landing`:
   ```bash
   airflow tasks clear yellow_taxi_monthly_ingest \
     -t download_to_landing -y
   airflow dags trigger yellow_taxi_monthly_ingest \
     --conf '{"year": YYYY, "month": MM}'
   ```

---

## Failure: GE Bronze gate fails unexpectedly

**Symptom:** `ge_checkpoint_bronze` routes to quarantine for a month that should pass.

**Cause:** data distribution shift (e.g., TLC revised historical data) or expectation bounds too tight.

**Diagnosis:**
```bash
# View GE Data Docs locally
open great_expectations/uncommitted/data_docs/local_site/index.html
# Or check the latest validation result
ls -lt great_expectations/uncommitted/validations/
```

**Fix:**
- If the data is genuinely anomalous: leave in quarantine, investigate source data.
- If bounds need adjustment: edit `great_expectations/expectations/bronze_yellow_trips_suite.json`,
  widen the failing expectation's `min_value`/`max_value`, re-run.
- Clear the quarantine partition and re-run tokenize + GE:
  ```bash
  airflow tasks clear yellow_taxi_monthly_ingest \
    -t tokenize_pii -d -y
  ```

---

## Failure: MinIO disk full

**Symptom:** Spark write fails with `S3Exception: disk full` or MinIO returns 507.

**Diagnosis:**
```bash
docker compose exec minio mc du local/
docker system df
```

**Fix:**
1. Run the Iceberg maintenance DAG manually to compact + expire old snapshots:
   ```bash
   airflow dags trigger iceberg_maintenance
   ```
2. If still full, clear old Landing files (raw parquet is safe to delete after Bronze write):
   ```bash
   docker compose exec minio mc rm --recursive --force \
     local/landing/yellow_taxi/year=2018/
   ```
3. Expand the Docker volume or add a bind-mount with more space.

---

## Failure: Iceberg REST catalog unreachable

**Symptom:** Spark jobs fail with `Unable to connect to http://iceberg-rest:8181`.

**Fix:**
```bash
docker compose restart iceberg-rest
# Wait ~10s then retry the failed task
airflow tasks run yellow_taxi_monthly_ingest landing_to_bronze <run_id>
```

---

## Failure: Airflow scheduler not processing DAGs

**Symptom:** DAG shows as "paused" or tasks never leave "queued" state.

**Fix:**
```bash
docker compose restart airflow-scheduler
# If the DB is stale:
docker compose exec airflow-webserver airflow db check
```

---

## Failure: dbt run fails with schema not found

**Symptom:** dbt fails with `Namespace 'silver' does not exist`.

**Fix:**
```bash
make init   # re-creates all Iceberg namespaces
```

---

## Failure: PII salt rotation breaks token joins

**Symptom:** After incrementing `PII_SALT_VERSION`, token lookups return NULL because
old tokens no longer match new tokens.

**Fix:** Re-tokenize all affected months after updating the salt:
```bash
for ym in 2023-01 2023-02 ...; do
  year=${ym%-*}; month=${ym#*-}
  spark-submit spark/jobs/tokenize_pii.py --year $year --month $month
done
```
The `fct_tokenization_audit.salt_version` column shows which version each month used.

---

## Failure: Grafana dashboard shows "No data"

**Symptom:** Panels are empty after a pipeline run.

**Causes and fixes:**
1. **Pushgateway not running**: `docker compose ps pushgateway` — restart if down.
2. **Prometheus not scraping**: open `:9090/targets`, check pushgateway target is UP.
3. **No runs yet**: trigger a demo run: `make demo-ingest` then wait ~5 min.
4. **Time range**: check the Grafana time picker is set to "Last 30 days" or wider.

---

## Useful one-liners

```bash
# Count Bronze rows for a month
docker compose exec spark-master /opt/bitnami/spark/bin/spark-sql \
  -e "SELECT COUNT(*) FROM iceberg.bronze.yellow_trips WHERE year(tpep_pickup_datetime)=2023 AND month(tpep_pickup_datetime)=1"

# Show Iceberg snapshot history
docker compose exec spark-master /opt/bitnami/spark/bin/spark-sql \
  -e "SELECT * FROM iceberg.bronze.yellow_trips.history ORDER BY made_current_at DESC LIMIT 10"

# Check audit table
docker compose exec spark-master /opt/bitnami/spark/bin/spark-sql \
  -e "SELECT dag_id, task_id, status, rows_out, duration_sec FROM iceberg.ops.pipeline_audit ORDER BY started_at DESC LIMIT 20"

# Tail Airflow task logs
docker compose exec airflow-scheduler \
  airflow tasks logs yellow_taxi_monthly_ingest landing_to_bronze <run_id>
```

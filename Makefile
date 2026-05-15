.PHONY: up down init lint test demo-ingest demo-backfill dbt-docs ge-docs

up:
	docker compose up -d

down:
	docker compose down

init:
	@echo "Waiting for MinIO..."
	@until docker compose exec minio mc ready local 2>/dev/null; do sleep 2; done
	@echo "MinIO ready."
	docker compose run --rm minio-init
	@echo "Creating Iceberg namespaces..."
	docker compose exec spark-master /opt/bitnami/spark/bin/spark-sql \
		--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.iceberg.type=rest \
		--conf spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181 \
		-e "CREATE NAMESPACE IF NOT EXISTS iceberg.bronze; \
		    CREATE NAMESPACE IF NOT EXISTS iceberg.silver; \
		    CREATE NAMESPACE IF NOT EXISTS iceberg.gold; \
		    CREATE NAMESPACE IF NOT EXISTS iceberg.quarantine; \
		    CREATE NAMESPACE IF NOT EXISTS iceberg.ops; \
		    CREATE NAMESPACE IF NOT EXISTS iceberg.meta;"
	@echo "Init complete."

lint:
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

test:
	.venv/bin/pytest tests/ -v

demo-ingest:
	docker compose exec airflow-webserver airflow dags trigger yellow_taxi_monthly_ingest \
		--conf '{"year": 2023, "month": 1}'
	@echo "DAG triggered. Monitor at http://localhost:8082"

# Load Jan and Jul for 2018, 2019, 2021, 2024 — spans all schema-evolution boundaries.
demo-backfill:
	docker compose exec airflow-webserver airflow dags trigger backfill_yellow_taxi \
		--conf '{"start_month": "2018-01", "end_month": "2018-07"}'
	docker compose exec airflow-webserver airflow dags trigger backfill_yellow_taxi \
		--conf '{"start_month": "2019-01", "end_month": "2019-07"}'
	docker compose exec airflow-webserver airflow dags trigger backfill_yellow_taxi \
		--conf '{"start_month": "2021-01", "end_month": "2021-07"}'
	docker compose exec airflow-webserver airflow dags trigger backfill_yellow_taxi \
		--conf '{"start_month": "2024-01", "end_month": "2024-07"}'
	@echo "Backfill triggered for 2018/2019/2021/2024. Monitor at http://localhost:8082"

dbt-docs:
	cd dbt && .venv/bin/dbt docs generate --profiles-dir . && .venv/bin/dbt docs serve --profiles-dir .
	@echo "dbt docs at http://localhost:8080"

ge-docs:
	@open great_expectations/uncommitted/data_docs/local_site/index.html 2>/dev/null || \
		xdg-open great_expectations/uncommitted/data_docs/local_site/index.html

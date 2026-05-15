.PHONY: up down init lint test demo-ingest demo-backfill

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

demo-backfill:
	@echo "Backfill DAG available in Milestone 4"

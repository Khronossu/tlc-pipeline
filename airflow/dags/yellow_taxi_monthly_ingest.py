"""Monthly ingestion DAG: Landing → Bronze → Audit."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_PATH = "/opt/spark/jobs"

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": False,
}


def _on_failure(context: dict) -> None:  # type: ignore[type-arg]
    """Write a FAILED audit row when a task exhausts all retries."""
    import subprocess

    run_id = context["run_id"]
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    now = datetime.now(tz=UTC).isoformat()

    record = {
        "run_id": run_id,
        "dag_id": dag_id,
        "task_id": task_id,
        "layer": "bronze",
        "status": "FAILED",
        "started_at": now,
        "finished_at": now,
        "duration_sec": 0.0,
        "error_message": str(context.get("exception", ""))[:500],
        "triggered_by": "scheduled",
    }
    subprocess.run(
        [
            "spark-submit",
            f"{SPARK_JOBS_PATH}/write_audit.py",
            "--record-json",
            json.dumps(record),
        ],
        check=False,
    )


@dag(
    dag_id="yellow_taxi_monthly_ingest",
    schedule="0 2 5 * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "year": Param(2023, type="integer", description="TLC data year"),
        "month": Param(1, type="integer", description="TLC data month (1-12)"),
        "force": Param(False, type="boolean", description="Overwrite even if already loaded"),
    },
    tags=["ingestion", "yellow_taxi"],
)
def yellow_taxi_monthly_ingest() -> None:
    year = "{{ params.year }}"
    month = "{{ params.month }}"
    run_id = "{{ run_id }}"
    source_url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{year}-{month:>02}.parquet"
    )

    download = SparkSubmitOperator(
        task_id="download_to_landing",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/download_to_landing.py",
        application_args=["--year", year, "--month", month],
        on_failure_callback=_on_failure,
    )

    to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/landing_to_bronze.py",
        application_args=[
            "--year", year,
            "--month", month,
            "--run-id", run_id,
            "--source-url", source_url,
            "--source-sha256",
            "{{ ti.xcom_pull(task_ids='download_to_landing', key='sha256') or '' }}",
        ],
        on_failure_callback=_on_failure,
    )

    @task(on_failure_callback=_on_failure)
    def write_audit_success(**context: object) -> None:
        import subprocess
        from datetime import UTC, datetime

        ti = context["ti"]  # type: ignore[index]
        now_iso = datetime.now(tz=UTC).isoformat()
        started = ti.start_date.isoformat() if ti.start_date else now_iso
        finished = datetime.now(tz=UTC).isoformat()
        started_dt = datetime.fromisoformat(started)
        finished_dt = datetime.fromisoformat(finished)
        duration = (finished_dt - started_dt).total_seconds()

        record = {
            "run_id": context["run_id"],
            "dag_id": context["dag"].dag_id,
            "task_id": "write_audit_success",
            "layer": "bronze",
            "table_name": "bronze.yellow_trips",
            "source_url": (
                str(context["params"].get("year", ""))  # type: ignore[index]
                + "-"
                + str(context["params"].get("month", ""))  # type: ignore[index]
            ),
            "status": "SUCCESS",
            "started_at": started,
            "finished_at": finished,
            "duration_sec": duration,
            "triggered_by": "scheduled",
        }
        subprocess.run(
            [
                "spark-submit",
                f"{SPARK_JOBS_PATH}/write_audit.py",
                "--record-json",
                json.dumps(record),
            ],
            check=True,
        )

    download >> to_bronze >> write_audit_success()


yellow_taxi_monthly_ingest()

"""SparkSubmit tasks for the download → Bronze ingest steps."""

from __future__ import annotations

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from ingest.callbacks import on_failure

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_PATH = "/opt/spark/jobs"


def make_download_task(year: str, month: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id="download_to_landing",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/download_to_landing.py",
        application_args=["--year", year, "--month", month],
        on_failure_callback=on_failure,
    )


def make_landing_to_bronze_task(
    year: str,
    month: str,
    run_id: str,
    source_url: str,
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id="landing_to_bronze",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/landing_to_bronze.py",
        application_args=[
            "--year",
            year,
            "--month",
            month,
            "--run-id",
            run_id,
            "--source-url",
            source_url,
            "--source-sha256",
            "{{ ti.xcom_pull(task_ids='download_to_landing', key='sha256') or '' }}",
        ],
        on_failure_callback=on_failure,
    )

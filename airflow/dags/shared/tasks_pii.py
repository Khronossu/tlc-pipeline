"""SparkSubmit tasks for PII lookup generation and tokenization."""

from __future__ import annotations

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from shared.callbacks import on_failure

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_PATH = "/opt/spark/jobs"


def make_generate_pii_task(year: str, month: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id="generate_pii_lookup",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/generate_pii_lookup.py",
        application_args=["--year", year, "--month", month],
        on_failure_callback=on_failure,
    )


def make_tokenize_task(year: str, month: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id="tokenize_pii",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/tokenize_pii.py",
        application_args=["--year", year, "--month", month],
        on_failure_callback=on_failure,
    )

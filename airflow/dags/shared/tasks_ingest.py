"""BashOperator-based tasks for the download → Bronze ingest steps."""

from __future__ import annotations

from airflow.operators.bash import BashOperator

from shared.callbacks import on_failure

SPARK_JOBS_PATH = "/opt/spark/jobs"
SPARK_CMD = "spark-submit --master local[*]"


def make_download_task(year: str, month: str) -> BashOperator:
    return BashOperator(
        task_id="download_to_landing",
        bash_command=(
            f"{SPARK_CMD} {SPARK_JOBS_PATH}/download_to_landing.py"
            f" --year {year} --month {month}"
        ),
        on_failure_callback=on_failure,
    )


def make_landing_to_bronze_task(
    year: str,
    month: str,
    run_id: str,
    source_url: str,
) -> BashOperator:
    return BashOperator(
        task_id="landing_to_bronze",
        bash_command=(
            f"{SPARK_CMD} {SPARK_JOBS_PATH}/landing_to_bronze.py"
            f" --year {year} --month {month}"
            f" --run-id {run_id}"
            f" --source-url '{source_url}'"
            " --source-sha256 ''"
        ),
        on_failure_callback=on_failure,
    )

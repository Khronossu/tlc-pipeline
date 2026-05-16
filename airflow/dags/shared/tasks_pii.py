"""BashOperator-based tasks for PII lookup generation and tokenization."""

from __future__ import annotations

from airflow.operators.bash import BashOperator

from shared.callbacks import on_failure

SPARK_JOBS_PATH = "/opt/spark/jobs"
SPARK_CMD = "spark-submit --master local[*]"


def make_generate_pii_task(year: str, month: str) -> BashOperator:
    return BashOperator(
        task_id="generate_pii_lookup",
        bash_command=(
            f"{SPARK_CMD} {SPARK_JOBS_PATH}/generate_pii_lookup.py"
            f" --year {year} --month {month}"
        ),
        on_failure_callback=on_failure,
    )


def make_tokenize_task(year: str, month: str) -> BashOperator:
    return BashOperator(
        task_id="tokenize_pii",
        bash_command=(
            f"{SPARK_CMD} {SPARK_JOBS_PATH}/tokenize_pii.py"
            f" --year {year} --month {month}"
        ),
        on_failure_callback=on_failure,
    )

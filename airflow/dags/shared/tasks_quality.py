"""GE Bronze gate task and branch routing logic."""

from __future__ import annotations

from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

from shared.callbacks import on_failure

SPARK_JOBS_PATH = "/opt/spark/jobs"
SPARK_CMD = "spark-submit --master local[*]"


def _branch_on_ge_result(**context: object) -> str:
    ti = context["ti"]  # type: ignore[index]
    ge_exit_code = ti.xcom_pull(task_ids="ge_checkpoint_bronze", key="return_value")
    if str(ge_exit_code) == "0":
        return "write_audit_success"
    return "write_to_quarantine"


def make_ge_gate_task(year: str, month: str) -> BashOperator:
    # Run bronze quality checks via spark-submit; exit code 0 = pass, 1 = fail.
    # BashOperator pushes the last stdout line to XCom — we ensure it's the exit code.
    return BashOperator(
        task_id="ge_checkpoint_bronze",
        bash_command=(
            f"{SPARK_CMD} {SPARK_JOBS_PATH}/run_ge_bronze.py"
            f" --year {year} --month {month}"
            "; echo $?"
        ),
        do_xcom_push=True,
        on_failure_callback=on_failure,
    )


def make_branch_task() -> BranchPythonOperator:
    return BranchPythonOperator(
        task_id="route_ge_result",
        python_callable=_branch_on_ge_result,
    )

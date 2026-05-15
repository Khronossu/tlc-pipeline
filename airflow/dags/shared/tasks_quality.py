"""GE Bronze gate task and branch routing logic."""

from __future__ import annotations

from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

from shared.callbacks import on_failure

GE_PATH = "/opt/airflow/great_expectations"


def _branch_on_ge_result(**context: object) -> str:
    ti = context["ti"]  # type: ignore[index]
    ge_exit_code = ti.xcom_pull(task_ids="ge_checkpoint_bronze", key="return_value")
    if str(ge_exit_code) == "0":
        return "write_audit_success"
    return "write_to_quarantine"


def make_ge_gate_task() -> BashOperator:
    # GE CLI does not accept --year/--month; the checkpoint resolves the batch
    # via the datasource config. The exit code (0 = pass, non-zero = fail) is
    # captured by the trailing `echo $?` so BashOperator never raises on GE failure.
    return BashOperator(
        task_id="ge_checkpoint_bronze",
        bash_command=(
            f"cd {GE_PATH} && "
            "great_expectations checkpoint run bronze_gate; echo $?"
        ),
        do_xcom_push=True,
        on_failure_callback=on_failure,
    )


def make_branch_task() -> BranchPythonOperator:
    return BranchPythonOperator(
        task_id="route_ge_result",
        python_callable=_branch_on_ge_result,
    )

"""BashOperator tasks for dbt Silver and Gold runs."""

from __future__ import annotations

from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"
GE_PATH = "/opt/airflow/great_expectations"


def _dbt_cmd(select: str, cmd: str = "run", extra_vars: str = "") -> str:
    vars_flag = f"--vars '{extra_vars}'" if extra_vars else ""
    return f"cd {DBT_DIR} && dbt {cmd} --select {select} {vars_flag} --profiles-dir ."


def make_dbt_silver_run(year: str, month: str) -> BashOperator:
    return BashOperator(
        task_id="dbt_run_silver",
        bash_command=_dbt_cmd(
            select="silver",
            extra_vars=f"{{year: {year}, month: {month}}}",
        ),
    )


def make_dbt_silver_test(year: str, month: str) -> BashOperator:
    return BashOperator(
        task_id="dbt_test_silver",
        bash_command=_dbt_cmd(
            select="silver",
            cmd="test",
            extra_vars=f"{{year: {year}, month: {month}}}",
        ),
    )


def make_dbt_gold_run(year: str, month: str) -> BashOperator:
    return BashOperator(
        task_id="dbt_run_gold",
        bash_command=_dbt_cmd(
            select="gold serving",
            extra_vars=f"{{year: {year}, month: {month}}}",
        ),
    )


def make_dbt_gold_test(year: str, month: str) -> BashOperator:
    return BashOperator(
        task_id="dbt_test_gold",
        bash_command=_dbt_cmd(
            select="gold",
            cmd="test",
            extra_vars=f"{{year: {year}, month: {month}}}",
        ),
    )


def make_ge_gold_gate() -> BashOperator:
    return BashOperator(
        task_id="ge_checkpoint_gold",
        bash_command=f"cd {GE_PATH} && great_expectations checkpoint run gold_gate; echo $?",
        do_xcom_push=False,
    )

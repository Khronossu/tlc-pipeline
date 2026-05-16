"""BashOperator tasks for dbt Silver and Gold runs."""

from __future__ import annotations

from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/project/dbt"
GE_PATH = "/opt/project/great_expectations"
DBT_BIN = "/home/airflow/.local/bin/dbt"


def _clean_metastore() -> str:
    """Return shell snippet that removes the stale Derby Hive metastore before each dbt run."""
    return f"rm -rf {DBT_DIR}/metastore_db"


def _dbt_cmd(select: str, cmd: str = "run", extra_vars: str = "") -> str:
    vars_flag = f"--vars '{extra_vars}'" if extra_vars else ""
    return f"{_clean_metastore()} && cd {DBT_DIR} && {DBT_BIN} {cmd} --select {select} {vars_flag} --profiles-dir ."


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


def make_dbt_snapshot() -> BashOperator:
    return BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{_clean_metastore()} && cd {DBT_DIR} && {DBT_BIN} snapshot --profiles-dir .",
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


SPARK_JOBS_PATH = "/opt/spark/jobs"
SPARK_CMD = "spark-submit --master local[*]"


def make_dbt_init() -> BashOperator:
    """Ensure Iceberg namespaces exist and seed data is loaded before dbt runs."""
    return BashOperator(
        task_id="dbt_init",
        bash_command=(
            f"{SPARK_CMD} {SPARK_JOBS_PATH}/init_namespaces.py"
            f" && {SPARK_CMD} {SPARK_JOBS_PATH}/seed_taxi_zones.py"
        ),
    )


def make_ge_gold_gate() -> BashOperator:
    return BashOperator(
        task_id="ge_checkpoint_gold",
        bash_command=f"{SPARK_CMD} {SPARK_JOBS_PATH}/run_ge_gold.py",
        do_xcom_push=False,
    )

"""Monthly ingestion DAG: Landing → PII Lookup → Bronze → Tokenize → GE gate → Audit."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_PATH = "/opt/spark/jobs"
GE_PATH = "/opt/airflow/great_expectations"

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


def _branch_on_ge_result(**context: object) -> str:
    """Route to success or quarantine path based on GE checkpoint exit code."""
    ti = context["ti"]  # type: ignore[index]
    ge_exit_code = ti.xcom_pull(task_ids="ge_checkpoint_bronze", key="return_value")
    if ge_exit_code == 0:
        return "write_audit_success"
    return "write_to_quarantine"


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
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{year}-{month:>02}.parquet"
    )

    # ── 1. Download TLC parquet to Landing ──────────────────────────────────
    download = SparkSubmitOperator(
        task_id="download_to_landing",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/download_to_landing.py",
        application_args=["--year", year, "--month", month],
        on_failure_callback=_on_failure,
    )

    # ── 2. Generate deterministic PII lookup (idempotent) ───────────────────
    gen_pii = SparkSubmitOperator(
        task_id="generate_pii_lookup",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/generate_pii_lookup.py",
        application_args=["--year", year, "--month", month],
        on_failure_callback=_on_failure,
    )

    # ── 3. Cast schema + write Bronze (no PII yet) ───────────────────────────
    to_bronze = SparkSubmitOperator(
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
        on_failure_callback=_on_failure,
    )

    # ── 4. Tokenize PII — join lookup + SHA-256, overwrite Bronze ────────────
    tokenize = SparkSubmitOperator(
        task_id="tokenize_pii",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/tokenize_pii.py",
        application_args=["--year", year, "--month", month],
        on_failure_callback=_on_failure,
    )

    # ── 5. GE Bronze gate ────────────────────────────────────────────────────
    ge_gate = BashOperator(
        task_id="ge_checkpoint_bronze",
        bash_command=(
            f"cd {GE_PATH} && "
            "great_expectations checkpoint run bronze_gate "
            f"--year {year} --month {month}; echo $?"
        ),
        do_xcom_push=True,
        on_failure_callback=_on_failure,
    )

    # ── 6. Branch on GE result ───────────────────────────────────────────────
    branch = BranchPythonOperator(
        task_id="route_ge_result",
        python_callable=_branch_on_ge_result,
    )

    # ── 7a. Success path ─────────────────────────────────────────────────────
    @task(task_id="write_audit_success", on_failure_callback=_on_failure)
    def write_audit_success(**context: object) -> None:
        import subprocess
        from datetime import UTC, datetime

        ti = context["ti"]  # type: ignore[index]
        now_iso = datetime.now(tz=UTC).isoformat()
        started = ti.start_date.isoformat() if ti.start_date else now_iso
        finished = datetime.now(tz=UTC).isoformat()
        duration = (datetime.fromisoformat(finished) - datetime.fromisoformat(started)).total_seconds()

        record = {
            "run_id": context["run_id"],
            "dag_id": context["dag"].dag_id,
            "task_id": "write_audit_success",
            "layer": "bronze",
            "table_name": "bronze.yellow_trips",
            "ge_suite_name": "bronze_yellow_trips_suite",
            "ge_pass_rate": 1.0,
            "status": "SUCCESS",
            "started_at": started,
            "finished_at": finished,
            "duration_sec": duration,
            "triggered_by": "scheduled",
        }
        subprocess.run(
            ["spark-submit", f"{SPARK_JOBS_PATH}/write_audit.py", "--record-json", json.dumps(record)],
            check=True,
        )

    # ── 7b. Failure path ─────────────────────────────────────────────────────
    quarantine = SparkSubmitOperator(
        task_id="write_to_quarantine",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/quarantine_writer.py",
        application_args=[
            "--year", year,
            "--month", month,
            "--failure-reason", "ge_bronze_gate_failed",
            "--run-id", run_id,
        ],
        trigger_rule="none_failed_min_one_success",
    )

    @task(task_id="write_audit_quarantined")
    def write_audit_quarantined(**context: object) -> None:
        import subprocess
        from datetime import UTC, datetime

        now_iso = datetime.now(tz=UTC).isoformat()
        record = {
            "run_id": context["run_id"],
            "dag_id": context["dag"].dag_id,
            "task_id": "write_audit_quarantined",
            "layer": "bronze",
            "table_name": "quarantine.yellow_trips",
            "ge_suite_name": "bronze_yellow_trips_suite",
            "ge_pass_rate": 0.0,
            "status": "QUARANTINED",
            "started_at": now_iso,
            "finished_at": now_iso,
            "duration_sec": 0.0,
            "triggered_by": "scheduled",
        }
        subprocess.run(
            ["spark-submit", f"{SPARK_JOBS_PATH}/write_audit.py", "--record-json", json.dumps(record)],
            check=True,
        )

    @task(task_id="alert_slack")
    def alert_slack(**context: object) -> None:
        """Placeholder — wire to real Slack webhook in production."""
        run_id = context["run_id"]
        print(f"[ALERT] GE Bronze gate failed for run {run_id}. Rows quarantined.")

    # ── Task graph ───────────────────────────────────────────────────────────
    download >> gen_pii >> to_bronze >> tokenize >> ge_gate >> branch
    branch >> write_audit_success()
    branch >> quarantine >> write_audit_quarantined() >> alert_slack()


yellow_taxi_monthly_ingest()

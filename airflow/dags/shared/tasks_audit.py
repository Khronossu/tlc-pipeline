"""Audit write tasks (success + quarantine paths) and Slack alert stub."""

from __future__ import annotations

import json

from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
SPARK_JOBS_PATH = "/opt/spark/jobs"


def make_quarantine_task(year: str, month: str, run_id: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id="write_to_quarantine",
        conn_id=SPARK_CONN_ID,
        application=f"{SPARK_JOBS_PATH}/quarantine_writer.py",
        application_args=[
            "--year",
            year,
            "--month",
            month,
            "--failure-reason",
            "ge_bronze_gate_failed",
            "--run-id",
            run_id,
        ],
        # default all_success: only runs when branched here (skipped upstream = skip, not fail)
    )


@task(task_id="write_audit_success")
def write_audit_success(**context: object) -> None:
    import subprocess
    from datetime import UTC, datetime

    ti = context["ti"]  # type: ignore[index]
    now_iso = datetime.now(tz=UTC).isoformat()
    started = ti.start_date.isoformat() if ti.start_date else now_iso
    finished = datetime.now(tz=UTC).isoformat()
    duration = (
        datetime.fromisoformat(finished) - datetime.fromisoformat(started)
    ).total_seconds()

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
        [
            "spark-submit",
            f"{SPARK_JOBS_PATH}/write_audit.py",
            "--record-json",
            json.dumps(record),
        ],
        check=True,
    )


@task(task_id="write_audit_quarantined")
def write_audit_quarantined(**context: object) -> None:
    import subprocess
    from datetime import UTC, datetime

    ti = context["ti"]  # type: ignore[index]
    now_iso = datetime.now(tz=UTC).isoformat()
    started = ti.start_date.isoformat() if ti.start_date else now_iso
    finished = datetime.now(tz=UTC).isoformat()
    duration = (
        datetime.fromisoformat(finished) - datetime.fromisoformat(started)
    ).total_seconds()

    record = {
        "run_id": context["run_id"],
        "dag_id": context["dag"].dag_id,
        "task_id": "write_audit_quarantined",
        "layer": "bronze",
        "table_name": "quarantine.yellow_trips",
        "ge_suite_name": "bronze_yellow_trips_suite",
        "ge_pass_rate": 0.0,
        "status": "QUARANTINED",
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


@task(task_id="alert_slack")
def alert_slack(**context: object) -> None:
    """Placeholder — wire to real Slack webhook in production."""
    run_id = context["run_id"]
    print(f"[ALERT] GE Bronze gate failed for run {run_id}. Rows quarantined.")

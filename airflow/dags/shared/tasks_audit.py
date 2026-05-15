"""Audit write tasks (success + quarantine paths) and email alert."""

from __future__ import annotations

import json

from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_MASTER = "local[*]"
SPARK_JOBS_PATH = "/opt/spark/jobs"


def make_quarantine_task(year: str, month: str, run_id: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id="write_to_quarantine",
        master=SPARK_MASTER,
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
            "--master", "local[*]",
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
            "--master", "local[*]",
            f"{SPARK_JOBS_PATH}/write_audit.py",
            "--record-json",
            json.dumps(record),
        ],
        check=True,
    )


@task(task_id="alert_email")
def alert_email(**context: object) -> None:
    """Send email alert when GE Bronze gate fails and rows are quarantined."""
    import smtplib
    from email.message import EmailMessage

    run_id = context["run_id"]
    dag_id = context["dag"].dag_id

    msg = EmailMessage()
    msg["Subject"] = f"[TLC Pipeline] GE Bronze gate FAILED — {dag_id}"
    msg["From"] = "noreply@tlc-pipeline.local"
    msg["To"] = "purinboonpetch@gmail.com"
    msg.set_content(
        f"GE Bronze quality gate failed.\n\n"
        f"DAG:    {dag_id}\n"
        f"Run ID: {run_id}\n\n"
        f"Affected rows have been quarantined. "
        f"Check the Airflow UI and Grafana for details.\n"
        f"Audit log: iceberg.ops.audit_log"
    )
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as smtp:
            smtp.starttls()
            smtp.sendmail(msg["From"], [msg["To"]], msg.as_string())
    except Exception as exc:  # noqa: BLE001
        # Log but don't fail the DAG — alert is best-effort
        print(f"[ALERT] Email send failed ({exc}). Run {run_id} quarantined.")

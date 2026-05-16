"""Monthly ingestion DAG: Landing → Bronze → PII Tokenize → GE Bronze → Silver → Gold → GE Gold."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from shared.tasks_audit import (
    alert_email,
    make_quarantine_task,
    write_audit_quarantined,
    write_audit_success,
)
from shared.tasks_dbt import (
    make_dbt_gold_run,
    make_dbt_gold_test,
    make_dbt_init,
    make_dbt_silver_run,
    make_dbt_silver_test,
    make_dbt_snapshot,
    make_ge_gold_gate,
)
from shared.tasks_ingest import make_download_task, make_landing_to_bronze_task
from shared.tasks_pii import make_generate_pii_task, make_tokenize_task
from shared.tasks_quality import make_branch_task, make_ge_gate_task

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": False,
}


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

    # ── Bronze ingest ────────────────────────────────────────────────────────
    download = make_download_task(year, month)
    to_bronze = make_landing_to_bronze_task(year, month, run_id, source_url)
    gen_pii = make_generate_pii_task(year, month)
    tokenize = make_tokenize_task(year, month)
    ge_bronze = make_ge_gate_task(year, month)
    branch = make_branch_task()
    quarantine = make_quarantine_task(year, month, run_id)

    # ── Silver / Gold pipeline (success path only) ───────────────────────────
    dbt_init = make_dbt_init()
    dbt_silver_run = make_dbt_silver_run(year, month)
    dbt_silver_test = make_dbt_silver_test(year, month)
    dbt_snapshot = make_dbt_snapshot()
    dbt_gold_run = make_dbt_gold_run(year, month)
    dbt_gold_test = make_dbt_gold_test(year, month)
    ge_gold = make_ge_gold_gate()

    # ── Task graph ───────────────────────────────────────────────────────────
    download >> to_bronze >> gen_pii >> tokenize >> ge_bronze >> branch

    # Success path: audit → init → Silver → snapshot → Gold → GE Gold
    audit_ok = write_audit_success()
    branch >> audit_ok >> dbt_init >> dbt_silver_run >> dbt_silver_test
    dbt_silver_test >> dbt_snapshot >> dbt_gold_run >> dbt_gold_test >> ge_gold

    # Failure path: quarantine → audit → alert
    branch >> quarantine >> write_audit_quarantined() >> alert_email()


yellow_taxi_monthly_ingest()

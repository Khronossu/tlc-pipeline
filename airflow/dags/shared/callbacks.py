"""DAG-level failure callback shared across all tasks."""

from __future__ import annotations

import json
from datetime import UTC, datetime

SPARK_JOBS_PATH = "/opt/spark/jobs"

_LAYER_BY_TASK: dict[str, str] = {
    "download_to_landing": "landing",
    "generate_pii_lookup": "meta",
    "landing_to_bronze": "bronze",
    "tokenize_pii": "bronze",
    "ge_checkpoint_bronze": "bronze",
}


def on_failure(context: dict) -> None:  # type: ignore[type-arg]
    """Write a FAILED audit row when a task exhausts all retries."""
    import subprocess

    task_id = context["task"].task_id
    now = datetime.now(tz=UTC).isoformat()

    record = {
        "run_id": context["run_id"],
        "dag_id": context["dag"].dag_id,
        "task_id": task_id,
        "layer": _LAYER_BY_TASK.get(task_id, "bronze"),
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

"""Tests for write_audit.py pure functions (no live Spark or Pushgateway needed)."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import patch

import pytest

from spark.jobs.write_audit import AuditRecord, push_metrics


def _make_record(**kwargs: object) -> AuditRecord:
    defaults = {
        "run_id": "run-123",
        "dag_id": "yellow_taxi_monthly_ingest",
        "task_id": "landing_to_bronze",
        "layer": "bronze",
        "status": "SUCCESS",
        "started_at": "2023-01-15T02:00:00+00:00",
        "finished_at": "2023-01-15T02:05:00+00:00",
        "duration_sec": 300.0,
    }
    defaults.update(kwargs)
    return AuditRecord(**defaults)  # type: ignore[arg-type]


def _fake_prometheus(captured: dict[str, float]) -> ModuleType:
    """Build a minimal prometheus_client stand-in that records Gauge.set() calls."""

    class _FakeGauge:
        def __init__(self, name: str, *args: object, **kwargs: object) -> None:
            self._name = name

        def labels(self, **kwargs: object) -> _FakeGauge:
            return self

        def set(self, value: float) -> None:
            captured[self._name] = value

    mod = ModuleType("prometheus_client")
    mod.CollectorRegistry = lambda: object()  # type: ignore[attr-defined]
    mod.Gauge = _FakeGauge  # type: ignore[attr-defined]
    mod.push_to_gateway = lambda *a, **kw: None  # type: ignore[attr-defined]
    return mod


# ── AuditRecord construction ─────────────────────────────────────────────────


def test_audit_record_defaults() -> None:
    r = _make_record()
    assert r.rows_in == 0
    assert r.rows_out == 0
    assert r.rows_quarantined == 0
    assert r.ge_pass_rate == 1.0
    assert r.ge_failed_expectations == []
    assert r.triggered_by == "scheduled"
    assert r.schema_version == 1


def test_audit_record_status_values() -> None:
    for status in ("SUCCESS", "FAILED", "QUARANTINED"):
        r = _make_record(status=status)
        assert r.status == status


def test_audit_record_failed_expectations_list() -> None:
    r = _make_record(ge_failed_expectations=["exp_a", "exp_b"])
    assert r.ge_failed_expectations == ["exp_a", "exp_b"]


def test_audit_record_custom_rows() -> None:
    r = _make_record(rows_in=1_000_000, rows_out=999_500, rows_quarantined=500)
    assert r.rows_in == 1_000_000
    assert r.rows_out == 999_500
    assert r.rows_quarantined == 500


# ── push_metrics ─────────────────────────────────────────────────────────────
# push_metrics does lazy `from prometheus_client import ...` inside the function
# body. prometheus_client is not installed in the dev venv, so we inject a
# fake module into sys.modules before each call.


def test_push_metrics_success_status_encodes_as_1() -> None:
    captured: dict[str, float] = {}
    record = _make_record(status="SUCCESS", table_name="iceberg.bronze.yellow_trips")

    with patch.dict(sys.modules, {"prometheus_client": _fake_prometheus(captured)}):
        push_metrics(record)

    assert captured.get("pipeline_run_status") == 1


def test_push_metrics_failed_status_encodes_as_0() -> None:
    captured: dict[str, float] = {}
    record = _make_record(status="FAILED")

    with patch.dict(sys.modules, {"prometheus_client": _fake_prometheus(captured)}):
        push_metrics(record)

    assert captured.get("pipeline_run_status") == 0


def test_push_metrics_quarantined_status_encodes_as_minus_1() -> None:
    captured: dict[str, float] = {}
    record = _make_record(status="QUARANTINED")

    with patch.dict(sys.modules, {"prometheus_client": _fake_prometheus(captured)}):
        push_metrics(record)

    assert captured.get("pipeline_run_status") == -1


def test_push_metrics_never_raises_on_exception() -> None:
    # Inject a broken module — push_metrics must swallow the error silently.
    broken: ModuleType = ModuleType("prometheus_client")
    broken.CollectorRegistry = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))  # type: ignore[attr-defined]

    with patch.dict(sys.modules, {"prometheus_client": broken}):
        push_metrics(_make_record())  # must not raise


def test_push_metrics_duration_forwarded() -> None:
    captured: dict[str, float] = {}
    record = _make_record(duration_sec=123.45)

    with patch.dict(sys.modules, {"prometheus_client": _fake_prometheus(captured)}):
        push_metrics(record)

    assert captured.get("pipeline_run_duration_seconds") == pytest.approx(123.45)

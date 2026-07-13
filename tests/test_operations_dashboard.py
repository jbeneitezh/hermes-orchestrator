from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import (
    Approval,
    AuditEvent,
    Base,
    Run,
    RunEvent,
    Task,
    UsageLedger,
)
from hermes_orchestrator.operations_watchdog import (
    PublicOperationsApiReader,
    _record_failure,
    evaluate_watchdog,
)

OPERATOR = {"X-Actor-Id": "agent:operator"}


class FakeFleetRunner:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    def status(self) -> dict[str, Any]:
        if self.fail:
            raise RuntimeError("offline")
        return {
            "compose_digest": "sha256:f18",
            "services": [
                {"name": "orchestrator-api", "state": "running", "health": "healthy"},
                {"name": "worker-leader", "state": "running", "health": "healthy"},
            ],
        }

    def plan(self) -> dict[str, Any]:
        return {}

    def apply(self, services: list[str]) -> dict[str, Any]:
        return {"services": services}


@contextmanager
def operations_context(tmp_path: Path, *, runner: FakeFleetRunner | None = None):
    database_path = tmp_path / f"operations-{uuid.uuid4()}.db"
    state_path = tmp_path / "watchdog-state.json"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        operations_watchdog_state_path=str(state_path),
        operations_stale_after_seconds=300,
    )
    app = create_app(settings, fleet_runner=runner or FakeFleetRunner())
    Base.metadata.create_all(app.state.engine)
    factory: sessionmaker[Session] = app.state.session_factory
    with TestClient(app) as client:
        yield client, factory, state_path


def seed_operation(
    factory: sessionmaker[Session],
    *,
    task_status: str = "running",
    task_updated_at: datetime | None = None,
    approval_status: str = "pending",
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    task_id = uuid.uuid4()
    operation_id = uuid.uuid4()
    run_id = uuid.uuid4()
    now = datetime.now(UTC)
    with factory() as session:
        task = Task(
            id=task_id,
            operation_id=operation_id,
            requester_actor_id="agent:leader",
            assignee_actor_id="agent:developer",
            reviewer_actor_id="agent:validator",
            status=task_status,
            idempotency_key=f"task-{task_id}",
            request_hash="a" * 64,
            objective="Validar señal M5 reproducible",
            acceptance_criteria=["Evidencia trazable"],
            updated_at=task_updated_at or now,
        )
        run = Run(
            id=run_id,
            task_id=task_id,
            operation_id=operation_id,
            attempt_number=1,
            worker_actor_id="agent:developer",
            requested_profile_id="terra-medium",
            effective_profile_id="terra-medium",
            dispatch_idempotency_key=f"run-{run_id}",
            dispatch_hash="b" * 64,
            status=task_status,
            timeout_at=now + timedelta(hours=1),
            started_at=now - timedelta(minutes=5),
        )
        approval = Approval(
            run_id=run_id,
            action="review_delivery",
            status=approval_status,
            requested_by_actor_id="agent:leader",
            expires_at=now + timedelta(hours=1),
        )
        event = RunEvent(
            run_id=run_id,
            sequence=1,
            event_type="run.started",
            payload={"phase": "implementation"},
            created_at=task_updated_at or now,
        )
        usage = UsageLedger(
            run_id=run_id,
            payload_hash="c" * 64,
            operation_id=operation_id,
            task_id=task_id,
            project_id="tradix",
            category="research",
            requesting_agent_id="agent:leader",
            executing_agent_id="agent:developer",
            requested_profile="terra-medium",
            effective_profile="terra-medium",
            model="gpt-5.6-terra",
            provider="openai-codex",
            reasoning_effort="medium",
            input_tokens=120,
            output_tokens=45,
            reasoning_tokens=10,
            cache_read_tokens=5,
            cache_write_tokens=0,
            api_calls=1,
            cost_status="unknown",
            quota_status="available",
            outcome="running",
            retry_number=0,
        )
        session.add_all([task, run, approval, event, usage])
        session.commit()
    return task_id, operation_id, run_id


def test_filters_rollups_fleet_and_dashboard_use_six_public_routes(tmp_path: Path) -> None:
    with operations_context(tmp_path) as (client, factory, _):
        _, operation_id, _ = seed_operation(factory)
        fleet = client.get("/v1/operations/fleet", headers=OPERATOR)
        tasks = client.get(
            f"/v1/operations/tasks?status=running&assignee=agent:developer&operation_id={operation_id}",
            headers=OPERATOR,
        )
        usage = client.get(
            f"/v1/operations/usage?group_by=operation&operation_id={operation_id}",
            headers=OPERATOR,
        )
        dashboard = client.get("/operations")

        assert fleet.status_code == 200
        assert fleet.json()["status"] == "ready"
        assert fleet.json()["service_count"] == 2
        assert tasks.status_code == 200
        assert tasks.json()["count"] == 1
        assert tasks.json()["items"][0]["assignee_actor_id"] == "agent:developer"
        assert usage.json()["entries"] == 1
        assert usage.json()["groups"][0]["tokens"]["input_tokens"] == 120
        assert dashboard.status_code == 200
        assert "Mesa de <em>operaciones</em>" in dashboard.text
        assert dashboard.text.count("/v1/operations/") >= 6


def test_pending_approval_is_visible_and_expired_is_derived(tmp_path: Path) -> None:
    with operations_context(tmp_path) as (client, factory, _):
        _, operation_id, run_id = seed_operation(factory)
        pending = client.get(
            f"/v1/operations/approvals?status=pending&operation_id={operation_id}",
            headers=OPERATOR,
        )
        assert pending.status_code == 200
        assert pending.json()["pending_count"] == 1
        assert pending.json()["items"][0]["run_id"] == str(run_id)

        with factory() as session:
            approval = session.query(Approval).filter_by(run_id=run_id).one()
            approval.expires_at = datetime.now(UTC) - timedelta(seconds=1)
            session.commit()
        expired = client.get(
            "/v1/operations/approvals?status=expired",
            headers=OPERATOR,
        )
        assert expired.json()["count"] == 1
        assert expired.json()["items"][0]["status"] == "expired"


def test_timeline_reconnect_returns_only_new_events(tmp_path: Path) -> None:
    with operations_context(tmp_path) as (client, factory, _):
        _, operation_id, run_id = seed_operation(factory)
        first = client.get(f"/v1/operations/timeline?operation_id={operation_id}", headers=OPERATOR)
        assert first.status_code == 200
        assert first.json()["count"] == 1
        cursor = first.json()["next_cursor"]
        with factory() as session:
            session.add(
                RunEvent(
                    run_id=run_id,
                    sequence=2,
                    event_type="artifact.created",
                    payload={"kind": "report"},
                    created_at=datetime.now(UTC) + timedelta(seconds=1),
                )
            )
            session.commit()
        second = client.get(
            f"/v1/operations/timeline?operation_id={operation_id}&cursor={cursor}",
            headers=OPERATOR,
        )
        assert second.status_code == 200
        assert second.json()["count"] == 1
        assert second.json()["items"][0]["event_type"] == "artifact.created"
        assert second.json()["reconnect"]["duplicates"] == 0
        invalid = client.get("/v1/operations/timeline?cursor=%%%", headers=OPERATOR)
        assert invalid.status_code == 422


def test_active_task_without_recent_activity_is_stale(tmp_path: Path) -> None:
    with operations_context(tmp_path) as (client, factory, _):
        seed_operation(factory, task_updated_at=datetime.now(UTC) - timedelta(hours=2))
        response = client.get("/v1/operations/tasks?active_only=true", headers=OPERATOR)
        assert response.status_code == 200
        assert response.json()["active_count"] == 1
        assert response.json()["stale_count"] == 1
        assert response.json()["items"][0]["stale"] is True


class FakeOperationsReader:
    def __init__(self, *, active: int, events: list[dict[str, Any]]) -> None:
        self.active = active
        self.events = events
        self.paths: list[str] = []

    def get(self, path: str, query: dict[str, str] | None = None) -> dict[str, Any]:
        self.paths.append(path)
        if path.endswith("/tasks"):
            return {"active_count": self.active, "stale_count": int(self.active > 0)}
        return {
            "count": len(self.events),
            "items": self.events,
            "next_cursor": "cursor-next" if self.events else query.get("cursor") if query else None,
        }


def test_active_window_emits_deterministic_summary_only_when_due(tmp_path: Path) -> None:
    state_path = tmp_path / "active.json"
    reader = FakeOperationsReader(active=2, events=[{"event_type": "run.started"}])
    first_at = datetime(2026, 7, 13, 8, 0, tzinfo=UTC)
    first = evaluate_watchdog(
        reader,
        str(state_path),
        now=first_at,
        summary_interval_seconds=9000,
    )
    assert first["summary_emitted"] is True
    assert first["summary_count"] == 1
    assert first["last_summary"]["kind"] == "deterministic_operations_rollup"
    assert first["model_calls"] == 0

    second = evaluate_watchdog(
        reader,
        str(state_path),
        now=first_at + timedelta(hours=1),
        summary_interval_seconds=9000,
    )
    assert second["summary_emitted"] is False
    assert second["summary_count"] == 1


def test_idle_window_has_zero_model_calls_and_quota_exposes_counter(tmp_path: Path) -> None:
    state_path = tmp_path / "idle.json"
    reader = FakeOperationsReader(active=0, events=[])
    state = evaluate_watchdog(
        reader,
        str(state_path),
        now=datetime.now(UTC),
        summary_interval_seconds=9000,
    )
    assert state["status"] == "idle"
    assert state["summary_emitted"] is False
    assert state["summary_count"] == 0
    assert state["idle_checks"] == 1
    assert state["model_calls"] == 0
    assert reader.paths == ["/v1/operations/tasks", "/v1/operations/timeline"]

    with operations_context(tmp_path) as (client, factory, configured_state):
        configured_state.write_text(json.dumps(state), encoding="utf-8")
        with factory() as session:
            session.add(
                AuditEvent(
                    actor_id="system:test",
                    event_type="watchdog.checked",
                    aggregate_type="operations",
                    aggregate_id="idle",
                    payload={"model_calls": 0},
                )
            )
            session.commit()
        quota = client.get("/v1/operations/quota", headers=OPERATOR)
        assert quota.status_code == 200
        assert quota.json()["watchdog"]["model_calls"] == 0
        assert quota.json()["watchdog"]["idle_checks"] == 1
        assert client.get("/v1/operations/quota").status_code == 422


def test_fleet_failure_and_invalid_watchdog_state_are_safe(tmp_path: Path) -> None:
    with operations_context(tmp_path, runner=FakeFleetRunner(fail=True)) as (
        client,
        _,
        state_path,
    ):
        assert client.get("/v1/operations/fleet", headers=OPERATOR).status_code == 503
        state_path.write_text("not-json", encoding="utf-8")
        quota = client.get("/v1/operations/quota", headers=OPERATOR)
        assert quota.json()["watchdog"] == {"status": "invalid", "model_calls": 0}
        assert (
            client.get("/v1/operations/tasks", headers={"X-Actor-Id": "agent:unknown"}).status_code
            == 403
        )


def test_public_watchdog_reader_uses_only_control_plane_api(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"active_count": 0}'

    def fake_urlopen(request, timeout: int):
        captured["url"] = request.full_url
        captured["actor"] = request.headers["X-actor-id"]
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(
        "hermes_orchestrator.operations_watchdog.urllib.request.urlopen",
        fake_urlopen,
    )
    reader = PublicOperationsApiReader("http://control/", "agent:operator")
    result = reader.get("/v1/operations/tasks", {"active_only": "true"})
    assert result == {"active_count": 0}
    assert captured == {
        "url": "http://control/v1/operations/tasks?active_only=true",
        "actor": "agent:operator",
        "timeout": 20,
    }


def test_watchdog_recovers_corrupt_state_and_records_safe_failure(tmp_path: Path) -> None:
    state_path = tmp_path / "corrupt.json"
    state_path.write_text("not-json", encoding="utf-8")
    reader = FakeOperationsReader(active=0, events=[])
    recovered = evaluate_watchdog(reader, str(state_path), now=datetime.now(UTC))
    assert recovered["idle_checks"] == 1
    assert recovered["model_calls"] == 0

    state_path.write_text(
        json.dumps({"last_summary_at": "invalid", "summary_count": 0}),
        encoding="utf-8",
    )
    active = evaluate_watchdog(
        FakeOperationsReader(active=1, events=[{"event_type": "new"}]),
        str(state_path),
        now=datetime.now(UTC),
    )
    assert active["summary_emitted"] is True

    _record_failure(str(state_path), RuntimeError("network unavailable"))
    failed = json.loads(state_path.read_text(encoding="utf-8"))
    assert failed["status"] == "degraded"
    assert failed["last_error"] == "RuntimeError"
    assert failed["model_calls"] == 0

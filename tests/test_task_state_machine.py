from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import Approval, AuditEvent, Base
from hermes_orchestrator.task_services import (
    LifecycleError,
    expire_due_runs,
    get_run,
    get_task,
    resolve_approval,
    transition_run,
)
from tests.auth_helpers import auth_headers, seed_active_auth_agents, token_settings

LEADER = auth_headers("agent:leader")
DEVELOPER = auth_headers("agent:developer")
VALIDATOR = auth_headers("agent:validator")


@pytest.fixture
def lifecycle_context(
    tmp_path: Path,
) -> Iterator[tuple[TestClient, sessionmaker[Session]]]:
    database_path = tmp_path / "lifecycle.db"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        actor_roles={
            "user:owner": "owner",
            "agent:leader": "leader",
            "agent:developer": "developer",
            "agent:researcher": "researcher",
            "agent:validator": "validator",
        },
        **token_settings(
            "user:owner",
            "agent:leader",
            "agent:developer",
            "agent:researcher",
            "agent:validator",
        ),
    )
    app = create_app(settings)
    Base.metadata.create_all(app.state.engine)
    seed_active_auth_agents(app.state.session_factory, settings)
    with TestClient(app) as client:
        yield client, app.state.session_factory


def task_payload(**overrides):
    payload = {
        "objective": "Implementar un cambio verificable",
        "acceptance_criteria": ["La prueba es reproducible"],
        "assignee_actor_id": "agent:developer",
        "reviewer_actor_id": "agent:validator",
        "independent_review": True,
        "priority": 50,
        "dependency_ids": [],
        "budget": {"max_runs": 2},
        "references": ["docs/architecture.md"],
    }
    return payload | overrides


def create_task_api(client: TestClient, key: str, **overrides):
    return client.post(
        "/v1/tasks",
        headers=LEADER | {"Idempotency-Key": key},
        json=task_payload(**overrides),
    )


def dispatch_api(
    client: TestClient,
    task_id: str,
    key: str,
    **overrides,
):
    payload = {
        "worker_actor_id": "agent:developer",
        "requested_profile_id": "terra-medium",
        "timeout_seconds": 900,
        "requires_approval": False,
    }
    return client.post(
        f"/v1/tasks/{task_id}/dispatch",
        headers=LEADER | {"Idempotency-Key": key},
        json=payload | overrides,
    )


def test_happy_transition_preserves_task_and_run(lifecycle_context) -> None:
    client, factory = lifecycle_context
    created = create_task_api(client, "happy-task-01")
    task_id = created.json()["id"]
    dispatched = dispatch_api(
        client,
        task_id,
        "happy-dispatch-01",
        requires_approval=True,
        approval_action="independent_review",
        approval_ttl_seconds=600,
    )
    run_id = dispatched.json()["run"]["id"]

    approval = client.post(
        f"/v1/runs/{run_id}/approval",
        headers=VALIDATOR | {"Idempotency-Key": "happy-approval-01"},
        json={"decision": "approved", "reason": "Revisión independiente correcta"},
    )
    approval_replay = client.post(
        f"/v1/runs/{run_id}/approval",
        headers=VALIDATOR | {"Idempotency-Key": "happy-approval-01"},
        json={"decision": "approved", "reason": "Revisión independiente correcta"},
    )
    with factory() as session:
        transition_run(
            session,
            run_id=uuid.UUID(run_id),
            new_status="running",
            actor_id="system:test",
        )
        transition_run(
            session,
            run_id=uuid.UUID(run_id),
            new_status="completed",
            actor_id="system:test",
            summary="Entrega completada",
        )

    task = client.get(f"/v1/tasks/{task_id}", headers=LEADER)
    run = client.get(f"/v1/runs/{run_id}", headers=DEVELOPER)
    events = client.get(f"/v1/runs/{run_id}/events", headers=DEVELOPER)
    completed_cancel = client.post(
        f"/v1/tasks/{task_id}/cancel",
        headers=LEADER | {"Idempotency-Key": "happy-cancel-invalid"},
        json={"reason": "No se puede cancelar lo completado"},
    )
    missing_task = client.get(f"/v1/tasks/{uuid.uuid4()}", headers=LEADER)
    missing_run = client.get(f"/v1/runs/{uuid.uuid4()}", headers=LEADER)
    assert created.status_code == 201
    assert dispatched.status_code == 202
    assert approval.status_code == 200
    assert approval_replay.status_code == 200
    assert task.json()["status"] == "completed"
    assert len(task.json()["runs"]) == 1
    assert run.json()["status"] == "completed"
    assert events.json() == []
    assert completed_cancel.status_code == 409
    assert missing_task.status_code == 404
    assert missing_run.status_code == 404


def test_dependency_blocks_until_prerequisite_completes(lifecycle_context) -> None:
    client, factory = lifecycle_context
    prerequisite = create_task_api(
        client,
        "dependency-parent-01",
        independent_review=False,
        reviewer_actor_id=None,
    ).json()
    dependent = create_task_api(
        client,
        "dependency-child-01",
        dependency_ids=[prerequisite["id"]],
    ).json()
    missing_dependency = create_task_api(
        client,
        "dependency-missing-01",
        dependency_ids=[str(uuid.uuid4())],
    )

    blocked = dispatch_api(client, dependent["id"], "dependency-blocked-dispatch")
    parent_run = dispatch_api(client, prerequisite["id"], "dependency-parent-dispatch").json()[
        "run"
    ]
    with factory() as session:
        transition_run(
            session,
            run_id=uuid.UUID(parent_run["id"]),
            new_status="running",
            actor_id="system:test",
        )
        transition_run(
            session,
            run_id=uuid.UUID(parent_run["id"]),
            new_status="completed",
            actor_id="system:test",
        )
    accepted = dispatch_api(client, dependent["id"], "dependency-accepted-dispatch")

    assert blocked.status_code == 409
    assert blocked.json()["detail"]["code"] == "dependency_unmet"
    assert accepted.status_code == 202
    assert missing_dependency.status_code == 409
    assert missing_dependency.json()["detail"]["code"] == "dependency_not_found"


def test_failed_attempt_can_retry_without_losing_history(lifecycle_context) -> None:
    client, factory = lifecycle_context
    task_id = create_task_api(client, "retry-task-01").json()["id"]
    first = dispatch_api(client, task_id, "retry-dispatch-01").json()["run"]
    replay = dispatch_api(client, task_id, "retry-dispatch-01")
    collision = dispatch_api(
        client,
        task_id,
        "retry-dispatch-01",
        requested_profile_id="sol-high",
    )
    second_while_active = dispatch_api(client, task_id, "retry-dispatch-active")
    with factory() as session:
        transition_run(
            session,
            run_id=uuid.UUID(first["id"]),
            new_status="running",
            actor_id="system:test",
        )
        transition_run(
            session,
            run_id=uuid.UUID(first["id"]),
            new_status="failed",
            actor_id="system:test",
            error_code="transient_provider_error",
        )
        with pytest.raises(LifecycleError):
            transition_run(
                session,
                run_id=uuid.UUID(first["id"]),
                new_status="completed",
                actor_id="system:test",
            )
    second = dispatch_api(client, task_id, "retry-dispatch-02").json()["run"]
    task = client.get(f"/v1/tasks/{task_id}", headers=LEADER).json()

    assert first["id"] != second["id"]
    assert replay.json()["replayed"] is True
    assert collision.status_code == 409
    assert second_while_active.status_code == 409
    assert second["attempt_number"] == 2
    assert [run["status"] for run in task["runs"]] == ["failed", "dispatching"]


def test_comment_and_cancel_are_durable_and_idempotent(lifecycle_context) -> None:
    client, _ = lifecycle_context
    task_id = create_task_api(client, "cancel-task-01").json()["id"]
    run_id = dispatch_api(client, task_id, "cancel-dispatch-01").json()["run"]["id"]
    comment = client.post(
        f"/v1/tasks/{task_id}/comments",
        headers=DEVELOPER | {"Idempotency-Key": "cancel-comment-01"},
        json={"body": "Bloqueo reproducido antes de cancelar"},
    )
    comment_replay = client.post(
        f"/v1/tasks/{task_id}/comments",
        headers=DEVELOPER | {"Idempotency-Key": "cancel-comment-01"},
        json={"body": "Bloqueo reproducido antes de cancelar"},
    )
    comment_collision = client.post(
        f"/v1/tasks/{task_id}/comments",
        headers=DEVELOPER | {"Idempotency-Key": "cancel-comment-01"},
        json={"body": "Otro comentario"},
    )
    cancel_headers = LEADER | {"Idempotency-Key": "cancel-command-01"}
    cancelled = client.post(
        f"/v1/tasks/{task_id}/cancel",
        headers=cancel_headers,
        json={"reason": "Ya no es necesario"},
    )
    replay = client.post(
        f"/v1/tasks/{task_id}/cancel",
        headers=cancel_headers,
        json={"reason": "Ya no es necesario"},
    )
    different_cancel = client.post(
        f"/v1/tasks/{task_id}/cancel",
        headers=LEADER | {"Idempotency-Key": "cancel-command-different"},
        json={"reason": "Otra cancelación"},
    )
    run = client.get(f"/v1/runs/{run_id}", headers=LEADER)

    assert comment.status_code == 201
    assert comment_replay.headers["Idempotent-Replayed"] == "true"
    assert comment_collision.status_code == 409
    assert cancelled.json()["status"] == "cancelled"
    assert replay.json()["replayed"] is True
    assert run.json()["status"] == "cancelled"
    assert different_cancel.status_code == 409


def test_cancel_rejects_pending_approval_and_replay_repairs_historical_projection(
    lifecycle_context,
) -> None:
    client, factory = lifecycle_context
    task_id = create_task_api(client, "cancel-pending-approval-task").json()["id"]
    run_id = dispatch_api(
        client,
        task_id,
        "cancel-pending-approval-dispatch",
        requires_approval=True,
        approval_ttl_seconds=600,
    ).json()["run"]["id"]
    headers = LEADER | {"Idempotency-Key": "cancel-pending-approval-command"}

    cancelled = client.post(
        f"/v1/tasks/{task_id}/cancel",
        headers=headers,
        json={"reason": "La iniciativa fue sustituida"},
    )
    with factory() as session:
        approval = session.scalar(select(Approval).where(Approval.run_id == uuid.UUID(run_id)))
        assert approval is not None
        approval.status = "pending"
        approval.decided_by_actor_id = None
        approval.decision_reason = None
        approval.decision_idempotency_key = None
        approval.decided_at = None
        session.commit()

    replay = client.post(
        f"/v1/tasks/{task_id}/cancel",
        headers=headers,
        json={"reason": "La iniciativa fue sustituida"},
    )

    with factory() as session:
        approval = session.scalar(select(Approval).where(Approval.run_id == uuid.UUID(run_id)))
        events = list(
            session.scalars(
                select(AuditEvent).where(
                    AuditEvent.aggregate_id == str(approval.id),
                    AuditEvent.event_type == "approval.rejected",
                )
            )
        )

    assert cancelled.status_code == 200
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert approval.status == "rejected"
    assert approval.decided_by_actor_id == "agent:leader"
    assert approval.decision_reason == "Tarea cancelada: La iniciativa fue sustituida"
    assert approval.decision_idempotency_key.startswith("cancel:")
    assert len(events) == 2


def test_watchdog_times_out_active_run(lifecycle_context) -> None:
    client, factory = lifecycle_context
    task_id = create_task_api(
        client,
        "timeout-task-01",
        deadline_at=(datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
    ).json()["id"]
    run_id = dispatch_api(client, task_id, "timeout-dispatch-01", timeout_seconds=1).json()["run"][
        "id"
    ]
    with factory() as session:
        expired = expire_due_runs(session, now=datetime.now(UTC) + timedelta(seconds=2))
        task = get_task(session, uuid.UUID(task_id))
        run = get_run(session, uuid.UUID(run_id))

    assert str(run_id) in {str(value) for value in expired}
    assert run.status == "timed_out"
    assert task.status == "timed_out"


def test_expired_approval_fails_closed(lifecycle_context) -> None:
    client, factory = lifecycle_context
    task_id = create_task_api(client, "expired-approval-task").json()["id"]
    run_id = dispatch_api(
        client,
        task_id,
        "expired-approval-dispatch",
        requires_approval=True,
        approval_ttl_seconds=1,
    ).json()["run"]["id"]

    with factory() as session, pytest.raises(LifecycleError) as captured:
        resolve_approval(
            session,
            run_id=uuid.UUID(run_id),
            actor_id="agent:validator",
            idempotency_key="expired-approval-decision",
            decision="approved",
            reason="Demasiado tarde",
            now=datetime.now(UTC) + timedelta(seconds=2),
        )
    with factory() as session:
        assert captured.value.code == "approval_expired"
        assert get_run(session, uuid.UUID(run_id)).status == "failed"
        assert get_task(session, uuid.UUID(task_id)).status == "failed"


def test_self_review_is_denied_at_definition_and_dispatch(lifecycle_context) -> None:
    client, factory = lifecycle_context
    invalid = create_task_api(
        client,
        "self-review-invalid-task",
        assignee_actor_id="agent:developer",
        reviewer_actor_id="agent:developer",
    )
    valid = create_task_api(
        client,
        "self-review-dispatch-task",
        assignee_actor_id="agent:researcher",
        reviewer_actor_id="agent:developer",
    ).json()
    denied = dispatch_api(client, valid["id"], "self-review-dispatch")
    approval_task = create_task_api(client, "self-review-approval-task").json()
    approval_run = dispatch_api(
        client,
        approval_task["id"],
        "self-review-approval-dispatch",
        requires_approval=True,
    ).json()["run"]
    with factory() as session, pytest.raises(LifecycleError) as self_review:
        resolve_approval(
            session,
            run_id=uuid.UUID(approval_run["id"]),
            actor_id="agent:developer",
            idempotency_key="self-review-approval-decision",
            decision="approved",
            reason="Intento del ejecutor",
        )
    wrong_reviewer = client.post(
        f"/v1/runs/{approval_run['id']}/approval",
        headers=LEADER | {"Idempotency-Key": "wrong-reviewer-decision"},
        json={"decision": "approved", "reason": "No soy el reviewer"},
    )
    rejected = client.post(
        f"/v1/runs/{approval_run['id']}/approval",
        headers=VALIDATOR | {"Idempotency-Key": "validator-rejection"},
        json={"decision": "rejected", "reason": "Falta evidencia"},
    )

    assert invalid.status_code == 422
    assert denied.status_code == 403
    assert denied.json()["detail"]["code"] == "self_review_denied"
    assert self_review.value.code == "self_review_denied"
    assert wrong_reviewer.status_code == 403
    assert rejected.status_code == 200


def test_task_create_replays_and_rejects_key_collision(lifecycle_context) -> None:
    client, _ = lifecycle_context
    first = create_task_api(client, "idempotent-task-01")
    replay = create_task_api(client, "idempotent-task-01")
    collision = create_task_api(client, "idempotent-task-01", objective="Un objetivo diferente")
    forbidden = client.get(f"/v1/tasks/{first.json()['id']}", headers={"X-Actor-Id": "unknown"})

    assert first.json()["id"] == replay.json()["id"]
    assert replay.json()["replayed"] is True
    assert collision.status_code == 409
    assert collision.json()["detail"]["code"] == "idempotency_conflict"
    assert forbidden.status_code == 401

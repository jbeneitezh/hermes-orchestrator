from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import AgentRequestRecord, AuditEvent, Base
from hermes_orchestrator.services import (
    IdempotencyConflictError,
    record_agent_request_application,
)
from tests.auth_helpers import auth_headers, seed_active_auth_agents, token_settings

PAYLOAD = {
    "slug": "data-steward-on-demand",
    "role": "data_steward",
    "description": "Custodia datasets y trazabilidad",
    "policy_set": {"name": "data-minimal"},
    "capabilities": ["dataset_read"],
    "secret_refs": ["secret://codex/broker"],
}


@pytest.fixture
def lifecycle_context(
    tmp_path: Path,
) -> Iterator[tuple[TestClient, sessionmaker[Session]]]:
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{(tmp_path / 'lifecycle.db').as_posix()}",
        actor_roles={
            "user:owner": "owner",
            "agent:leader": "leader",
            "agent:operator": "operator",
        },
        **token_settings("user:owner", "agent:leader", "agent:operator"),
    )
    app = create_app(settings)
    Base.metadata.create_all(app.state.engine)
    seed_active_auth_agents(app.state.session_factory, settings)
    with TestClient(app) as client:
        yield client, app.state.session_factory


def create_request(client: TestClient, *, actor_id: str = "agent:leader", key: str) -> str:
    response = client.post(
        "/v1/agents/requests",
        headers=auth_headers(actor_id, **{"Idempotency-Key": key}),
        json=PAYLOAD,
    )
    assert response.status_code == 202
    return response.json()["id"]


def decide(
    client: TestClient,
    request_id: str,
    *,
    decision: str,
    key: str,
    actor_id: str = "agent:operator",
    reason: str = "Decisión revisada de forma independiente",
):
    return client.post(
        f"/v1/agents/requests/{request_id}/decide",
        headers=auth_headers(actor_id, **{"Idempotency-Key": key}),
        json={"decision": decision, "reason": reason},
    )


def test_pending_expone_detalle_sin_renderizar_compose(lifecycle_context) -> None:
    client, _ = lifecycle_context
    request_id = create_request(client, key="pending-request-001")

    detail = client.get(f"/v1/agents/requests/{request_id}", headers=auth_headers("agent:leader"))
    missing = client.get(
        f"/v1/agents/requests/{uuid.uuid4()}", headers=auth_headers("agent:leader")
    )

    assert detail.status_code == 200
    assert detail.json()["status"] == "pending"
    assert detail.json()["payload"] == PAYLOAD
    assert detail.json()["decided_at"] is None
    assert missing.status_code == 404


def test_approve_exige_y_registra_decisor_independiente(lifecycle_context) -> None:
    client, factory = lifecycle_context
    request_id = create_request(client, key="approve-request-001")

    response = decide(client, request_id, decision="approve", key="approve-decision-001")

    assert response.status_code == 200
    assert response.json()["status"] == "approved"
    assert response.json()["decided_by_actor_id"] == "agent:operator"
    with factory() as session:
        events = list(
            session.scalars(select(AuditEvent).where(AuditEvent.aggregate_id == request_id))
        )
        assert [event.event_type for event in events] == [
            "agent.requested",
            "agent.request_approved",
        ]


def test_reject_cierra_la_solicitud_con_motivo(lifecycle_context) -> None:
    client, _ = lifecycle_context
    request_id = create_request(client, key="reject-request-001")

    response = decide(
        client,
        request_id,
        decision="reject",
        key="reject-decision-001",
        reason="La capacidad solicitada excede el mínimo necesario",
    )

    assert response.status_code == 200
    assert response.json()["status"] == "rejected"
    assert response.json()["decision_reason"].startswith("La capacidad")


def test_self_escalation_deny_incluso_para_owner(lifecycle_context) -> None:
    client, _ = lifecycle_context
    request_id = create_request(client, actor_id="user:owner", key="self-escalation-request-001")

    response = decide(
        client,
        request_id,
        decision="approve",
        key="self-escalation-decision-001",
        actor_id="user:owner",
    )

    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "self_escalation_denied"


def test_replay_de_decision_es_idempotente_y_detecta_conflicto(lifecycle_context) -> None:
    client, _ = lifecycle_context
    request_id = create_request(client, key="replay-request-001")
    headers_key = "replay-decision-001"

    first = decide(client, request_id, decision="approve", key=headers_key)
    replay = decide(client, request_id, decision="approve", key=headers_key)
    conflict = decide(
        client,
        request_id,
        decision="approve",
        key=headers_key,
        reason="Contenido diferente para la misma clave",
    )

    assert first.status_code == 200
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "idempotency_conflict"


def test_invalid_transition_no_reabre_una_decision_terminal(lifecycle_context) -> None:
    client, _ = lifecycle_context
    request_id = create_request(client, key="invalid-transition-request-001")
    rejected = decide(client, request_id, decision="reject", key="invalid-transition-reject")
    reopened = decide(client, request_id, decision="approve", key="invalid-transition-approve")
    premature_retire = client.post(
        f"/v1/agents/requests/{request_id}/retire",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": "invalid-transition-retire"}),
        json={"reason": "No se puede retirar algo no aplicado"},
    )

    assert rejected.status_code == 200
    assert reopened.status_code == 409
    assert reopened.json()["detail"]["code"] == "invalid_transition"
    assert premature_retire.status_code == 409


def test_apply_failure_es_auditable_y_reintentable(lifecycle_context) -> None:
    client, factory = lifecycle_context
    request_id = create_request(client, key="apply-failure-request-001")
    assert (
        decide(client, request_id, decision="approve", key="apply-failure-decision-001").status_code
        == 200
    )

    with factory() as session:
        failed = record_agent_request_application(
            session,
            request_id=uuid.UUID(request_id),
            actor_id="system:agent-provisioner",
            idempotency_key="apply-failure-attempt-001",
            outcome="failed",
            error_code="compose_template_unavailable",
            error_detail="No existe aún un renderer habilitado",
        )
        replay = record_agent_request_application(
            session,
            request_id=uuid.UUID(request_id),
            actor_id="system:agent-provisioner",
            idempotency_key="apply-failure-attempt-001",
            outcome="failed",
            error_code="compose_template_unavailable",
            error_detail="No existe aún un renderer habilitado",
        )
        assert failed.request.status == "apply_failed"
        assert replay.replayed is True
        with pytest.raises(IdempotencyConflictError):
            record_agent_request_application(
                session,
                request_id=uuid.UUID(request_id),
                actor_id="system:other-provisioner",
                idempotency_key="apply-failure-attempt-001",
                outcome="failed",
                error_code="different",
            )

    detail = client.get(f"/v1/agents/requests/{request_id}", headers=auth_headers("agent:operator"))
    assert detail.json()["application_error_code"] == "compose_template_unavailable"
    assert detail.json()["applied_at"] is None


def test_retire_es_logico_idempotente_y_preserva_auditoria(lifecycle_context) -> None:
    client, factory = lifecycle_context
    request_id = create_request(client, key="retire-request-001")
    assert decide(client, request_id, decision="approve", key="retire-decision-001").is_success
    with factory() as session:
        applied = record_agent_request_application(
            session,
            request_id=uuid.UUID(request_id),
            actor_id="system:agent-provisioner",
            idempotency_key="retire-application-001",
            outcome="applied",
        )
        assert applied.request.status == "applied"

    headers = auth_headers("agent:operator", **{"Idempotency-Key": "retire-command-001"})
    first = client.post(
        f"/v1/agents/requests/{request_id}/retire",
        headers=headers,
        json={"reason": "Capacidad temporal ya no necesaria"},
    )
    replay = client.post(
        f"/v1/agents/requests/{request_id}/retire",
        headers=headers,
        json={"reason": "Capacidad temporal ya no necesaria"},
    )

    assert first.status_code == 200
    assert first.json()["status"] == "retired"
    assert replay.json()["replayed"] is True
    with factory() as session:
        assert session.get(AgentRequestRecord, uuid.UUID(request_id)) is not None
        event_types = list(
            session.scalars(
                select(AuditEvent.event_type).where(AuditEvent.aggregate_id == request_id)
            )
        )
        assert event_types[-1] == "agent.request_retired"

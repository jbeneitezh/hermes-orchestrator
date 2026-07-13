from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from starlette.requests import Request

from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.mcp_server import (
    McpDomainError,
    McpIdentity,
    _allowed_tools,
    _execute_tool,
    _identity_from_request,
)
from hermes_orchestrator.models import (
    Agent,
    AgentRequestRecord,
    AuditEvent,
    Base,
    CommunicationEdge,
    Run,
    Task,
    TaskComment,
)
from hermes_orchestrator.task_services import (
    LifecycleError,
    handoff_task,
    resolve_approval,
    transition_run,
)
from hermes_orchestrator.usage_services import ingest_run_usage
from tests.auth_helpers import auth_headers, seed_active_auth_agents, token_settings

LEADER = "agent:leader"
DEVELOPER = "agent:developer"
RESEARCHER = "agent:researcher"


@pytest.fixture
def mcp_context(
    tmp_path: Path,
) -> Iterator[tuple[TestClient, sessionmaker[Session], dict[str, Agent], Settings]]:
    database_path = tmp_path / "mcp.db"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        **token_settings(LEADER, DEVELOPER, RESEARCHER),
    )
    app = create_app(settings)
    Base.metadata.create_all(app.state.engine)
    agents = {
        "leader": Agent(
            slug="leader", role="leader", description="Coordina", owner_actor_id="user:owner"
        ),
        "developer": Agent(
            slug="developer",
            role="developer",
            description="Implementa",
            owner_actor_id="user:owner",
        ),
        "researcher": Agent(
            slug="researcher",
            role="researcher",
            description="Investiga",
            owner_actor_id="user:owner",
        ),
        "hidden": Agent(
            slug="hidden",
            role="validator",
            description="No visible",
            owner_actor_id="user:owner",
        ),
    }
    with app.state.session_factory() as session:
        session.add_all(agents.values())
        session.flush()
        session.add_all(
            [
                CommunicationEdge(
                    source_agent_id=agents["leader"].id,
                    target_agent_id=agents["developer"].id,
                    task_classes=["visibility", "task"],
                    scopes=["read", "dispatch"],
                    approved_by_actor_id="user:owner",
                ),
                CommunicationEdge(
                    source_agent_id=agents["leader"].id,
                    target_agent_id=agents["researcher"].id,
                    task_classes=["visibility"],
                    scopes=["read"],
                    approved_by_actor_id="user:owner",
                ),
            ]
        )
        session.commit()
        for agent in agents.values():
            session.expunge(agent)
    seed_active_auth_agents(app.state.session_factory, settings)
    with TestClient(app) as client:
        yield client, app.state.session_factory, agents, settings


def identity(agent: Agent) -> McpIdentity:
    return McpIdentity(agent=agent, actor_id=f"agent:{agent.slug}", role=agent.role)


def task_arguments(key: str) -> dict[str, Any]:
    return {
        "idempotency_key": key,
        "objective": "Implementar el contrato MCP",
        "acceptance_criteria": ["REST y MCP comparten estado"],
        "assignee_actor_id": DEVELOPER,
        "reviewer_actor_id": None,
        "independent_review": False,
        "priority": 50,
        "dependency_ids": [],
        "budget": {"max_runs": 1},
        "references": [],
    }


def mcp_post(client: TestClient, agent: Agent, body: dict[str, Any]):
    return client.post(
        "/mcp/",
        headers={
            **auth_headers(f"agent:{agent.slug}"),
            "X-Agent-Id": str(agent.id),
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
        },
        json=body,
    )


def mcp_call(
    client: TestClient, agent: Agent, name: str, arguments: dict[str, Any], request_id: int
) -> dict[str, Any]:
    response = mcp_post(
        client,
        agent,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
    )
    assert response.status_code == 200
    return response.json()["result"]


def start_developer_run(
    session: Session,
    agents: dict[str, Agent],
    settings: Settings,
    key: str,
    *,
    independent_review: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    arguments = task_arguments(f"{key}-task")
    if independent_review:
        arguments |= {
            "reviewer_actor_id": RESEARCHER,
            "independent_review": True,
        }
    created = _execute_tool(session, identity(agents["leader"]), "task_create", arguments, settings)
    dispatched = _execute_tool(
        session,
        identity(agents["leader"]),
        "task_dispatch",
        {
            "task_id": created["task"]["id"],
            "worker_agent_id": str(agents["developer"].id),
            "requested_profile_id": "spark-low",
            "idempotency_key": f"{key}-dispatch",
        },
        settings,
    )
    transition_run(
        session,
        run_id=uuid.UUID(dispatched["run"]["id"]),
        new_status="running",
        actor_id="system:test",
        settings=settings,
    )
    return created, dispatched


def test_streamable_http_introspection_filters_tools(mcp_context) -> None:
    client, _, agents, _ = mcp_context
    initialized = mcp_post(
        client,
        agents["developer"],
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "f9-test", "version": "1"},
            },
        },
    )
    listed = mcp_post(
        client,
        agents["developer"],
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    )

    assert initialized.status_code == 200
    assert listed.status_code == 200
    assert {tool["name"] for tool in listed.json()["result"]["tools"]} == {
        "task_create",
        "task_get",
        "task_comment",
        "task_block",
        "task_complete",
    }


def test_agent_request_is_idempotent_and_shares_the_rest_domain(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    arguments = {
        "idempotency_key": "mcp-agent-request-01",
        "slug": "data-steward-01",
        "role": "data_steward",
        "description": "Valida calidad del dataset",
        "policy_set": {"name": "data-readonly"},
        "capabilities": ["dataset_read"],
        "secret_refs": ["secret://codex/broker-client"],
    }
    with factory() as session:
        leader = identity(agents["leader"])
        first = _execute_tool(session, leader, "agent_request", arguments, settings)
        replay = _execute_tool(session, leader, "agent_request", arguments, settings)
        requests = list(session.scalars(select(AgentRequestRecord)))
        audits = list(
            session.scalars(select(AuditEvent).where(AuditEvent.event_type == "agent.requested"))
        )

    assert first["request"]["id"] == replay["request"]["id"]
    assert replay["replayed"] is True
    assert len(requests) == 1
    assert len(audits) == 1

    with factory() as session:
        with pytest.raises(McpDomainError) as conflict:
            _execute_tool(
                session,
                identity(agents["leader"]),
                "agent_request",
                arguments | {"description": "Otra capacidad"},
                settings,
            )
        with pytest.raises(McpDomainError) as unknown:
            _execute_tool(session, identity(agents["leader"]), "unknown", {}, settings)
        with pytest.raises(McpDomainError, match="X-Agent-Id no es válido"):
            _identity_from_request(session, None, settings)

    assert conflict.value.code == "idempotency_conflict"
    assert unknown.value.code == "permission_denied"


def test_task_block_records_typed_handoff_and_survives_worker_finalization(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    with factory() as session:
        created, dispatched = start_developer_run(session, agents, settings, "mcp-block-0001")
        blocked = _execute_tool(
            session,
            identity(agents["developer"]),
            "task_block",
            {
                "task_id": created["task"]["id"],
                "idempotency_key": "mcp-block-handoff-0001",
                "block_type": "clarification",
                "summary": "El criterio de cálculo es ambiguo",
                "needed_action": "Confirmar si el spread se aplica antes del redondeo",
                "references": ["docs/architecture-proposal.md"],
            },
            settings,
        )
        replay = _execute_tool(
            session,
            identity(agents["developer"]),
            "task_block",
            {
                "task_id": created["task"]["id"],
                "idempotency_key": "mcp-block-handoff-0001",
                "block_type": "clarification",
                "summary": "El criterio de cálculo es ambiguo",
                "needed_action": "Confirmar si el spread se aplica antes del redondeo",
                "references": ["docs/architecture-proposal.md"],
            },
            settings,
        )
        transition_run(
            session,
            run_id=uuid.UUID(dispatched["run"]["id"]),
            new_status="completed",
            actor_id="system:test",
            summary="Worker finalizado",
            settings=settings,
        )
        persisted = _execute_tool(
            session,
            identity(agents["leader"]),
            "task_get",
            {"task_id": created["task"]["id"]},
            settings,
        )

    assert blocked["task"]["status"] == "blocked"
    assert blocked["handoff"]["block_type"] == "clarification"
    assert blocked["handoff"]["needed_action"].startswith("Confirmar")
    assert replay["replayed"] is True
    assert persisted["task"]["status"] == "blocked"


def test_task_complete_creates_terminal_handoff_with_rest_parity(mcp_context) -> None:
    client, factory, agents, settings = mcp_context
    with factory() as session:
        created, dispatched = start_developer_run(session, agents, settings, "mcp-complete-0001")
        completed = _execute_tool(
            session,
            identity(agents["developer"]),
            "task_complete",
            {
                "task_id": created["task"]["id"],
                "idempotency_key": "mcp-complete-handoff-01",
                "summary": "Contrato MCP implementado y verificado",
                "outputs": ["rama feat/mcp-lifecycle"],
                "evidence": ["pytest tests/test_mcp_parity.py"],
            },
            settings,
        )
        transition_run(
            session,
            run_id=uuid.UUID(dispatched["run"]["id"]),
            new_status="completed",
            actor_id="system:test",
            summary="Worker finalizado",
            settings=settings,
        )

    rest = client.get(f"/v1/tasks/{created['task']['id']}", headers=auth_headers(LEADER)).json()
    assert completed["task"]["status"] == "completed"
    assert completed["approval"] is None
    assert completed["handoff"]["outcome"] == "completed"
    assert rest["status"] == "completed"
    assert rest["comments"][0]["id"] == completed["handoff"]["comment_id"]


def test_independent_completion_cannot_be_approved_by_its_worker(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    with factory() as session:
        created, dispatched = start_developer_run(
            session,
            agents,
            settings,
            "mcp-review-0001",
            independent_review=True,
        )
        completed = _execute_tool(
            session,
            identity(agents["developer"]),
            "task_complete",
            {
                "task_id": created["task"]["id"],
                "idempotency_key": "mcp-review-handoff-0001",
                "summary": "Cambio listo para revisión independiente",
                "outputs": ["PR #123"],
                "evidence": ["suite verde"],
            },
            settings,
        )
        transition_run(
            session,
            run_id=uuid.UUID(dispatched["run"]["id"]),
            new_status="completed",
            actor_id="system:test",
            settings=settings,
        )
        with pytest.raises(LifecycleError) as captured:
            resolve_approval(
                session,
                run_id=uuid.UUID(dispatched["run"]["id"]),
                actor_id=DEVELOPER,
                idempotency_key="mcp-self-approval-denied",
                decision="approved",
                reason="Intento de autoaprobación",
                settings=settings,
            )

    assert completed["task"]["status"] == "awaiting_approval"
    assert completed["approval"]["status"] == "pending"
    assert captured.value.code == "self_review_denied"


@pytest.mark.parametrize(
    ("decision", "expected_status"), [("approved", "completed"), ("rejected", "failed")]
)
def test_independent_completion_is_decided_by_the_assigned_reviewer(
    mcp_context, decision: str, expected_status: str
) -> None:
    _, factory, agents, settings = mcp_context
    suffix = "approve" if decision == "approved" else "reject"
    with factory() as session:
        created, dispatched = start_developer_run(
            session,
            agents,
            settings,
            f"mcp-review-{suffix}",
            independent_review=True,
        )
        _execute_tool(
            session,
            identity(agents["developer"]),
            "task_complete",
            {
                "task_id": created["task"]["id"],
                "idempotency_key": f"mcp-review-{suffix}-handoff",
                "summary": "Entrega pendiente de decisión",
                "outputs": ["PR verificable"],
                "evidence": ["suite focalizada"],
            },
            settings,
        )
        if decision == "approved":
            with pytest.raises(LifecycleError) as early:
                resolve_approval(
                    session,
                    run_id=uuid.UUID(dispatched["run"]["id"]),
                    actor_id=RESEARCHER,
                    idempotency_key="review-too-early-01",
                    decision="approved",
                    reason="El Run sigue activo",
                    settings=settings,
                )
            assert early.value.code == "handoff_not_terminal"
        transition_run(
            session,
            run_id=uuid.UUID(dispatched["run"]["id"]),
            new_status="completed",
            actor_id="system:test",
            settings=settings,
        )
        resolved = resolve_approval(
            session,
            run_id=uuid.UUID(dispatched["run"]["id"]),
            actor_id=RESEARCHER,
            idempotency_key=f"review-{suffix}-decision",
            decision=decision,
            reason="Decisión independiente",
            settings=settings,
        )
        replay = resolve_approval(
            session,
            run_id=uuid.UUID(dispatched["run"]["id"]),
            actor_id=RESEARCHER,
            idempotency_key=f"review-{suffix}-decision",
            decision=decision,
            reason="Decisión independiente",
            settings=settings,
        )
        with pytest.raises(LifecycleError) as collision:
            resolve_approval(
                session,
                run_id=uuid.UUID(dispatched["run"]["id"]),
                actor_id=RESEARCHER,
                idempotency_key=f"review-{suffix}-other-decision",
                decision=decision,
                reason="Decisión independiente",
                settings=settings,
            )
        persisted = _execute_tool(
            session,
            identity(agents["leader"]),
            "task_get",
            {"task_id": created["task"]["id"]},
            settings,
        )

    assert resolved.replayed is False
    assert replay.replayed is True
    assert collision.value.code == "idempotency_conflict"
    assert persisted["task"]["status"] == expected_status


def test_handoff_domain_rejects_invalid_state_actor_payload_and_key_reuse(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    with factory() as session:
        leader = identity(agents["leader"])
        developer = identity(agents["developer"])
        pending = _execute_tool(
            session, leader, "task_create", task_arguments("mcp-pending-handoff-01"), settings
        )
        with pytest.raises(LifecycleError) as inactive:
            handoff_task(
                session,
                task_id=uuid.UUID(pending["task"]["id"]),
                actor_id=DEVELOPER,
                idempotency_key="inactive-handoff-01",
                handoff={"type": "task_handoff", "outcome": "completed"},
            )

        created, dispatched = start_developer_run(
            session, agents, settings, "mcp-handoff-boundaries"
        )
        with pytest.raises(LifecycleError) as no_approval:
            resolve_approval(
                session,
                run_id=uuid.UUID(dispatched["run"]["id"]),
                actor_id=RESEARCHER,
                idempotency_key="missing-approval-decision",
                decision="approved",
                reason="No existe approval",
                settings=settings,
            )
        with pytest.raises(LifecycleError) as wrong_worker:
            handoff_task(
                session,
                task_id=uuid.UUID(created["task"]["id"]),
                actor_id=leader.actor_id,
                idempotency_key="wrong-worker-handoff",
                handoff={"type": "task_handoff", "outcome": "completed"},
            )
        with pytest.raises(LifecycleError) as invalid:
            handoff_task(
                session,
                task_id=uuid.UUID(created["task"]["id"]),
                actor_id=developer.actor_id,
                idempotency_key="invalid-outcome-handoff",
                handoff={"type": "task_handoff", "outcome": "unknown"},
            )
        task_record = session.get(Task, uuid.UUID(created["task"]["id"]))
        assert task_record is not None
        task_record.independent_review = True
        task_record.reviewer_actor_id = DEVELOPER
        session.commit()
        with pytest.raises(LifecycleError) as self_review:
            handoff_task(
                session,
                task_id=task_record.id,
                actor_id=developer.actor_id,
                idempotency_key="self-review-handoff",
                handoff={"type": "task_handoff", "outcome": "completed"},
            )
        session.rollback()
        session.add(
            TaskComment(
                task_id=uuid.UUID(created["task"]["id"]),
                actor_id=developer.actor_id,
                body="no-es-json",
                idempotency_key="malformed-handoff-comment",
            )
        )
        session.commit()
        with pytest.raises(LifecycleError) as malformed:
            handoff_task(
                session,
                task_id=uuid.UUID(created["task"]["id"]),
                actor_id=developer.actor_id,
                idempotency_key="malformed-handoff-comment",
                handoff={"type": "task_handoff", "outcome": "completed"},
            )

        block_arguments = {
            "task_id": created["task"]["id"],
            "idempotency_key": "boundary-block-handoff",
            "block_type": "dependency",
            "summary": "Falta una entrada",
            "needed_action": "Entregar el contrato",
        }
        _execute_tool(session, developer, "task_block", block_arguments, settings)
        with pytest.raises(LifecycleError) as collision:
            handoff_task(
                session,
                task_id=uuid.UUID(created["task"]["id"]),
                actor_id=developer.actor_id,
                idempotency_key="boundary-block-handoff",
                handoff={
                    "type": "task_handoff",
                    "outcome": "blocked",
                    "block_type": "dependency",
                    "summary": "Otro motivo",
                    "needed_action": "Entregar el contrato",
                    "references": [],
                },
            )
        run = session.get(Run, uuid.UUID(dispatched["run"]["id"]))
        assert run is not None
        run.error_details = {}
        session.commit()
        replay = _execute_tool(session, developer, "task_block", block_arguments, settings)

    assert inactive.value.code == "active_run_not_found"
    assert no_approval.value.code == "approval_not_found"
    assert wrong_worker.value.code == "worker_mismatch"
    assert invalid.value.code == "invalid_handoff"
    assert self_review.value.code == "self_review_denied"
    assert malformed.value.code == "idempotency_conflict"
    assert collision.value.code == "idempotency_conflict"
    assert replay["replayed"] is True
    assert replay["handoff"]["comment_id"]


def test_completion_approval_expires_without_rewriting_the_finished_run(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    now = datetime.now(UTC)
    with factory() as session:
        created, dispatched = start_developer_run(
            session,
            agents,
            settings,
            "mcp-review-expiry",
            independent_review=True,
        )
        handoff_task(
            session,
            task_id=uuid.UUID(created["task"]["id"]),
            actor_id=DEVELOPER,
            idempotency_key="mcp-review-expiry-handoff",
            handoff={
                "type": "task_handoff",
                "outcome": "completed",
                "summary": "Entrega sujeta a caducidad",
                "outputs": ["PR verificable"],
                "evidence": [],
            },
            approval_ttl_seconds=1,
            now=now,
        )
        transition_run(
            session,
            run_id=uuid.UUID(dispatched["run"]["id"]),
            new_status="completed",
            actor_id="system:test",
            settings=settings,
        )
        with pytest.raises(LifecycleError) as expired:
            resolve_approval(
                session,
                run_id=uuid.UUID(dispatched["run"]["id"]),
                actor_id=RESEARCHER,
                idempotency_key="mcp-review-expiry-decision",
                decision="approved",
                reason="Decisión tardía",
                now=now + timedelta(seconds=2),
                settings=settings,
            )
        run = session.get(Run, uuid.UUID(dispatched["run"]["id"]))
        assert run is not None
        task = _execute_tool(
            session,
            identity(agents["leader"]),
            "task_get",
            {"task_id": created["task"]["id"]},
            settings,
        )

    assert expired.value.code == "approval_expired"
    assert run.status == "completed"
    assert task["task"]["status"] == "failed"


def test_allowed_dispatch_and_rest_mcp_parity(mcp_context) -> None:
    client, _, agents, _ = mcp_context
    created = mcp_call(
        client, agents["leader"], "task_create", task_arguments("mcp-task-0001"), 10
    )["structuredContent"]
    replayed = mcp_call(
        client, agents["leader"], "task_create", task_arguments("mcp-task-0001"), 11
    )["structuredContent"]
    dispatched = mcp_call(
        client,
        agents["leader"],
        "task_dispatch",
        {
            "task_id": created["task"]["id"],
            "worker_agent_id": str(agents["developer"].id),
            "requested_profile_id": "spark-low",
            "idempotency_key": "mcp-dispatch-0001",
        },
        12,
    )["structuredContent"]

    rest = client.get(f"/v1/tasks/{created['task']['id']}", headers=auth_headers(LEADER)).json()
    assert replayed["replayed"] is True
    assert dispatched["run"]["status"] == "dispatching"
    assert rest["id"] == created["task"]["id"]
    assert rest["runs"][0]["id"] == dispatched["run"]["id"]


def test_denied_edge_hidden_agent_and_role_tools(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    with factory() as session:
        leader = identity(agents["leader"])
        visible = _execute_tool(session, leader, "agents_list", {}, settings)
        created = _execute_tool(
            session, leader, "task_create", task_arguments("mcp-task-0002"), settings
        )
        with pytest.raises(McpDomainError, match="Edge de dispatch"):
            _execute_tool(
                session,
                leader,
                "task_dispatch",
                {
                    "task_id": created["task"]["id"],
                    "worker_agent_id": str(agents["researcher"].id),
                    "requested_profile_id": "spark-low",
                    "idempotency_key": "mcp-dispatch-0002",
                },
                settings,
            )
        developer_tools = {
            tool.name for tool in _allowed_tools(identity(agents["developer"]), settings)
        }

    assert {agent["slug"] for agent in visible["agents"]} == {
        "leader",
        "developer",
        "researcher",
    }
    assert "hidden" not in {agent["slug"] for agent in visible["agents"]}
    assert "task_dispatch" not in developer_tools


def test_invalid_input_participant_visibility_comment_usage_and_redaction(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    with factory() as session:
        leader = identity(agents["leader"])
        created = _execute_tool(
            session, leader, "task_create", task_arguments("mcp-task-0003"), settings
        )
        task_id = created["task"]["id"]
        with pytest.raises(ValueError):
            _execute_tool(session, leader, "task_get", {"task_id": "invalid"}, settings)
        participant_view = _execute_tool(
            session, identity(agents["developer"]), "task_get", {"task_id": task_id}, settings
        )
        comment = _execute_tool(
            session,
            identity(agents["developer"]),
            "task_comment",
            {
                "task_id": task_id,
                "idempotency_key": "mcp-comment-0003",
                "body": "Listo para revisar",
            },
            settings,
        )
        usage = _execute_tool(session, leader, "usage_summary", {}, settings)

    serialized = str({"view": participant_view, "comment": comment, "usage": usage})
    assert participant_view["task"]["id"] == task_id
    assert comment["comment"]["body"] == "Listo para revisar"
    assert usage["cost_status"] == "ledger"
    assert "worker_run_id" not in serialized
    assert "internal_endpoint" not in serialized
    assert "token" not in serialized.lower()


def test_authentication_and_denied_tool_errors_are_redacted(mcp_context) -> None:
    client, _, agents, _ = mcp_context
    unknown = Agent(
        id=uuid.uuid4(),
        slug="unknown",
        role="developer",
        description="No registrado",
        owner_actor_id="user:owner",
    )
    listed = mcp_post(
        client,
        unknown,
        {"jsonrpc": "2.0", "id": 20, "method": "tools/list", "params": {}},
    )
    denied = mcp_call(
        client,
        agents["developer"],
        "task_dispatch",
        {
            "task_id": str(uuid.uuid4()),
            "worker_agent_id": str(agents["developer"].id),
            "requested_profile_id": "spark-low",
            "idempotency_key": "denied-dispatch-20",
        },
        21,
    )

    assert listed.status_code == 401
    assert listed.json()["detail"]["code"] == "token_unknown"
    assert denied["isError"] is True
    assert denied["structuredContent"]["error"]["code"] == "permission_denied"
    assert "token" not in str(denied).lower()


def test_inactive_untrusted_identity_and_missing_target_are_denied(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/mcp/",
            "headers": [(b"x-agent-id", str(agents["hidden"].id).encode())],
        }
    )
    with factory() as session:
        hidden = session.get(Agent, agents["hidden"].id)
        assert hidden is not None
        hidden.desired_state = "disabled"
        session.commit()
        with pytest.raises(McpDomainError, match="no activa"):
            _identity_from_request(session, request, settings)
        hidden.desired_state = "active"
        hidden.role = "developer"
        session.commit()
        with pytest.raises(McpDomainError, match="no confiable"):
            _identity_from_request(session, request, settings)

        leader = identity(agents["leader"])
        created = _execute_tool(
            session, leader, "task_create", task_arguments("mcp-task-missing-target"), settings
        )
        with pytest.raises(McpDomainError, match="ejecutor no encontrado"):
            _execute_tool(
                session,
                leader,
                "task_dispatch",
                {
                    "task_id": created["task"]["id"],
                    "worker_agent_id": str(uuid.uuid4()),
                    "requested_profile_id": "spark-low",
                    "idempotency_key": "missing-target-20",
                },
                settings,
            )


def test_unrelated_task_is_hidden_and_invalid_create_is_normalized(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    with factory() as session:
        leader = identity(agents["leader"])
        created = _execute_tool(
            session, leader, "task_create", task_arguments("mcp-task-hidden-0001"), settings
        )
        researcher = identity(agents["researcher"])
        with pytest.raises(McpDomainError, match="Tarea no encontrada"):
            _execute_tool(
                session, researcher, "task_get", {"task_id": created["task"]["id"]}, settings
            )
        with pytest.raises(McpDomainError, match="Tarea no encontrada"):
            _execute_tool(
                session,
                researcher,
                "task_comment",
                {
                    "task_id": created["task"]["id"],
                    "idempotency_key": "hidden-comment-01",
                    "body": "No debería verlo",
                },
                settings,
            )
        with pytest.raises(McpDomainError, match="Tarea no encontrada"):
            _execute_tool(
                session,
                researcher,
                "task_block",
                {
                    "task_id": created["task"]["id"],
                    "idempotency_key": "hidden-block-0001",
                    "block_type": "access",
                    "summary": "No debería poder operar esta tarea",
                    "needed_action": "No aplica",
                },
                settings,
            )
        with pytest.raises(McpDomainError, match="Argumentos no válidos"):
            _execute_tool(
                session, leader, "task_create", {"idempotency_key": "invalid-01"}, settings
            )
        with pytest.raises(McpDomainError, match="Argumentos no válidos"):
            _execute_tool(
                session,
                identity(agents["developer"]),
                "task_block",
                {
                    "task_id": created["task"]["id"],
                    "idempotency_key": "invalid-block-01",
                    "block_type": "inventado",
                    "summary": "Tipo fuera de contrato",
                    "needed_action": "Corregir",
                },
                settings,
            )


def test_usage_summary_aggregates_and_filters_operation(mcp_context) -> None:
    _, factory, agents, settings = mcp_context
    with factory() as session:
        leader = identity(agents["leader"])
        created = _execute_tool(
            session, leader, "task_create", task_arguments("mcp-usage-task-0001"), settings
        )
        dispatched = _execute_tool(
            session,
            leader,
            "task_dispatch",
            {
                "task_id": created["task"]["id"],
                "worker_agent_id": str(agents["developer"].id),
                "requested_profile_id": "spark-low",
                "idempotency_key": "mcp-usage-dispatch-01",
            },
            settings,
        )
        run = session.scalar(select(Run).where(Run.id == uuid.UUID(dispatched["run"]["id"])))
        assert run is not None
        run.usage_snapshot = {
            "input_tokens": 100,
            "output_tokens": 20,
            "reasoning_tokens": "unknown",
        }
        ingest_run_usage(session, run, settings=settings)
        session.commit()
        usage = _execute_tool(
            session,
            leader,
            "usage_summary",
            {"operation_id": created["task"]["operation_id"]},
            settings,
        )

    assert usage["groups"][0]["key"] == created["task"]["operation_id"]
    assert usage["groups"][0]["tokens"]["input_tokens"] == 100
    assert usage["groups"][0]["tokens"]["output_tokens"] == 20
    assert usage["groups"][0]["tokens"]["reasoning_tokens"] is None
    assert usage["groups"][0]["unknown_tokens"]["reasoning_tokens"] == 1

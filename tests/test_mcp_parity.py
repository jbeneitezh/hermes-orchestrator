from __future__ import annotations

import uuid
from collections.abc import Iterator
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
from hermes_orchestrator.models import Agent, Base, CommunicationEdge, Run
from hermes_orchestrator.usage_services import ingest_run_usage

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
    }


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

    rest = client.get(f"/v1/tasks/{created['task']['id']}", headers={"X-Actor-Id": LEADER}).json()
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

    assert listed.json()["result"]["tools"] == []
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
        with pytest.raises(McpDomainError, match="Argumentos no válidos"):
            _execute_tool(
                session, leader, "task_create", {"idempotency_key": "invalid-01"}, settings
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

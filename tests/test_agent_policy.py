from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import (
    Agent,
    AgentInstance,
    AgentRequestRecord,
    AuditEvent,
    Base,
    CommunicationEdge,
    ExecutionProfile,
)
from hermes_orchestrator.policy import communication_is_allowed

LEADER_HEADERS = {"X-Actor-Id": "agent:leader"}
OWNER_HEADERS = {"X-Actor-Id": "user:owner"}
AGENT_PAYLOAD = {
    "slug": "researcher-01",
    "role": "researcher",
    "description": "Investiga hipótesis reproducibles",
    "policy_set": {"name": "research-default"},
    "capabilities": ["git_read"],
    "secret_refs": ["secret://codex/broker"],
}


@pytest.fixture
def catalog_context(
    tmp_path: Path,
) -> Iterator[tuple[TestClient, sessionmaker[Session]]]:
    database_path = tmp_path / "catalog.db"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        actor_roles={
            "user:owner": "owner",
            "agent:leader": "leader",
            "agent:operator": "operator",
        },
    )
    app = create_app(settings)
    Base.metadata.create_all(app.state.engine)
    with TestClient(app) as client:
        yield client, app.state.session_factory


def test_agent_request_is_idempotent_and_detects_key_reuse(catalog_context) -> None:
    client, _ = catalog_context
    headers = LEADER_HEADERS | {"Idempotency-Key": "request-researcher-01"}

    first = client.post("/v1/agents/requests", headers=headers, json=AGENT_PAYLOAD)
    replay = client.post("/v1/agents/requests", headers=headers, json=AGENT_PAYLOAD)
    conflict = client.post(
        "/v1/agents/requests",
        headers=headers,
        json=AGENT_PAYLOAD | {"description": "Otra solicitud"},
    )

    assert first.status_code == 202
    assert replay.status_code == 202
    assert first.json()["id"] == replay.json()["id"]
    assert replay.json()["replayed"] is True
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert conflict.status_code == 409


def test_policy_allows_known_roles_and_denies_by_default(catalog_context) -> None:
    client, _ = catalog_context

    assert client.get("/v1/agents", headers=LEADER_HEADERS).status_code == 200
    assert client.get("/v1/agents", headers={"X-Actor-Id": "unknown"}).status_code == 403
    denied_request = client.post(
        "/v1/agents/requests",
        headers={"X-Actor-Id": "agent:operator", "Idempotency-Key": "operator-request"},
        json=AGENT_PAYLOAD,
    )
    assert denied_request.status_code == 403


def test_communication_requires_an_active_matching_edge(catalog_context) -> None:
    _, factory = catalog_context
    now = datetime.now(UTC)
    with factory() as session:
        source = Agent(
            slug="leader",
            role="leader",
            description="Coordina",
            owner_actor_id="user:owner",
        )
        target = Agent(
            slug="developer",
            role="developer",
            description="Implementa",
            owner_actor_id="user:owner",
        )
        session.add_all([source, target])
        session.flush()
        session.add_all(
            [
                CommunicationEdge(
                    source_agent_id=source.id,
                    target_agent_id=target.id,
                    task_classes=["implementation"],
                    scopes=["task:create"],
                    expires_at=now + timedelta(hours=1),
                    approved_by_actor_id="user:owner",
                ),
                CommunicationEdge(
                    source_agent_id=source.id,
                    target_agent_id=target.id,
                    task_classes=["review"],
                    scopes=["task:create"],
                    expires_at=now - timedelta(seconds=1),
                    approved_by_actor_id="user:owner",
                ),
            ]
        )
        session.commit()

        assert communication_is_allowed(
            session, source.id, target.id, "implementation", "task:create", now=now
        )
        assert not communication_is_allowed(
            session, source.id, target.id, "implementation", "task:approve", now=now
        )
        assert not communication_is_allowed(
            session, source.id, target.id, "review", "task:create", now=now
        )


def test_actor_cannot_escalate_with_a_role_header(catalog_context) -> None:
    client, _ = catalog_context
    response = client.get(
        "/v1/agents",
        headers={"X-Actor-Id": "unknown", "X-Actor-Role": "owner"},
    )
    assert response.status_code == 403


def test_only_secret_references_are_accepted_and_persisted(catalog_context) -> None:
    client, factory = catalog_context
    headers = OWNER_HEADERS | {"Idempotency-Key": "secret-reference-only"}

    accepted = client.post("/v1/agents/requests", headers=headers, json=AGENT_PAYLOAD)
    rejected = client.post(
        "/v1/agents/requests",
        headers=OWNER_HEADERS | {"Idempotency-Key": "raw-secret-rejected"},
        json=AGENT_PAYLOAD | {"policy_set": {"name": "unsafe", "password": "no-debe-persistirse"}},
    )

    assert accepted.status_code == 202
    assert rejected.status_code == 422
    with factory() as session:
        records = list(session.scalars(select(AgentRequestRecord)))
        assert len(records) == 1
        serialized = json.dumps(records[0].payload)
        assert "no-debe-persistirse" not in serialized
        assert records[0].payload["secret_refs"] == ["secret://codex/broker"]


def test_agent_request_appends_exactly_one_audit_event(catalog_context) -> None:
    client, factory = catalog_context
    headers = OWNER_HEADERS | {"Idempotency-Key": "audit-request-01"}

    client.post("/v1/agents/requests", headers=headers, json=AGENT_PAYLOAD)
    client.post("/v1/agents/requests", headers=headers, json=AGENT_PAYLOAD)

    with factory() as session:
        events = list(session.scalars(select(AuditEvent)))
        assert len(events) == 1
        assert events[0].event_type == "agent.requested"
        assert events[0].actor_id == "user:owner"


def test_agent_list_and_detail_include_observed_instances(catalog_context) -> None:
    client, factory = catalog_context
    with factory() as session:
        agent = Agent(
            slug="validator",
            role="validator",
            description="Reproduce resultados",
            owner_actor_id="user:owner",
            capabilities=["test_run"],
        )
        agent.instances.append(
            AgentInstance(
                container_ref="hermes-validator-1",
                hermes_version="0.18.2",
                health="healthy",
                reconciliation_state="observed",
            )
        )
        session.add(agent)
        session.commit()
        agent_id = agent.id

    listing = client.get("/v1/agents", headers=LEADER_HEADERS)
    detail = client.get(f"/v1/agents/{agent_id}", headers=LEADER_HEADERS)

    assert listing.status_code == 200
    assert listing.json()[0]["slug"] == "validator"
    assert detail.status_code == 200
    assert detail.json()["instances"][0]["container_ref"] == "hermes-validator-1"
    missing = client.get(f"/v1/agents/{uuid.uuid4()}", headers=LEADER_HEADERS)
    assert missing.status_code == 404
    assert missing.json()["detail"]["code"] == "not_found"


def test_profiles_and_capabilities_publish_only_enabled_contracts(catalog_context) -> None:
    client, factory = catalog_context
    with factory() as session:
        session.add_all(
            [
                ExecutionProfile(
                    id="spark-low",
                    provider="openai-codex",
                    model="gpt-5.3-codex-spark",
                    reasoning_effort="low",
                    max_iterations=8,
                    timeout_seconds=300,
                    relative_cost=1,
                ),
                ExecutionProfile(
                    id="disabled",
                    provider="none",
                    model="none",
                    reasoning_effort="none",
                    max_iterations=1,
                    timeout_seconds=1,
                    relative_cost=0,
                    enabled=False,
                ),
            ]
        )
        session.commit()

    profiles = client.get("/v1/execution-profiles", headers=LEADER_HEADERS)
    capabilities = client.get("/v1/capabilities")

    assert profiles.status_code == 200
    assert [profile["id"] for profile in profiles.json()] == ["spark-low"]
    assert capabilities.status_code == 200
    assert "deny_by_default_policy" in capabilities.json()["capabilities"]
    assert session_count(factory, AuditEvent) == 0


def session_count(factory: sessionmaker[Session], model: type[AuditEvent]) -> int:
    with factory() as session:
        return session.scalar(select(func.count()).select_from(model)) or 0

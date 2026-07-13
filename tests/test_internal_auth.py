from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from hermes_orchestrator.auth import token_sha256
from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import Agent, Base
from tests.auth_helpers import auth_headers, seed_active_auth_agents, token_for, token_settings


@contextmanager
def auth_context(
    tmp_path: Path,
    *,
    actors: tuple[str, ...] = ("user:owner", "agent:leader", "agent:operator"),
    tokens: dict[str, str] | None = None,
    revoked: set[str] | None = None,
) -> Iterator[tuple[TestClient, Settings]]:
    database_path = tmp_path / f"auth-{uuid.uuid4()}.db"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        internal_auth_tokens=tokens or token_settings(*actors)["internal_auth_tokens"],
        internal_auth_revoked_token_hashes=revoked or set(),
    )
    app = create_app(settings)
    Base.metadata.create_all(app.state.engine)
    seed_active_auth_agents(app.state.session_factory, settings)
    with TestClient(app) as client:
        yield client, settings


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/v1/agents"),
        ("POST", "/v1/tasks"),
        ("POST", f"/v1/tasks/{uuid.uuid4()}/dispatch"),
        ("GET", f"/v1/runs/{uuid.uuid4()}"),
        ("POST", "/mcp/"),
        ("GET", "/v1/operations/tasks"),
    ],
)
def test_six_representative_surfaces_require_bearer(tmp_path: Path, method: str, path: str) -> None:
    with auth_context(tmp_path) as (client, _):
        response = client.request(method, path)

    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "token_missing"
    assert response.headers["WWW-Authenticate"] == "Bearer"


def test_distinct_owner_and_agent_tokens_resolve_server_side(tmp_path: Path) -> None:
    with auth_context(tmp_path) as (client, settings):
        owner = client.get("/v1/agents", headers=auth_headers("user:owner"))
        leader = client.get("/v1/agents", headers=auth_headers("agent:leader"))

    assert owner.status_code == 200
    assert leader.status_code == 200
    assert (
        settings.internal_auth_tokens["user:owner"].get_secret_value()
        != settings.internal_auth_tokens["agent:leader"].get_secret_value()
    )


def test_unknown_token_is_rejected_without_echoing_it(tmp_path: Path) -> None:
    with auth_context(tmp_path) as (client, _):
        response = client.get(
            "/v1/agents",
            headers={"Authorization": "Bearer unknown-test-credential"},
        )

    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "token_unknown"
    assert "unknown-test-credential" not in response.text


def test_revoked_token_is_rejected_before_identity_resolution(tmp_path: Path) -> None:
    token = token_for("user:owner")
    with auth_context(tmp_path, revoked={token_sha256(token)}) as (client, _):
        response = client.get("/v1/agents", headers=auth_headers("user:owner"))

    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "token_revoked"


def test_retired_agent_token_is_rejected(tmp_path: Path) -> None:
    with auth_context(tmp_path) as (client, _):
        factory = client.app.state.session_factory
        with factory() as session:
            agent = session.scalar(select(Agent).where(Agent.slug == "leader"))
            assert agent is not None
            agent.desired_state = "retired"
            session.commit()
        response = client.get("/v1/agents", headers=auth_headers("agent:leader"))

    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "agent_inactive"


def test_conflicting_tracing_header_cannot_impersonate_another_actor(tmp_path: Path) -> None:
    with auth_context(tmp_path) as (client, _):
        response = client.get(
            "/v1/agents",
            headers=auth_headers("agent:leader", **{"X-Actor-Id": "agent:operator"}),
        )

    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "identity_header_conflict"


def test_conflicting_agent_trace_is_rejected(tmp_path: Path) -> None:
    with auth_context(tmp_path) as (client, _):
        response = client.get(
            "/v1/agents",
            headers=auth_headers("agent:leader", **{"X-Agent-Id": str(uuid.uuid4())}),
        )

    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "identity_header_conflict"


def test_token_for_actor_without_role_is_rejected(tmp_path: Path) -> None:
    with auth_context(tmp_path, tokens={"agent:orphan": "test-only-orphan-token"}) as (
        client,
        _,
    ):
        response = client.get(
            "/v1/agents", headers={"Authorization": "Bearer test-only-orphan-token"}
        )

    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "identity_unknown"


def test_authenticated_role_without_permission_remains_forbidden(tmp_path: Path) -> None:
    payload: dict[str, Any] = {
        "slug": "another-agent",
        "role": "researcher",
        "description": "Solicitud no autorizada",
        "policy_set": {},
        "capabilities": [],
        "secret_refs": [],
    }
    with auth_context(tmp_path) as (client, _):
        response = client.post(
            "/v1/agents/requests",
            headers=auth_headers("agent:operator", **{"Idempotency-Key": "operator-denied-01"}),
            json=payload,
        )

    assert response.status_code == 403


def test_rotation_accepts_only_the_new_token_after_restart(tmp_path: Path) -> None:
    old_token = "test-only-old-owner-token"
    new_token = "test-only-new-owner-token"
    with auth_context(tmp_path, tokens={"user:owner": old_token}) as (client, _):
        assert (
            client.get("/v1/agents", headers={"Authorization": f"Bearer {old_token}"}).status_code
            == 200
        )
    with auth_context(tmp_path, tokens={"user:owner": new_token}) as (client, _):
        old = client.get("/v1/agents", headers={"Authorization": f"Bearer {old_token}"})
        new = client.get("/v1/agents", headers={"Authorization": f"Bearer {new_token}"})

    assert old.status_code == 401
    assert new.status_code == 200

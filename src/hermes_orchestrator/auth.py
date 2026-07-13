from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from typing import Any, cast

from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import Agent

PUBLIC_PATHS = frozenset(
    {
        "/health",
        "/v1/capabilities",
        "/openapi.json",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
    }
)
INACTIVE_AGENT_STATES = frozenset({"disabled", "decommissioned", "retired"})


@dataclass(frozen=True)
class AuthenticatedIdentity:
    actor_id: str
    role: str
    agent_id: str | None


@dataclass(frozen=True)
class AuthenticationError(Exception):
    code: str
    detail: str
    status_code: int = 401


def token_sha256(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _secret_value(value: SecretStr | str) -> str:
    return value.get_secret_value() if isinstance(value, SecretStr) else value


def _resolve_actor(token: str, settings: Settings) -> str | None:
    if token_sha256(token) in settings.internal_auth_revoked_token_hashes:
        raise AuthenticationError("token_revoked", "Credencial interna revocada")
    matches = [
        actor_id
        for actor_id, configured in settings.internal_auth_tokens.items()
        if secrets.compare_digest(token, _secret_value(configured))
    ]
    if len(matches) > 1:
        raise AuthenticationError("identity_unknown", "Credencial interna ambigua")
    return matches[0] if matches else None


def authenticate_bearer(
    authorization: str | None,
    settings: Settings,
    factory: sessionmaker[Session],
) -> AuthenticatedIdentity:
    scheme, separator, token = (authorization or "").partition(" ")
    if separator != " " or scheme.lower() != "bearer" or not token.strip():
        raise AuthenticationError("token_missing", "Se requiere bearer interno")
    raw_token = token.strip()
    actor_id = _resolve_actor(raw_token, settings)
    if actor_id is None:
        credential_hash = token_sha256(raw_token)
        with factory() as session:
            agents = list(session.scalars(select(Agent)))
            if any(
                secrets.compare_digest(
                    credential_hash,
                    str(agent.policy_set.get("revoked_runtime_auth_token_sha256", "")),
                )
                for agent in agents
            ):
                raise AuthenticationError("token_revoked", "Credencial interna revocada")
            matches = [
                agent
                for agent in agents
                if secrets.compare_digest(
                    credential_hash,
                    str(agent.policy_set.get("runtime_auth_token_sha256", "")),
                )
            ]
            if len(matches) != 1:
                raise AuthenticationError("token_unknown", "Credencial interna desconocida")
            dynamic_agent = matches[0]
            if (
                dynamic_agent.desired_state in INACTIVE_AGENT_STATES
                or dynamic_agent.desired_state != "active"
            ):
                raise AuthenticationError("agent_inactive", "Identidad de agente no activa")
            actor_id = f"agent:{dynamic_agent.slug}"
            settings.actor_roles[actor_id] = dynamic_agent.role
            return AuthenticatedIdentity(
                actor_id=actor_id,
                role=dynamic_agent.role,
                agent_id=str(dynamic_agent.id),
            )
    role = settings.actor_roles.get(actor_id)
    if role is None:
        raise AuthenticationError("identity_unknown", "Identidad interna no configurada")
    if not actor_id.startswith("agent:"):
        return AuthenticatedIdentity(actor_id=actor_id, role=role, agent_id=None)

    slug = actor_id.removeprefix("agent:")
    with factory() as session:
        agent = session.scalar(select(Agent).where(Agent.slug == slug))
        if (
            agent is None
            or agent.desired_state in INACTIVE_AGENT_STATES
            or agent.desired_state != "active"
            or agent.role != role
        ):
            raise AuthenticationError("agent_inactive", "Identidad de agente no activa")
        return AuthenticatedIdentity(actor_id=actor_id, role=role, agent_id=str(agent.id))


def _header(headers: list[tuple[bytes, bytes]], name: bytes) -> str | None:
    values = [value.decode("latin-1") for key, value in headers if key.lower() == name]
    return values[-1] if values else None


def _authoritative_headers(
    headers: list[tuple[bytes, bytes]], identity: AuthenticatedIdentity
) -> list[tuple[bytes, bytes]]:
    trace_actor = _header(headers, b"x-actor-id")
    trace_agent = _header(headers, b"x-agent-id")
    if trace_actor is not None and trace_actor != identity.actor_id:
        raise AuthenticationError(
            "identity_header_conflict", "X-Actor-Id no coincide con el bearer", 403
        )
    valid_agent_traces = {identity.actor_id}
    if identity.agent_id is not None:
        valid_agent_traces.add(identity.agent_id)
    if trace_agent is not None and trace_agent not in valid_agent_traces:
        raise AuthenticationError(
            "identity_header_conflict", "X-Agent-Id no coincide con el bearer", 403
        )

    filtered = [
        (key, value) for key, value in headers if key.lower() not in {b"x-actor-id", b"x-agent-id"}
    ]
    filtered.append((b"x-actor-id", identity.actor_id.encode("latin-1")))
    if identity.agent_id is not None:
        filtered.append((b"x-agent-id", identity.agent_id.encode("latin-1")))
    return filtered


class InternalAuthMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        settings: Settings,
        session_factory: sessionmaker[Session],
    ) -> None:
        self.app = app
        self.settings = settings
        self.session_factory = session_factory

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") in PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return
        headers = cast(list[tuple[bytes, bytes]], list(scope.get("headers", [])))
        try:
            identity = authenticate_bearer(
                _header(headers, b"authorization"), self.settings, self.session_factory
            )
            scope["headers"] = _authoritative_headers(headers, identity)
            state = cast(dict[str, Any], scope.setdefault("state", {}))
            state["authenticated_identity"] = identity
        except AuthenticationError as error:
            response = JSONResponse(
                status_code=error.status_code,
                content={"detail": {"code": error.code, "message": error.detail}},
                headers={"WWW-Authenticate": "Bearer"} if error.status_code == 401 else None,
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)

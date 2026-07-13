from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.orm import Session

from hermes_orchestrator.models import CommunicationEdge


class Permission(StrEnum):
    AGENTS_READ = "agents:read"
    AGENTS_REQUEST = "agents:request"
    PROFILES_READ = "profiles:read"
    TASKS_READ = "tasks:read"
    TASKS_CREATE = "tasks:create"
    TASKS_DISPATCH = "tasks:dispatch"
    TASKS_COMMENT = "tasks:comment"
    TASKS_CANCEL = "tasks:cancel"
    RUNS_READ = "runs:read"
    APPROVALS_DECIDE = "approvals:decide"


ROLE_PERMISSIONS: dict[str, frozenset[Permission]] = {
    "owner": frozenset(Permission),
    "leader": frozenset(
        {
            Permission.AGENTS_READ,
            Permission.AGENTS_REQUEST,
            Permission.PROFILES_READ,
            Permission.TASKS_READ,
            Permission.TASKS_CREATE,
            Permission.TASKS_DISPATCH,
            Permission.TASKS_COMMENT,
            Permission.TASKS_CANCEL,
            Permission.RUNS_READ,
            Permission.APPROVALS_DECIDE,
        }
    ),
    "operator": frozenset(
        {
            Permission.AGENTS_READ,
            Permission.PROFILES_READ,
            Permission.TASKS_READ,
            Permission.TASKS_CANCEL,
            Permission.RUNS_READ,
        }
    ),
    "researcher": frozenset(
        {
            Permission.AGENTS_READ,
            Permission.PROFILES_READ,
            Permission.TASKS_READ,
            Permission.TASKS_CREATE,
            Permission.TASKS_COMMENT,
            Permission.RUNS_READ,
        }
    ),
    "developer": frozenset(
        {
            Permission.AGENTS_READ,
            Permission.PROFILES_READ,
            Permission.TASKS_READ,
            Permission.TASKS_CREATE,
            Permission.TASKS_COMMENT,
            Permission.RUNS_READ,
        }
    ),
    "validator": frozenset(
        {
            Permission.AGENTS_READ,
            Permission.PROFILES_READ,
            Permission.TASKS_READ,
            Permission.TASKS_COMMENT,
            Permission.RUNS_READ,
            Permission.APPROVALS_DECIDE,
        }
    ),
}


def actor_is_allowed(
    actor_id: str,
    permission: Permission,
    actor_roles: dict[str, str],
) -> bool:
    role = actor_roles.get(actor_id)
    if role is None:
        return False
    return permission in ROLE_PERMISSIONS.get(role, frozenset())


def communication_is_allowed(
    session: Session,
    source_agent_id: uuid.UUID,
    target_agent_id: uuid.UUID,
    task_class: str,
    scope: str,
    *,
    now: datetime | None = None,
) -> bool:
    effective_now = now or datetime.now(UTC)
    edges = session.scalars(
        select(CommunicationEdge).where(
            CommunicationEdge.source_agent_id == source_agent_id,
            CommunicationEdge.target_agent_id == target_agent_id,
        )
    )

    def is_active(edge: CommunicationEdge) -> bool:
        if edge.expires_at is None:
            return True
        expires_at = edge.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        return expires_at > effective_now

    return any(
        is_active(edge) and task_class in edge.task_classes and scope in edge.scopes
        for edge in edges
    )

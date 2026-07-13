from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from hermes_orchestrator.models import (
    Agent,
    AgentRequestRecord,
    AuditEvent,
    ExecutionProfile,
    FleetReconcileRecord,
)


class AgentRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list(self) -> list[Agent]:
        statement = select(Agent).options(selectinload(Agent.instances)).order_by(Agent.slug)
        return list(self.session.scalars(statement).unique())

    def get(self, agent_id: uuid.UUID) -> Agent | None:
        statement = select(Agent).options(selectinload(Agent.instances)).where(Agent.id == agent_id)
        return self.session.scalar(statement)


class AgentRequestRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_idempotency_key(self, key: str) -> AgentRequestRecord | None:
        return self.session.scalar(
            select(AgentRequestRecord).where(AgentRequestRecord.idempotency_key == key)
        )

    def add(self, request: AgentRequestRecord) -> None:
        self.session.add(request)


class ExecutionProfileRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_enabled(self) -> list[ExecutionProfile]:
        statement = (
            select(ExecutionProfile)
            .where(ExecutionProfile.enabled.is_(True))
            .order_by(ExecutionProfile.relative_cost, ExecutionProfile.id)
        )
        return list(self.session.scalars(statement))


class AuditRepository:
    """Repositorio intencionadamente append-only: no expone update ni delete."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def append(self, event: AuditEvent) -> None:
        self.session.add(event)


class FleetReconcileRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, record: FleetReconcileRecord) -> None:
        self.session.add(record)

    def get_by_idempotency_key(self, key: str) -> FleetReconcileRecord | None:
        return self.session.scalar(
            select(FleetReconcileRecord).where(FleetReconcileRecord.idempotency_key == key)
        )

    def latest(self, project_name: str, compose_path: str) -> FleetReconcileRecord | None:
        statement = (
            select(FleetReconcileRecord)
            .where(
                FleetReconcileRecord.project_name == project_name,
                FleetReconcileRecord.compose_path == compose_path,
            )
            .order_by(FleetReconcileRecord.created_at.desc())
            .limit(1)
        )
        return self.session.scalar(statement)

    def latest_applied(self, project_name: str, compose_path: str) -> FleetReconcileRecord | None:
        statement = (
            select(FleetReconcileRecord)
            .where(
                FleetReconcileRecord.project_name == project_name,
                FleetReconcileRecord.compose_path == compose_path,
                FleetReconcileRecord.status.in_(("applied", "no_change")),
            )
            .order_by(FleetReconcileRecord.created_at.desc())
            .limit(1)
        )
        return self.session.scalar(statement)

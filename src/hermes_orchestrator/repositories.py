from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from hermes_orchestrator.models import Agent, AgentRequestRecord, AuditEvent, ExecutionProfile


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

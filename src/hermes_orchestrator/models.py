from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Agent(Base):
    __tablename__ = "agents"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    role: Mapped[str] = mapped_column(String(40), index=True)
    description: Mapped[str] = mapped_column(Text)
    desired_state: Mapped[str] = mapped_column(String(30), default="requested")
    owner_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    policy_set: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    capabilities: Mapped[list[str]] = mapped_column(JSON, default=list)
    secret_refs: Mapped[list[str]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )

    instances: Mapped[list[AgentInstance]] = relationship(
        back_populates="agent", cascade="all, delete-orphan"
    )


class AgentInstance(Base):
    __tablename__ = "agent_instances"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("agents.id"), index=True)
    container_ref: Mapped[str | None] = mapped_column(String(200))
    hermes_version: Mapped[str | None] = mapped_column(String(80))
    image_digest: Mapped[str | None] = mapped_column(String(160))
    internal_endpoint: Mapped[str | None] = mapped_column(String(500))
    config_revision: Mapped[str | None] = mapped_column(String(120))
    health: Mapped[str] = mapped_column(String(30), default="unknown")
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reconciliation_state: Mapped[str] = mapped_column(String(30), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    agent: Mapped[Agent] = relationship(back_populates="instances")


class ExecutionProfile(Base):
    __tablename__ = "execution_profiles"

    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    provider: Mapped[str] = mapped_column(String(80))
    model: Mapped[str] = mapped_column(String(120))
    reasoning_effort: Mapped[str] = mapped_column(String(20))
    service_tier: Mapped[str | None] = mapped_column(String(40))
    max_iterations: Mapped[int] = mapped_column(Integer)
    timeout_seconds: Mapped[int] = mapped_column(Integer)
    tool_policy: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    relative_cost: Mapped[int] = mapped_column(Integer)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class CommunicationEdge(Base):
    __tablename__ = "communication_edges"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    source_agent_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("agents.id"), index=True)
    target_agent_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("agents.id"), index=True)
    task_classes: Mapped[list[str]] = mapped_column(JSON, default=list)
    scopes: Mapped[list[str]] = mapped_column(JSON, default=list)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    approved_by_actor_id: Mapped[str] = mapped_column(String(160))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class AgentRequestRecord(Base):
    __tablename__ = "agent_requests"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    requested_by_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    request_type: Mapped[str] = mapped_column(String(30), default="create")
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(30), default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    actor_id: Mapped[str] = mapped_column(String(160), index=True)
    event_type: Mapped[str] = mapped_column(String(100), index=True)
    aggregate_type: Mapped[str] = mapped_column(String(80), index=True)
    aggregate_id: Mapped[str] = mapped_column(String(160), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

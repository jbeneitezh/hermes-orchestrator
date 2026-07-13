from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
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
    decision_idempotency_key: Mapped[str | None] = mapped_column(String(160), unique=True)
    decision_hash: Mapped[str | None] = mapped_column(String(64))
    decided_by_actor_id: Mapped[str | None] = mapped_column(String(160), index=True)
    decision_reason: Mapped[str | None] = mapped_column(Text)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    application_idempotency_key: Mapped[str | None] = mapped_column(String(160), unique=True)
    application_hash: Mapped[str | None] = mapped_column(String(64))
    applied_by_actor_id: Mapped[str | None] = mapped_column(String(160), index=True)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    application_error_code: Mapped[str | None] = mapped_column(String(100))
    application_error_detail: Mapped[str | None] = mapped_column(Text)
    retirement_idempotency_key: Mapped[str | None] = mapped_column(String(160), unique=True)
    retirement_hash: Mapped[str | None] = mapped_column(String(64))
    retired_by_actor_id: Mapped[str | None] = mapped_column(String(160), index=True)
    retirement_reason: Mapped[str | None] = mapped_column(Text)
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    actor_id: Mapped[str] = mapped_column(String(160), index=True)
    event_type: Mapped[str] = mapped_column(String(100), index=True)
    aggregate_type: Mapped[str] = mapped_column(String(80), index=True)
    aggregate_id: Mapped[str] = mapped_column(String(160), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class FleetReconcileRecord(Base):
    __tablename__ = "fleet_reconcile_records"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    requested_by_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    project_name: Mapped[str] = mapped_column(String(120), index=True)
    compose_path: Mapped[str] = mapped_column(String(500))
    mode: Mapped[str] = mapped_column(String(20))
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    desired_hash: Mapped[str] = mapped_column(String(64), index=True)
    diff_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    risk: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(30), index=True)
    approval_actor_id: Mapped[str | None] = mapped_column(String(160))
    approval_reason: Mapped[str | None] = mapped_column(Text)
    runner_result: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    rollback_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        Index("ux_tasks_cancel_idempotency_key", "cancel_idempotency_key", unique=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    operation_id: Mapped[uuid.UUID] = mapped_column(Uuid, default=uuid.uuid4, index=True)
    parent_task_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tasks.id"), index=True)
    workflow_ref: Mapped[str | None] = mapped_column(String(160))
    requester_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    assignee_actor_id: Mapped[str | None] = mapped_column(String(160), index=True)
    reviewer_actor_id: Mapped[str | None] = mapped_column(String(160), index=True)
    priority: Mapped[int] = mapped_column(Integer, default=50)
    status: Mapped[str] = mapped_column(String(40), default="pending", index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    objective: Mapped[str] = mapped_column(Text)
    acceptance_criteria: Mapped[list[str]] = mapped_column(JSON, default=list)
    budget: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    references: Mapped[list[str]] = mapped_column(JSON, default=list)
    independent_review: Mapped[bool] = mapped_column(Boolean, default=False)
    deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancel_idempotency_key: Mapped[str | None] = mapped_column(String(160))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )

    runs: Mapped[list[Run]] = relationship(
        back_populates="task", cascade="all, delete-orphan", order_by="Run.attempt_number"
    )
    comments: Mapped[list[TaskComment]] = relationship(
        back_populates="task", cascade="all, delete-orphan", order_by="TaskComment.created_at"
    )
    dependency_links: Mapped[list[TaskDependency]] = relationship(
        foreign_keys="TaskDependency.task_id", cascade="all, delete-orphan"
    )
    artifacts: Mapped[list[Artifact]] = relationship(
        back_populates="task", cascade="all, delete-orphan"
    )


class TaskDependency(Base):
    __tablename__ = "task_dependencies"

    task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    depends_on_task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    required_status: Mapped[str] = mapped_column(String(40), default="completed")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class TaskComment(Base):
    __tablename__ = "task_comments"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), index=True)
    actor_id: Mapped[str] = mapped_column(String(160), index=True)
    body: Mapped[str] = mapped_column(Text)
    idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    task: Mapped[Task] = relationship(back_populates="comments")


class Run(Base):
    __tablename__ = "runs"
    __table_args__ = (
        UniqueConstraint("task_id", "attempt_number", name="uq_runs_task_attempt"),
        Index("ix_runs_dispatch_claim", "status", "next_attempt_at", "lease_expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), index=True)
    operation_id: Mapped[uuid.UUID] = mapped_column(Uuid, index=True)
    attempt_number: Mapped[int] = mapped_column(Integer)
    worker_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    requested_profile_id: Mapped[str] = mapped_column(String(80))
    effective_profile_id: Mapped[str | None] = mapped_column(String(80))
    dispatch_idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    dispatch_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(40), default="dispatching", index=True)
    dispatch_attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    lease_owner: Mapped[str | None] = mapped_column(String(160))
    lease_acquired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    timeout_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_code: Mapped[str | None] = mapped_column(String(100))
    summary: Mapped[str | None] = mapped_column(Text)
    worker_run_id: Mapped[str | None] = mapped_column(String(160), index=True)
    requested_runtime: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    observed_runtime: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    runtime_fallback: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    usage_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error_details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    task: Mapped[Task] = relationship(back_populates="runs")
    approvals: Mapped[list[Approval]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="Approval.created_at"
    )
    artifacts: Mapped[list[Artifact]] = relationship(back_populates="run")
    events: Mapped[list[RunEvent]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="RunEvent.sequence"
    )


class Approval(Base):
    __tablename__ = "approvals"
    __table_args__ = (
        Index(
            "ux_approvals_decision_idempotency_key",
            "decision_idempotency_key",
            unique=True,
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id"), index=True)
    action: Mapped[str] = mapped_column(String(100))
    context_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    requested_by_actor_id: Mapped[str] = mapped_column(String(160))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    decided_by_actor_id: Mapped[str | None] = mapped_column(String(160))
    decision_reason: Mapped[str | None] = mapped_column(Text)
    decision_idempotency_key: Mapped[str | None] = mapped_column(String(160))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    run: Mapped[Run] = relationship(back_populates="approvals")


class Artifact(Base):
    __tablename__ = "artifacts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), index=True)
    run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("runs.id"), index=True)
    kind: Mapped[str] = mapped_column(String(80))
    uri: Mapped[str] = mapped_column(String(1000))
    sha256: Mapped[str | None] = mapped_column(String(64))
    producer_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    sensitivity: Mapped[str] = mapped_column(String(40), default="internal")
    validation: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    task: Mapped[Task] = relationship(back_populates="artifacts")
    run: Mapped[Run | None] = relationship(back_populates="artifacts")


class RunEvent(Base):
    __tablename__ = "run_events"
    __table_args__ = (UniqueConstraint("run_id", "sequence", name="uq_run_events_sequence"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id"), index=True)
    sequence: Mapped[int] = mapped_column(Integer)
    worker_event_id: Mapped[str | None] = mapped_column(String(160))
    event_type: Mapped[str] = mapped_column(String(100), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    terminal: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

    run: Mapped[Run] = relationship(back_populates="events")


class WorkflowContinuation(Base):
    __tablename__ = "workflow_continuations"
    __table_args__ = (
        UniqueConstraint(
            "trigger_event_id",
            "target_actor_id",
            "action",
            name="uq_workflow_continuations_trigger_actor_action",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    operation_id: Mapped[uuid.UUID] = mapped_column(Uuid, index=True)
    parent_task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), index=True)
    trigger_event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("run_events.id"), index=True)
    target_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    action: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    context_snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    failure_code: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UsageLedger(Base):
    __tablename__ = "usage_ledger"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id"), unique=True, index=True)
    payload_hash: Mapped[str] = mapped_column(String(64))
    operation_id: Mapped[uuid.UUID] = mapped_column(Uuid, index=True)
    task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), index=True)
    parent_run_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, index=True)
    project_id: Mapped[str] = mapped_column(String(120), index=True)
    category: Mapped[str] = mapped_column(String(120), index=True)
    requesting_agent_id: Mapped[str] = mapped_column(String(160), index=True)
    executing_agent_id: Mapped[str] = mapped_column(String(160), index=True)
    requested_profile: Mapped[str] = mapped_column(String(80), index=True)
    effective_profile: Mapped[str | None] = mapped_column(String(80), index=True)
    requested_model: Mapped[str | None] = mapped_column(String(120))
    requested_provider: Mapped[str | None] = mapped_column(String(80))
    requested_reasoning_effort: Mapped[str | None] = mapped_column(String(20))
    model: Mapped[str | None] = mapped_column(String(120), index=True)
    provider: Mapped[str | None] = mapped_column(String(80), index=True)
    reasoning_effort: Mapped[str | None] = mapped_column(String(20))
    runtime_fallback: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)
    reasoning_tokens: Mapped[int | None] = mapped_column(Integer)
    cache_read_tokens: Mapped[int | None] = mapped_column(Integer)
    cache_write_tokens: Mapped[int | None] = mapped_column(Integer)
    api_calls: Mapped[int | None] = mapped_column(Integer)
    estimated_cost: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    actual_cost: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    currency: Mapped[str | None] = mapped_column(String(8))
    cost_status: Mapped[str] = mapped_column(String(20), default="unknown", index=True)
    cost_source: Mapped[str | None] = mapped_column(String(120))
    quota_status: Mapped[str] = mapped_column(String(30), default="available", index=True)
    quota_reset_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    outcome: Mapped[str] = mapped_column(String(40), index=True)
    retry_number: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class Budget(Base):
    __tablename__ = "budgets"
    __table_args__ = (UniqueConstraint("scope_type", "scope_key", name="uq_budget_scope"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    scope_type: Mapped[str] = mapped_column(String(30), index=True)
    scope_key: Mapped[str] = mapped_column(String(160), index=True)
    window_seconds: Mapped[int] = mapped_column(Integer, default=86400)
    soft_token_limit: Mapped[int | None] = mapped_column(Integer)
    hard_token_limit: Mapped[int | None] = mapped_column(Integer)
    max_concurrent_runs: Mapped[int | None] = mapped_column(Integer)
    max_fan_out: Mapped[int | None] = mapped_column(Integer)
    max_retries: Mapped[int | None] = mapped_column(Integer)
    circuit_failure_threshold: Mapped[int | None] = mapped_column(Integer)
    circuit_cooldown_seconds: Mapped[int | None] = mapped_column(Integer)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class CircuitBreaker(Base):
    __tablename__ = "circuit_breakers"
    __table_args__ = (
        UniqueConstraint("worker_actor_id", "profile_id", name="uq_circuit_worker_profile"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    worker_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    profile_id: Mapped[str] = mapped_column(String(80), index=True)
    state: Mapped[str] = mapped_column(String(20), default="closed", index=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reset_eligible_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reset_by_actor_id: Mapped[str | None] = mapped_column(String(160))
    reset_reason: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class EnvironmentDeployment(Base):
    __tablename__ = "environment_deployments"
    __table_args__ = (
        Index("ix_environment_deployments_current", "environment", "instance_key", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    environment: Mapped[str] = mapped_column(String(30), index=True)
    instance_key: Mapped[str] = mapped_column(String(160), index=True)
    repository: Mapped[str] = mapped_column(String(240), index=True)
    ref_kind: Mapped[str] = mapped_column(String(20))
    ref_value: Mapped[str] = mapped_column(String(300))
    resolved_sha: Mapped[str] = mapped_column(String(40), index=True)
    task_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("tasks.id"), index=True)
    allocated_port: Mapped[int | None] = mapped_column(Integer, index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str] = mapped_column(String(30), default="active", index=True)
    source_deployment_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("environment_deployments.id"), index=True
    )
    rollback_of_deployment_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("environment_deployments.id"), index=True
    )
    requested_by_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    approval_actor_id: Mapped[str | None] = mapped_column(String(160))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class EnvironmentAction(Base):
    __tablename__ = "environment_actions"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    action: Mapped[str] = mapped_column(String(30), index=True)
    environment: Mapped[str] = mapped_column(String(30), index=True)
    requested_by_actor_id: Mapped[str] = mapped_column(String(160), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(30), index=True)
    deployment_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("environment_deployments.id"), index=True
    )
    previous_deployment_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("environment_deployments.id"), index=True
    )
    approval_actor_id: Mapped[str | None] = mapped_column(String(160))
    approval_reason: Mapped[str | None] = mapped_column(Text)
    error_code: Mapped[str | None] = mapped_column(String(100), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)

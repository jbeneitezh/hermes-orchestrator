from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "unavailable"]
    database: Literal["ok", "unavailable"]


class CapabilitiesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: str
    version: str
    api_version: Literal["v1"] = "v1"
    capabilities: list[str]


AgentRole = Literal[
    "leader",
    "researcher",
    "developer",
    "validator",
    "operator",
    "data_steward",
    "risk_manager",
    "trader",
]


class AgentRequestCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: str
    role: AgentRole
    description: str
    policy_set: dict[str, Any] = Field(default_factory=dict)
    capabilities: list[str] = Field(default_factory=list)
    secret_refs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def reject_embedded_secrets(self) -> AgentRequestCreate:
        forbidden_fragments = ("password", "secret", "token", "api_key", "credential")

        def visit(value: Any, key: str = "") -> None:
            normalized_key = key.lower()
            is_reference = normalized_key.endswith(("_ref", "_refs"))
            is_token_budget = normalized_key.endswith("_token_limit") or normalized_key in {
                "max_tokens",
                "token_budget",
            }
            if (
                normalized_key
                and not is_reference
                and not is_token_budget
                and any(fragment in normalized_key for fragment in forbidden_fragments)
            ):
                raise ValueError("Los secretos solo pueden declararse mediante *_ref o *_refs")
            if isinstance(value, dict):
                for child_key, child_value in value.items():
                    visit(child_value, str(child_key))
            elif isinstance(value, list):
                for child in value:
                    visit(child, key)

        visit(self.policy_set)
        return self


class AgentRequestResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    request_type: str
    requested_by_actor_id: str
    payload: dict[str, Any]
    status: str
    decision_idempotency_key: str | None
    decided_by_actor_id: str | None
    decision_reason: str | None
    decided_at: datetime | None
    application_idempotency_key: str | None
    applied_by_actor_id: str | None
    applied_at: datetime | None
    application_error_code: str | None
    application_error_detail: str | None
    retirement_idempotency_key: str | None
    retired_by_actor_id: str | None
    retirement_reason: str | None
    retired_at: datetime | None
    replayed: bool = False
    created_at: datetime
    updated_at: datetime


class AgentRequestDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: Literal["approve", "reject"]
    reason: str = Field(min_length=1, max_length=2000)


class AgentRequestRetire(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=2000)


class AgentProvisionCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=2000)


class AgentProvisionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: uuid.UUID
    status: Literal["applied", "no_change", "rolled_back"]
    service_name: str
    config_digest: str
    health: str
    replayed: bool = False


class AgentInstanceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    container_ref: str | None
    hermes_version: str | None
    image_digest: str | None
    internal_endpoint: str | None
    config_revision: str | None
    health: str
    last_heartbeat_at: datetime | None
    reconciliation_state: str


class AgentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    slug: str
    role: str
    description: str
    desired_state: str
    owner_actor_id: str
    policy_set: dict[str, Any]
    capabilities: list[str]
    secret_refs: list[str]
    instances: list[AgentInstanceResponse]
    created_at: datetime
    updated_at: datetime


class ExecutionProfileResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: str
    provider: str
    model: str
    reasoning_effort: str
    service_tier: str | None
    max_iterations: int
    timeout_seconds: int
    tool_policy: dict[str, Any]
    relative_cost: int


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    detail: str


class ErrorEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    detail: ErrorResponse


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    objective: str = Field(min_length=1, max_length=4000)
    acceptance_criteria: list[str] = Field(min_length=1)
    assignee_actor_id: str | None = None
    reviewer_actor_id: str | None = None
    independent_review: bool = False
    priority: int = Field(default=50, ge=0, le=100)
    parent_task_id: uuid.UUID | None = None
    workflow_ref: str | None = None
    dependency_ids: list[uuid.UUID] = Field(default_factory=list)
    budget: dict[str, Any] = Field(default_factory=dict)
    references: list[str] = Field(default_factory=list)
    deadline_at: datetime | None = None

    @model_validator(mode="after")
    def require_independent_reviewer(self) -> TaskCreate:
        if self.independent_review:
            if self.reviewer_actor_id is None:
                raise ValueError("Una tarea con revisión independiente exige reviewer")
            if self.assignee_actor_id == self.reviewer_actor_id:
                raise ValueError("El reviewer debe ser distinto del assignee")
        return self


class TaskCommentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    actor_id: str
    body: str
    created_at: datetime


class ApprovalResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    action: str
    status: str
    expires_at: datetime
    decided_by_actor_id: str | None
    decision_reason: str | None
    decided_at: datetime | None


class RunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    task_id: uuid.UUID
    operation_id: uuid.UUID
    attempt_number: int
    worker_actor_id: str
    requested_profile_id: str
    effective_profile_id: str | None
    status: str
    timeout_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_code: str | None
    summary: str | None
    worker_run_id: str | None
    requested_runtime: dict[str, Any]
    observed_runtime: dict[str, Any]
    runtime_fallback: dict[str, Any]
    usage_snapshot: dict[str, Any]
    error_details: dict[str, Any]
    approvals: list[ApprovalResponse]
    created_at: datetime


class RunEventResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    sequence: int
    worker_event_id: str | None
    event_type: str
    payload: dict[str, Any]
    terminal: bool
    created_at: datetime


class TaskResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    operation_id: uuid.UUID
    parent_task_id: uuid.UUID | None
    workflow_ref: str | None
    requester_actor_id: str
    assignee_actor_id: str | None
    reviewer_actor_id: str | None
    priority: int
    status: str
    objective: str
    acceptance_criteria: list[str]
    budget: dict[str, Any]
    references: list[str]
    independent_review: bool
    deadline_at: datetime | None
    runs: list[RunResponse]
    comments: list[TaskCommentResponse]
    dependency_ids: list[uuid.UUID] = Field(default_factory=list)
    replayed: bool = False
    created_at: datetime
    updated_at: datetime


class DispatchCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_actor_id: str
    requested_profile_id: str
    timeout_seconds: int = Field(default=900, ge=1, le=86400)
    requires_approval: bool = False
    approval_action: str = "dispatch"
    approval_ttl_seconds: int = Field(default=900, ge=1, le=86400)


class DispatchResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run: RunResponse
    replayed: bool = False


class TaskCommentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    body: str = Field(min_length=1, max_length=10000)


class CancelCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=2000)


class ApprovalDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: Literal["approved", "rejected"]
    reason: str = Field(min_length=1, max_length=2000)


class FleetMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1, max_length=500)
    target: str = Field(min_length=1, max_length=500)
    read_only: bool = True


class FleetServiceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,79}$")
    image: str = Field(min_length=1, max_length=200)
    mounts: list[FleetMountSpec] = Field(default_factory=list)
    privileged: bool = False
    network_mode: str | None = None
    pid_mode: str | None = None


class FleetApprovalEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor_id: str = Field(min_length=1, max_length=160)
    decision: Literal["approved"] = "approved"
    reason: str = Field(min_length=1, max_length=2000)


class FleetReconcileCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_name: str = Field(min_length=1, max_length=120)
    compose_path: str = Field(min_length=1, max_length=500)
    mode: Literal["dry_run", "apply"]
    targets: list[FleetServiceSpec] = Field(min_length=1)
    approval: FleetApprovalEvidence | None = None


class FleetReconcileResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: uuid.UUID
    status: str
    mode: str
    risk: str
    desired_hash: str
    diff: dict[str, Any]
    approval_actor_id: str | None
    runner_result: dict[str, Any]
    rollback_available: bool
    replayed: bool = False
    created_at: datetime


class FleetStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_name: str
    compose_path: str
    compose_digest: str
    services: list[dict[str, Any]]
    last_reconcile: FleetReconcileResponse | None


class UsageDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    run_id: uuid.UUID
    operation_id: uuid.UUID
    task_id: uuid.UUID
    parent_run_id: uuid.UUID | None
    project_id: str
    category: str
    requesting_agent_id: str
    executing_agent_id: str
    requested_profile: str
    effective_profile: str | None
    requested_model: str | None
    requested_provider: str | None
    requested_reasoning_effort: str | None
    model: str | None
    provider: str | None
    reasoning_effort: str | None
    runtime_fallback: dict[str, Any]
    input_tokens: int | None
    output_tokens: int | None
    reasoning_tokens: int | None
    cache_read_tokens: int | None
    cache_write_tokens: int | None
    api_calls: int | None
    estimated_cost: Decimal | None
    actual_cost: Decimal | None
    currency: str | None
    cost_status: str
    cost_source: str | None
    quota_status: str
    quota_reset_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    duration_ms: int | None
    outcome: str
    retry_number: int
    created_at: datetime


class UsageSummaryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_by: Literal["operation", "agent", "profile", "day"]
    groups: list[dict[str, Any]]
    entries: int
    cost_status: Literal["ledger"]


class UsageControlStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    budgets: list[dict[str, Any]]
    quota: list[dict[str, Any]]
    circuits: list[dict[str, Any]]
    audit: list[dict[str, Any]]


class CircuitResetCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=2000)


class CircuitResetResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    worker_actor_id: str
    profile_id: str
    state: Literal["closed"]
    consecutive_failures: int
    reset_by_actor_id: str | None
    reset_reason: str | None


EnvironmentName = Literal["local", "dev", "pre", "prod-sim"]


class EnvironmentApprovalEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor_id: str = Field(min_length=1, max_length=160)
    decision: Literal["approved"] = "approved"
    reason: str = Field(min_length=1, max_length=2000)


class EnvironmentDeployCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: Literal["local", "dev"]
    repository: str = Field(min_length=1, max_length=240)
    branch: str = Field(min_length=1, max_length=300)
    resolved_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    task_id: uuid.UUID | None = None
    ttl_seconds: int | None = Field(default=None, gt=0)


class EnvironmentPromotionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_deployment_id: uuid.UUID
    target_environment: Literal["pre", "prod-sim", "live"]
    repository: str = Field(min_length=1, max_length=240)
    resolved_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    tag: str | None = Field(default=None, min_length=1, max_length=300)
    approval: EnvironmentApprovalEvidence | None = None


class EnvironmentRollbackCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_deployment_id: uuid.UUID
    approval: EnvironmentApprovalEvidence


class EnvironmentDeploymentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: uuid.UUID
    environment: EnvironmentName
    instance_key: str
    repository: str
    ref_kind: Literal["branch", "sha", "tag"]
    ref_value: str
    resolved_sha: str
    task_id: uuid.UUID | None
    allocated_port: int | None
    expires_at: datetime | None
    status: str
    source_deployment_id: uuid.UUID | None
    rollback_of_deployment_id: uuid.UUID | None
    requested_by_actor_id: str
    approval_actor_id: str | None
    replayed: bool = False
    created_at: datetime


class EnvironmentInventoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    definitions: list[dict[str, Any]]
    deployments: list[EnvironmentDeploymentResponse]

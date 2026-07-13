from __future__ import annotations

import uuid
from datetime import datetime
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
            if (
                normalized_key
                and not is_reference
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
    status: str
    replayed: bool
    created_at: datetime


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
    approvals: list[ApprovalResponse]
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

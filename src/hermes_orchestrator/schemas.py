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

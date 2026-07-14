from __future__ import annotations

import hashlib
import json
import threading
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Literal, Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import Agent, AgentRequestRecord
from hermes_orchestrator.provisioning import AgentProvisioner, ProvisioningError
from hermes_orchestrator.provisioning_services import provision_agent_request
from hermes_orchestrator.services import decide_agent_request


class StopFlag(Protocol):
    def is_set(self) -> bool: ...


@dataclass(frozen=True)
class SafeAgentProfile:
    profile_id: str
    role: str
    slug_prefix: str
    policy_names: tuple[str, ...]
    capabilities: tuple[str, ...]
    secret_refs: tuple[str, ...]
    toolsets: tuple[str, ...]
    mcp_tools: tuple[str, ...]
    communication: tuple[str, ...]
    mount_profiles: tuple[str, ...]
    budget_limits: tuple[tuple[str, int], ...]
    max_active_role: int


DATA_STEWARD_PROFILE = SafeAgentProfile(
    profile_id="data-steward-v3",
    role="data_steward",
    slug_prefix="data-steward-",
    policy_names=("data-minimal", "data-read-only", "data-steward-v3"),
    capabilities=(
        "dataset_read",
        "git_read",
        "git_write_branch",
        "github_pr_create",
        "knowledge_read",
        "knowledge_write_branch",
        "task_block",
        "task_comment",
        "task_complete",
        "task_read",
    ),
    secret_refs=(
        "secret://codex/broker-client",
        "secret://github/shared-agent",
    ),
    toolsets=("terminal_read", "files_read", "git", "mcp"),
    mcp_tools=("task_get", "task_comment", "task_block", "task_complete"),
    communication=("agent:leader", "agent:researcher", "agent:validator"),
    mount_profiles=("tradix-dataset-readonly", "knowledge-branch-workspace"),
    budget_limits=(
        ("hard_token_limit", 500_000),
        ("max_concurrent_runs", 1),
        ("max_iterations", 8),
        ("max_sources", 25),
    ),
    max_active_role=2,
)

RISK_MANAGER_PROFILE = SafeAgentProfile(
    profile_id="risk-manager-v3",
    role="risk_manager",
    slug_prefix="risk-manager-",
    policy_names=("risk-manager-v3",),
    capabilities=(
        "dataset_read",
        "git_read",
        "git_write_branch",
        "github_pr_create",
        "knowledge_read",
        "knowledge_write_branch",
        "metrics_read",
        "task_block",
        "task_comment",
        "task_complete",
        "task_read",
    ),
    secret_refs=(
        "secret://codex/broker-client",
        "secret://github/shared-agent",
    ),
    toolsets=("terminal_read", "files_read", "git", "mcp"),
    mcp_tools=("task_get", "task_comment", "task_block", "task_complete"),
    communication=(
        "agent:leader",
        "agent:trader",
        "agent:researcher",
        "agent:validator",
    ),
    mount_profiles=("risk-knowledge-dataset-tradix-readonly",),
    budget_limits=(
        ("hard_token_limit", 750_000),
        ("max_concurrent_runs", 1),
        ("max_iterations", 10),
        ("max_sources", 30),
    ),
    max_active_role=1,
)

TRADER_PROFILE = SafeAgentProfile(
    profile_id="trader-v3",
    role="trader",
    slug_prefix="trader-",
    policy_names=("trader-v3",),
    capabilities=(
        "dataset_read",
        "git_read",
        "git_write_branch",
        "github_pr_create",
        "knowledge_read",
        "knowledge_write_branch",
        "market_data_read",
        "metrics_read",
        "signals_read",
        "task_block",
        "task_comment",
        "task_complete",
        "task_read",
    ),
    secret_refs=(
        "secret://codex/broker-client",
        "secret://github/shared-agent",
    ),
    toolsets=("terminal_read", "files_read", "git", "mcp"),
    mcp_tools=("task_get", "task_comment", "task_block", "task_complete"),
    communication=(
        "agent:leader",
        "agent:risk_manager",
        "agent:researcher",
        "agent:validator",
    ),
    mount_profiles=("trader-knowledge-dataset-tradix-readonly",),
    budget_limits=(
        ("hard_token_limit", 750_000),
        ("max_concurrent_runs", 1),
        ("max_iterations", 10),
        ("max_sources", 30),
    ),
    max_active_role=1,
)

SAFE_AGENT_PROFILES = {
    DATA_STEWARD_PROFILE.role: DATA_STEWARD_PROFILE,
    RISK_MANAGER_PROFILE.role: RISK_MANAGER_PROFILE,
    TRADER_PROFILE.role: TRADER_PROFILE,
}
ALLOWED_POLICY_KEYS = {
    "allowed_profiles",
    "budget",
    "communication",
    "execution_profile_default",
    "mcp_tools",
    "mount_profile",
    "name",
    "toolsets",
}
DANGEROUS_MARKERS = {
    "capital",
    "command",
    "docker",
    "entrypoint",
    "environment_promote",
    "host_network",
    "live_trade",
    "network_mode",
    "order_submit",
    "pid_mode",
    "privileged",
    "secret_admin",
    "socket",
}


@dataclass(frozen=True)
class AgentPolicyDecision:
    action: Literal["approve", "reject", "defer"]
    code: str
    reason: str
    profile_id: str | None = None
    profile_digest: str | None = None


@dataclass(frozen=True)
class AgentPolicyResult:
    request_id: uuid.UUID
    action: str
    status: str
    code: str
    replayed: bool = False
    profile_digest: str | None = None


def _profile_digest(profile: SafeAgentProfile) -> str:
    payload = json.dumps(asdict(profile), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def _sequence(value: object) -> list[str] | None:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return None
    return value


def _dangerous(value: object) -> str | None:
    if isinstance(value, dict):
        for key, child in value.items():
            marker = _dangerous(str(key)) or _dangerous(child)
            if marker:
                return marker
    elif isinstance(value, list):
        for child in value:
            marker = _dangerous(child)
            if marker:
                return marker
    elif isinstance(value, str):
        normalized = value.lower().replace("-", "_")
        return next((item for item in DANGEROUS_MARKERS if item in normalized), None)
    return None


def _subset(payload: dict[str, Any], key: str, allowed: tuple[str, ...]) -> bool:
    if key not in payload:
        return True
    values = _sequence(payload[key])
    return values is not None and set(values).issubset(allowed)


def _managed_agents(session: Session) -> list[Agent]:
    return [
        agent
        for agent in session.scalars(select(Agent).where(Agent.desired_state == "active"))
        if agent.policy_set.get("managed_by") == "agent-provisioner"
    ]


def evaluate_agent_request(
    session: Session,
    request: AgentRequestRecord,
    settings: Settings,
) -> AgentPolicyDecision:
    if not settings.agent_policy_enabled:
        return AgentPolicyDecision("defer", "policy_disabled", "Policy automática desactivada")
    if request.requested_by_actor_id == settings.agent_policy_actor_id:
        return AgentPolicyDecision(
            "reject", "self_escalation_denied", "El actor de policy no puede solicitarse autoridad"
        )

    payload = request.payload
    role = payload.get("role")
    profile = SAFE_AGENT_PROFILES.get(str(role))
    if profile is None or role not in settings.agent_policy_allowed_roles:
        return AgentPolicyDecision(
            "reject", "role_not_allowlisted", "Rol fuera del catálogo seguro"
        )
    digest = _profile_digest(profile)

    slug = payload.get("slug")
    if not isinstance(slug, str) or not slug.startswith(profile.slug_prefix):
        return AgentPolicyDecision(
            "reject", "slug_profile_mismatch", "Slug incompatible con el perfil firmado"
        )
    active = _managed_agents(session)
    if any(agent.slug == slug for agent in active):
        return AgentPolicyDecision(
            "reject", "slug_already_active", "Ya existe un agente activo con el mismo slug"
        )
    if len(active) >= settings.agent_policy_max_active_agents:
        return AgentPolicyDecision(
            "reject", "fleet_limit_exceeded", "La flota dinámica alcanzó su límite"
        )
    if sum(agent.role == profile.role for agent in active) >= profile.max_active_role:
        return AgentPolicyDecision(
            "reject", "role_limit_exceeded", "El rol alcanzó su límite de instancias activas"
        )

    capabilities = _sequence(payload.get("capabilities", []))
    if capabilities is None or not set(capabilities).issubset(profile.capabilities):
        return AgentPolicyDecision(
            "reject", "capability_not_allowlisted", "Capabilities fuera del perfil firmado"
        )
    marker = _dangerous(capabilities)
    if marker:
        return AgentPolicyDecision(
            "reject", "sensitive_authority_denied", f"Autoridad sensible denegada: {marker}"
        )
    secret_refs = _sequence(payload.get("secret_refs", []))
    if secret_refs is None or not set(secret_refs).issubset(profile.secret_refs):
        return AgentPolicyDecision(
            "reject", "secret_ref_not_allowlisted", "Referencia de secreto no estándar"
        )

    policy_set = payload.get("policy_set", {})
    if not isinstance(policy_set, dict) or not set(policy_set).issubset(ALLOWED_POLICY_KEYS):
        return AgentPolicyDecision(
            "reject", "policy_field_not_allowlisted", "Campo de policy fuera del perfil firmado"
        )
    marker = _dangerous(policy_set)
    if marker:
        return AgentPolicyDecision(
            "reject", "sensitive_authority_denied", f"Autoridad sensible denegada: {marker}"
        )
    if policy_set.get("name") not in profile.policy_names:
        return AgentPolicyDecision(
            "reject", "policy_name_not_allowlisted", "Nombre de policy no allowlisted"
        )
    if policy_set.get("execution_profile_default", "sol-high") != "sol-high":
        return AgentPolicyDecision("reject", "model_policy_denied", "El perfil debe ser sol-high")
    if policy_set.get("allowed_profiles", ["sol-high"]) != ["sol-high"]:
        return AgentPolicyDecision(
            "reject", "model_policy_denied", "El allowlist debe contener sólo sol-high"
        )
    for key, allowed in (
        ("toolsets", profile.toolsets),
        ("mcp_tools", profile.mcp_tools),
        ("communication", profile.communication),
    ):
        if not _subset(policy_set, key, allowed):
            return AgentPolicyDecision(
                "reject", f"{key}_not_allowlisted", f"{key} fuera del perfil firmado"
            )
    if policy_set.get("mount_profile", profile.mount_profiles[0]) not in profile.mount_profiles:
        return AgentPolicyDecision(
            "reject", "mount_not_allowlisted", "Perfil de mounts no allowlisted"
        )

    budget = policy_set.get("budget", {})
    limits = dict(profile.budget_limits)
    if not isinstance(budget, dict) or not set(budget).issubset(limits):
        return AgentPolicyDecision(
            "reject", "budget_not_allowlisted", "Presupuesto fuera del perfil firmado"
        )
    for key, value in budget.items():
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value > limits[key]
        ):
            return AgentPolicyDecision(
                "reject", "budget_limit_exceeded", f"Presupuesto excedido: {key}"
            )

    return AgentPolicyDecision(
        "approve",
        "profile_allowlisted",
        "Solicitud contenida en un perfil seguro versionado",
        profile.profile_id,
        digest,
    )


def process_agent_request(
    session: Session,
    *,
    request_id: uuid.UUID,
    settings: Settings,
    provisioner: AgentProvisioner,
) -> AgentPolicyResult:
    request = session.get(AgentRequestRecord, request_id)
    if request is None:
        return AgentPolicyResult(request_id, "noop", "missing", "request_not_found")
    actor_id = settings.agent_policy_actor_id
    if request.status in {"applied", "rejected", "retired"}:
        return AgentPolicyResult(request.id, "noop", request.status, "terminal", replayed=True)
    if request.status == "approved":
        if request.decided_by_actor_id != actor_id:
            return AgentPolicyResult(request.id, "defer", request.status, "external_approval_owner")
        decision = evaluate_agent_request(session, request, settings)
        if decision.action != "approve":
            return AgentPolicyResult(request.id, "defer", request.status, decision.code)
    elif request.status == "pending":
        decision = evaluate_agent_request(session, request, settings)
        if decision.action == "defer":
            return AgentPolicyResult(request.id, "defer", request.status, decision.code)
        if decision.action == "reject":
            resolved = decide_agent_request(
                session,
                request_id=request.id,
                actor_id=actor_id,
                idempotency_key=f"agent-policy-decision:{request.id}",
                decision="reject",
                reason=f"{decision.code}: {decision.reason}",
            )
            return AgentPolicyResult(request.id, "reject", resolved.request.status, decision.code)
        decide_agent_request(
            session,
            request_id=request.id,
            actor_id=actor_id,
            idempotency_key=f"agent-policy-decision:{request.id}",
            decision="approve",
            reason=(f"{decision.code}: {decision.profile_id}; digest={decision.profile_digest}"),
        )
    else:
        return AgentPolicyResult(request.id, "defer", request.status, "state_not_eligible")

    try:
        applied = provision_agent_request(
            session,
            provisioner=provisioner,
            request_id=request.id,
            actor_id=actor_id,
            idempotency_key=f"agent-policy-apply:{request.id}",
        )
    except ProvisioningError as error:
        return AgentPolicyResult(request.id, "apply", "apply_failed", error.code)
    return AgentPolicyResult(
        request.id,
        "apply",
        "applied",
        "profile_applied",
        replayed=applied.replayed,
        profile_digest=decision.profile_digest,
    )


class AgentPolicyCoordinator:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        settings: Settings,
        provisioner: AgentProvisioner,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings
        self.provisioner = provisioner

    def run_once(self, stop: StopFlag | threading.Event | None = None) -> list[AgentPolicyResult]:
        if not self.settings.agent_policy_enabled or (stop is not None and stop.is_set()):
            return []
        with self.session_factory() as session:
            request_ids = list(
                session.scalars(
                    select(AgentRequestRecord.id)
                    .where(
                        (AgentRequestRecord.status == "pending")
                        | (
                            (AgentRequestRecord.status == "approved")
                            & (
                                AgentRequestRecord.decided_by_actor_id
                                == self.settings.agent_policy_actor_id
                            )
                        )
                    )
                    .order_by(AgentRequestRecord.created_at, AgentRequestRecord.id)
                    .limit(self.settings.agent_policy_batch_size)
                )
            )
        results: list[AgentPolicyResult] = []
        for request_id in request_ids:
            with self.session_factory() as session:
                results.append(
                    process_agent_request(
                        session,
                        request_id=request_id,
                        settings=self.settings,
                        provisioner=self.provisioner,
                    )
                )
        return results

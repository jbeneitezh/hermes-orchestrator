from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from hermes_orchestrator.models import Agent, AgentInstance, AuditEvent, CommunicationEdge
from hermes_orchestrator.provisioning import (
    PROGRAM_ALLOWED_PROFILES,
    PROGRAM_EXECUTION_PROFILE,
    WORKER_API_SECRET_PREFIX,
    AgentProvisioner,
    ProvisionerResult,
    ProvisioningError,
    ProvisioningPayload,
)
from hermes_orchestrator.repositories import AgentRepository, AuditRepository
from hermes_orchestrator.services import (
    AgentRequestLifecycleError,
    IdempotencyConflictError,
    get_agent_request,
    record_agent_request_application,
    retire_agent_request,
)


@dataclass(frozen=True)
class AgentProvisioningResult:
    request_id: uuid.UUID
    provisioner: ProvisionerResult
    replayed: bool = False


def _ensure_communication_edge(
    session: Session,
    *,
    source: Agent,
    target: Agent,
    task_classes: list[str],
    scopes: list[str],
    approved_by_actor_id: str,
) -> None:
    edges = session.scalars(
        select(CommunicationEdge).where(
            CommunicationEdge.source_agent_id == source.id,
            CommunicationEdge.target_agent_id == target.id,
        )
    )
    if any(
        set(task_classes).issubset(edge.task_classes) and set(scopes).issubset(edge.scopes)
        for edge in edges
    ):
        return
    session.add(
        CommunicationEdge(
            source_agent_id=source.id,
            target_agent_id=target.id,
            task_classes=task_classes,
            scopes=scopes,
            approved_by_actor_id=approved_by_actor_id,
        )
    )


def _materialize_communication_policy(
    session: Session, *, agent: Agent, approved_by_actor_id: str
) -> None:
    """Proyecta las relaciones allowlisted sin conceder dispatch al especialista."""
    references = agent.policy_set.get("communication", [])
    if not isinstance(references, list):
        return
    for reference in references:
        if not isinstance(reference, str) or not reference.startswith("agent:"):
            continue
        target = session.scalar(
            select(Agent).where(
                Agent.slug == reference.removeprefix("agent:"),
                Agent.desired_state == "active",
            )
        )
        if target is None or target.id == agent.id:
            continue
        _ensure_communication_edge(
            session,
            source=agent,
            target=target,
            task_classes=["visibility"],
            scopes=["read"],
            approved_by_actor_id=approved_by_actor_id,
        )
        if target.role == "leader":
            _ensure_communication_edge(
                session,
                source=target,
                target=agent,
                task_classes=["visibility", "task"],
                scopes=["read", "dispatch"],
                approved_by_actor_id=approved_by_actor_id,
            )


def _payload(request_id: uuid.UUID, raw: dict[str, Any]) -> ProvisioningPayload:
    try:
        return ProvisioningPayload.model_validate({"request_id": request_id, **raw})
    except ValidationError as error:
        first = error.errors()[0]
        code = "slug_invalid" if "slug" in first.get("loc", ()) else "template_policy_denied"
        raise ProvisioningError(
            code, "Solicitud incompatible con la plantilla allowlisted"
        ) from error


def provision_agent_request(
    session: Session,
    *,
    provisioner: AgentProvisioner,
    request_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
) -> AgentProvisioningResult:
    request = get_agent_request(session, request_id)
    slug = str(request.payload.get("slug", ""))
    worker_secret_ref = f"{WORKER_API_SECRET_PREFIX}{slug}"
    if request.application_idempotency_key == idempotency_key and request.status == "applied":
        payload = _payload(request.id, request.payload)
        result = provisioner.apply(payload)
        agent = AgentRepository(session).get_by_slug(slug)
        if agent is None or not agent.instances:
            raise AgentRequestLifecycleError(
                "provisioning_state_missing", "Falta la instancia aplicada", 500
            )
        instance = agent.instances[-1]
        now = datetime.now(UTC)
        instance.health = result.health
        instance.config_revision = result.config_digest
        instance.last_heartbeat_at = now
        instance.reconciliation_state = "in_sync"
        if result.credential_sha256:
            agent.policy_set = agent.policy_set | {
                "runtime_auth_token_sha256": result.credential_sha256
            }
        agent.secret_refs = sorted(set([*agent.secret_refs, worker_secret_ref]))
        agent.updated_at = now
        _materialize_communication_policy(session, agent=agent, approved_by_actor_id=actor_id)
        session.commit()
        return AgentProvisioningResult(
            request_id=request.id,
            provisioner=result,
            replayed=True,
        )
    if request.status not in {"approved", "apply_failed"}:
        raise AgentRequestLifecycleError(
            "invalid_transition", "La solicitud no está aprobada para provisionar"
        )
    payload = _payload(request.id, request.payload)
    try:
        result = provisioner.apply(payload)
    except ProvisioningError as error:
        record_agent_request_application(
            session,
            request_id=request.id,
            actor_id=actor_id,
            idempotency_key=idempotency_key,
            outcome="failed",
            error_code=error.code,
            error_detail=error.detail,
        )
        raise
    if not result.credential_sha256:
        missing_credential_error = ProvisioningError(
            "runtime_credential_missing",
            "El provisioner no devolvió la huella de la credencial dinámica",
            503,
        )
        record_agent_request_application(
            session,
            request_id=request.id,
            actor_id=actor_id,
            idempotency_key=idempotency_key,
            outcome="failed",
            error_code=missing_credential_error.code,
            error_detail=missing_credential_error.detail,
        )
        raise missing_credential_error
    record_agent_request_application(
        session,
        request_id=request.id,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        outcome="applied",
    )

    now = datetime.now(UTC)
    agents = AgentRepository(session)
    agent = agents.get_by_slug(payload.slug)
    if agent is None:
        agent = Agent(
            slug=payload.slug,
            role=payload.role,
            description=payload.description,
            owner_actor_id=request.requested_by_actor_id,
        )
        session.add(agent)
        session.flush()
    agent.role = payload.role
    agent.description = payload.description
    agent.desired_state = "active"
    agent.policy_set = payload.policy_set | {
        "execution_profile_default": PROGRAM_EXECUTION_PROFILE,
        "allowed_profiles": PROGRAM_ALLOWED_PROFILES,
        "managed_by": "agent-provisioner",
        "request_id": str(request.id),
        "runtime_auth_token_sha256": result.credential_sha256,
    }
    agent.capabilities = payload.capabilities
    agent.secret_refs = sorted(set([*payload.secret_refs, worker_secret_ref]))
    agent.updated_at = now
    instance = agent.instances[-1] if agent.instances else AgentInstance(agent_id=agent.id)
    if not agent.instances:
        session.add(instance)
    instance.container_ref = f"hermes-tradix-f11-{result.service_name}-1"
    instance.hermes_version = "0.18.2"
    instance.internal_endpoint = f"http://{result.service_name}:8642"
    instance.config_revision = result.config_digest
    instance.health = result.health
    instance.last_heartbeat_at = now
    instance.reconciliation_state = "in_sync"
    _materialize_communication_policy(session, agent=agent, approved_by_actor_id=actor_id)
    AuditRepository(session).append(
        AuditEvent(
            actor_id=actor_id,
            event_type="agent.provisioned",
            aggregate_type="agent_request",
            aggregate_id=str(request.id),
            payload={
                "service_name": result.service_name,
                "config_digest": result.config_digest,
                "status": result.status,
            },
        )
    )
    session.commit()
    return AgentProvisioningResult(request.id, result)


def rollback_agent_request(
    session: Session,
    *,
    provisioner: AgentProvisioner,
    request_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
    reason: str,
) -> AgentProvisioningResult:
    request = get_agent_request(session, request_id)
    payload = _payload(request.id, request.payload)
    agent = AgentRepository(session).get_by_slug(payload.slug)
    if agent is None:
        raise AgentRequestLifecycleError("agent_not_found", "Agente aplicado no encontrado", 404)
    previous_key = str(agent.policy_set.get("rollback_idempotency_key", ""))
    if previous_key:
        if previous_key != idempotency_key:
            raise IdempotencyConflictError
        return AgentProvisioningResult(
            request.id,
            ProvisionerResult(
                status="rolled_back",
                service_name=f"worker-{payload.slug}",
                config_digest=str(agent.policy_set.get("rollback_config_digest", "unknown")),
                health="stopped",
            ),
            replayed=True,
        )
    if request.status != "applied" or agent.desired_state != "active":
        raise AgentRequestLifecycleError(
            "invalid_transition", "La solicitud no tiene un worker activo que revertir"
        )
    result = provisioner.rollback(payload)
    agent.desired_state = "disabled"
    policy_set = dict(agent.policy_set)
    credential_hash = str(policy_set.pop("runtime_auth_token_sha256", ""))
    agent.policy_set = policy_set | {
        "rollback_idempotency_key": idempotency_key,
        "rollback_config_digest": result.config_digest,
        "revoked_runtime_auth_token_sha256": credential_hash,
    }
    for instance in agent.instances:
        instance.health = "stopped"
        instance.reconciliation_state = "rolled_back"
    AuditRepository(session).append(
        AuditEvent(
            actor_id=actor_id,
            event_type="agent.provisioning_rolled_back",
            aggregate_type="agent_request",
            aggregate_id=str(request.id),
            payload={
                "service_name": result.service_name,
                "config_digest": result.config_digest,
                "idempotency_key": idempotency_key,
            },
        )
    )
    retire_agent_request(
        session,
        request_id=request.id,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        reason=reason,
    )
    return AgentProvisioningResult(request.id, result)

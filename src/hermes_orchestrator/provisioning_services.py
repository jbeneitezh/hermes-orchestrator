from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError
from sqlalchemy.orm import Session

from hermes_orchestrator.models import Agent, AgentInstance, AuditEvent
from hermes_orchestrator.provisioning import (
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


def _payload(request_id: uuid.UUID, raw: dict[str, Any]) -> ProvisioningPayload:
    try:
        return ProvisioningPayload.model_validate({"request_id": request_id, **raw})
    except ValidationError as error:
        first = error.errors()[0]
        code = "slug_invalid" if "slug" in first.get("loc", ()) else "template_policy_denied"
        raise ProvisioningError(
            code, "Solicitud incompatible con la plantilla allowlisted"
        ) from error


def _existing_result(session: Session, request_id: uuid.UUID, slug: str) -> ProvisionerResult:
    agent = AgentRepository(session).get_by_slug(slug)
    if agent is None or not agent.instances:
        raise AgentRequestLifecycleError(
            "provisioning_state_missing", "Falta la instancia aplicada", 500
        )
    instance = agent.instances[-1]
    return ProvisionerResult(
        status="no_change",
        service_name=f"worker-{slug}",
        config_digest=instance.config_revision or "unknown",
        health=instance.health,
        credential_sha256=str(agent.policy_set.get("runtime_auth_token_sha256", "")) or None,
    )


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
    if request.application_idempotency_key == idempotency_key and request.status == "applied":
        return AgentProvisioningResult(
            request_id=request.id,
            provisioner=_existing_result(session, request.id, slug),
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
        "managed_by": "agent-provisioner",
        "request_id": str(request.id),
        "runtime_auth_token_sha256": result.credential_sha256,
    }
    agent.capabilities = payload.capabilities
    agent.secret_refs = payload.secret_refs
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

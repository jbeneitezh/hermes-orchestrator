from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from hermes_orchestrator.models import AgentRequestRecord, AuditEvent
from hermes_orchestrator.repositories import AgentRequestRepository, AuditRepository


class IdempotencyConflictError(Exception):
    pass


class AgentRequestLifecycleError(Exception):
    def __init__(self, code: str, detail: str, status_code: int = 409) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code


@dataclass(frozen=True)
class RequestAgentResult:
    request: AgentRequestRecord
    replayed: bool


def _hash(payload: dict[str, Any]) -> str:
    canonical_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_payload.encode()).hexdigest()


def _append_transition_audit(
    session: Session,
    request: AgentRequestRecord,
    *,
    actor_id: str,
    event_type: str,
    previous_status: str,
    idempotency_key: str,
    extra: dict[str, Any] | None = None,
) -> None:
    AuditRepository(session).append(
        AuditEvent(
            actor_id=actor_id,
            event_type=event_type,
            aggregate_type="agent_request",
            aggregate_id=str(request.id),
            payload={
                "idempotency_key": idempotency_key,
                "previous_status": previous_status,
                "status": request.status,
                **(extra or {}),
            },
        )
    )


def request_agent(
    session: Session,
    *,
    actor_id: str,
    idempotency_key: str,
    payload: dict[str, Any],
) -> RequestAgentResult:
    request_hash = _hash(payload)
    requests = AgentRequestRepository(session)
    existing = requests.get_by_idempotency_key(idempotency_key)
    if existing is not None:
        if existing.request_hash != request_hash or existing.requested_by_actor_id != actor_id:
            raise IdempotencyConflictError
        return RequestAgentResult(request=existing, replayed=True)

    record = AgentRequestRecord(
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        requested_by_actor_id=actor_id,
        payload=payload,
    )
    requests.add(record)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        concurrent = requests.get_by_idempotency_key(idempotency_key)
        if (
            concurrent is None
            or concurrent.request_hash != request_hash
            or concurrent.requested_by_actor_id != actor_id
        ):
            raise IdempotencyConflictError from None
        return RequestAgentResult(request=concurrent, replayed=True)
    AuditRepository(session).append(
        AuditEvent(
            actor_id=actor_id,
            event_type="agent.requested",
            aggregate_type="agent_request",
            aggregate_id=str(record.id),
            payload={"idempotency_key": idempotency_key, "request_hash": request_hash},
        )
    )
    session.commit()
    return RequestAgentResult(request=record, replayed=False)


def get_agent_request(session: Session, request_id: uuid.UUID) -> AgentRequestRecord:
    request = AgentRequestRepository(session).get(request_id)
    if request is None:
        raise AgentRequestLifecycleError("not_found", "Solicitud de agente no encontrada", 404)
    return request


def decide_agent_request(
    session: Session,
    *,
    request_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
    decision: Literal["approve", "reject"],
    reason: str,
) -> RequestAgentResult:
    request = get_agent_request(session, request_id)
    command_hash = _hash({"decision": decision, "reason": reason})
    if request.decision_idempotency_key == idempotency_key:
        if request.decision_hash != command_hash or request.decided_by_actor_id != actor_id:
            raise IdempotencyConflictError
        return RequestAgentResult(request=request, replayed=True)
    if request.status != "pending":
        raise AgentRequestLifecycleError(
            "invalid_transition", "La solicitud ya no admite una decisión"
        )
    if decision == "approve" and actor_id == request.requested_by_actor_id:
        raise AgentRequestLifecycleError(
            "self_escalation_denied",
            "El solicitante no puede aprobar su propia elevación de capacidades",
            403,
        )

    previous_status = request.status
    now = datetime.now(UTC)
    request.status = "approved" if decision == "approve" else "rejected"
    request.decision_idempotency_key = idempotency_key
    request.decision_hash = command_hash
    request.decided_by_actor_id = actor_id
    request.decision_reason = reason
    request.decided_at = now
    request.updated_at = now
    _append_transition_audit(
        session,
        request,
        actor_id=actor_id,
        event_type=f"agent.request_{request.status}",
        previous_status=previous_status,
        idempotency_key=idempotency_key,
        extra={"decision": decision, "reason": reason},
    )
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise IdempotencyConflictError from exc
    return RequestAgentResult(request=request, replayed=False)


def record_agent_request_application(
    session: Session,
    *,
    request_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
    outcome: Literal["applied", "failed"],
    error_code: str | None = None,
    error_detail: str | None = None,
) -> RequestAgentResult:
    """Punto de integración interno para el provisionador de F15."""
    request = get_agent_request(session, request_id)
    command = {
        "outcome": outcome,
        "error_code": error_code,
        "error_detail": error_detail,
    }
    command_hash = _hash(command)
    if request.application_idempotency_key == idempotency_key:
        if request.application_hash != command_hash or request.applied_by_actor_id != actor_id:
            raise IdempotencyConflictError
        return RequestAgentResult(request=request, replayed=True)
    if request.status not in {"approved", "apply_failed"}:
        raise AgentRequestLifecycleError(
            "invalid_transition", "La solicitud no admite aplicación en su estado actual"
        )
    if outcome == "failed" and not error_code:
        raise AgentRequestLifecycleError(
            "application_error_required", "Una aplicación fallida exige un código de error", 422
        )

    previous_status = request.status
    now = datetime.now(UTC)
    request.status = "applied" if outcome == "applied" else "apply_failed"
    request.application_idempotency_key = idempotency_key
    request.application_hash = command_hash
    request.applied_by_actor_id = actor_id
    request.applied_at = now if outcome == "applied" else None
    request.application_error_code = error_code if outcome == "failed" else None
    request.application_error_detail = error_detail if outcome == "failed" else None
    request.updated_at = now
    _append_transition_audit(
        session,
        request,
        actor_id=actor_id,
        event_type=(
            "agent.request_applied" if outcome == "applied" else "agent.request_apply_failed"
        ),
        previous_status=previous_status,
        idempotency_key=idempotency_key,
        extra={"error_code": request.application_error_code},
    )
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise IdempotencyConflictError from exc
    return RequestAgentResult(request=request, replayed=False)


def retire_agent_request(
    session: Session,
    *,
    request_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
    reason: str,
) -> RequestAgentResult:
    request = get_agent_request(session, request_id)
    command_hash = _hash({"reason": reason})
    if request.retirement_idempotency_key == idempotency_key:
        if request.retirement_hash != command_hash or request.retired_by_actor_id != actor_id:
            raise IdempotencyConflictError
        return RequestAgentResult(request=request, replayed=True)
    if request.status not in {"applied", "apply_failed"}:
        raise AgentRequestLifecycleError(
            "invalid_transition", "Solo una solicitud aplicada o fallida puede retirarse"
        )

    previous_status = request.status
    now = datetime.now(UTC)
    request.status = "retired"
    request.retirement_idempotency_key = idempotency_key
    request.retirement_hash = command_hash
    request.retired_by_actor_id = actor_id
    request.retirement_reason = reason
    request.retired_at = now
    request.updated_at = now
    _append_transition_audit(
        session,
        request,
        actor_id=actor_id,
        event_type="agent.request_retired",
        previous_status=previous_status,
        idempotency_key=idempotency_key,
        extra={"reason": reason},
    )
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise IdempotencyConflictError from exc
    return RequestAgentResult(request=request, replayed=False)

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from hermes_orchestrator.models import AgentRequestRecord, AuditEvent
from hermes_orchestrator.repositories import AgentRequestRepository, AuditRepository


class IdempotencyConflictError(Exception):
    pass


@dataclass(frozen=True)
class RequestAgentResult:
    request: AgentRequestRecord
    replayed: bool


def request_agent(
    session: Session,
    *,
    actor_id: str,
    idempotency_key: str,
    payload: dict[str, Any],
) -> RequestAgentResult:
    canonical_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    request_hash = hashlib.sha256(canonical_payload.encode()).hexdigest()
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

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import (
    RunEvent,
    UsageLedger,
    WorkflowContinuation,
)
from hermes_orchestrator.task_services import append_audit

CONTINUATION_STATUSES = {"pending", "dispatched", "failed"}
CONTINUATION_TRANSITIONS = {"pending": {"dispatched", "failed"}, "dispatched": {"failed"}}


class ContinuationError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class ContinuationResult:
    continuation: WorkflowContinuation
    created: bool


def utc_now() -> datetime:
    return datetime.now(UTC)


def _idempotency_key(event_id: uuid.UUID, target_actor_id: str, action: str) -> str:
    value = f"{event_id}:{target_actor_id}:{action}".encode()
    return f"workflow:{hashlib.sha256(value).hexdigest()}"


def _audit_rejection(
    session: Session,
    *,
    event: RunEvent,
    actor_id: str,
    target_actor_id: str,
    action: str,
    reason: str,
) -> None:
    append_audit(
        session,
        actor_id=actor_id,
        event_type="workflow.continuation_skipped",
        aggregate_type="run_event",
        aggregate_id=event.id,
        payload={"target_actor_id": target_actor_id, "action": action, "reason": reason},
    )
    session.commit()


def _effective_max_fan_out(event: RunEvent, settings: Settings) -> int:
    configured = event.run.task.budget.get("max_fan_out")
    if isinstance(configured, int) and not isinstance(configured, bool):
        return max(0, min(settings.usage_max_fan_out, configured))
    return settings.usage_max_fan_out


def _usage_snapshot(session: Session, event: RunEvent) -> dict[str, Any]:
    ledger = session.scalar(select(UsageLedger).where(UsageLedger.run_id == event.run_id))
    if ledger is None:
        usage = event.run.usage_snapshot
        return {
            key: usage[key]
            for key in ("input_tokens", "output_tokens", "reasoning_tokens", "api_calls")
            if key in usage
        }
    return {
        "input_tokens": ledger.input_tokens,
        "output_tokens": ledger.output_tokens,
        "reasoning_tokens": ledger.reasoning_tokens,
        "api_calls": ledger.api_calls,
        "cost_status": ledger.cost_status,
        "outcome": ledger.outcome,
    }


def _context_snapshot(
    session: Session,
    event: RunEvent,
    *,
    used_continuations: int,
    max_fan_out: int,
) -> dict[str, Any]:
    run = event.run
    task = run.task
    return {
        "schema_version": 1,
        "operation_id": str(task.operation_id),
        "parent_task": {
            "id": str(task.id),
            "objective": task.objective[:2000],
            "status": task.status,
            "workflow_ref": task.workflow_ref,
        },
        "trigger": {
            "event_id": str(event.id),
            "event_type": event.event_type,
            "run_id": str(run.id),
            "run_status": run.status,
        },
        "result": {
            "summary": run.summary[:4000] if run.summary else None,
            "error_code": run.error_code,
            "artifacts": [
                {
                    "kind": artifact.kind,
                    "uri": artifact.uri,
                    "sha256": artifact.sha256,
                    "sensitivity": artifact.sensitivity,
                }
                for artifact in run.artifacts[:20]
            ],
            "approvals": [
                {"action": approval.action, "status": approval.status}
                for approval in run.approvals[:20]
            ],
            "usage": _usage_snapshot(session, event),
        },
        "budget": {
            "max_fan_out": max_fan_out,
            "used_continuations": used_continuations,
            "remaining_continuations": max(0, max_fan_out - used_continuations),
        },
    }


def create_workflow_continuation(
    session: Session,
    *,
    trigger_event_id: uuid.UUID,
    target_actor_id: str,
    action: str,
    actor_id: str = "system:workflow",
    settings: Settings | None = None,
) -> ContinuationResult:
    event = session.get(RunEvent, trigger_event_id)
    if event is None:
        raise ContinuationError("event_not_found", "El evento disparador no existe")
    if not event.terminal:
        _audit_rejection(
            session,
            event=event,
            actor_id=actor_id,
            target_actor_id=target_actor_id,
            action=action,
            reason="event_not_terminal",
        )
        raise ContinuationError("event_not_terminal", "El evento aún no es terminal")

    run = event.run
    task = run.task
    if (
        task.status == "cancelled"
        or run.status == "cancelled"
        or event.event_type == "run.cancelled"
    ):
        _audit_rejection(
            session,
            event=event,
            actor_id=actor_id,
            target_actor_id=target_actor_id,
            action=action,
            reason="workflow_cancelled",
        )
        raise ContinuationError("workflow_cancelled", "El workflow está cancelado")

    existing = session.scalar(
        select(WorkflowContinuation).where(
            WorkflowContinuation.trigger_event_id == event.id,
            WorkflowContinuation.target_actor_id == target_actor_id,
            WorkflowContinuation.action == action,
        )
    )
    if existing is not None:
        append_audit(
            session,
            actor_id=actor_id,
            event_type="workflow.continuation_replayed",
            aggregate_type="workflow_continuation",
            aggregate_id=existing.id,
            payload={"trigger_event_id": str(event.id)},
        )
        session.commit()
        return ContinuationResult(existing, created=False)

    effective_settings = settings or Settings()
    max_fan_out = _effective_max_fan_out(event, effective_settings)
    used = (
        session.scalar(
            select(func.count(WorkflowContinuation.id)).where(
                WorkflowContinuation.parent_task_id == task.id,
                WorkflowContinuation.status != "failed",
            )
        )
        or 0
    )
    if used >= max_fan_out:
        _audit_rejection(
            session,
            event=event,
            actor_id=actor_id,
            target_actor_id=target_actor_id,
            action=action,
            reason="workflow_budget_exhausted",
        )
        raise ContinuationError("workflow_budget_exhausted", "El fan-out del workflow está agotado")

    continuation = WorkflowContinuation(
        operation_id=task.operation_id,
        parent_task_id=task.id,
        trigger_event_id=event.id,
        target_actor_id=target_actor_id,
        action=action,
        status="pending",
        idempotency_key=_idempotency_key(event.id, target_actor_id, action),
        context_snapshot=_context_snapshot(
            session,
            event,
            used_continuations=used + 1,
            max_fan_out=max_fan_out,
        ),
    )
    try:
        with session.begin_nested():
            session.add(continuation)
            session.flush()
    except IntegrityError:
        concurrent = session.scalar(
            select(WorkflowContinuation).where(
                WorkflowContinuation.trigger_event_id == event.id,
                WorkflowContinuation.target_actor_id == target_actor_id,
                WorkflowContinuation.action == action,
            )
        )
        if concurrent is None:
            raise
        append_audit(
            session,
            actor_id=actor_id,
            event_type="workflow.continuation_replayed",
            aggregate_type="workflow_continuation",
            aggregate_id=concurrent.id,
            payload={"trigger_event_id": str(event.id), "race": True},
        )
        session.commit()
        return ContinuationResult(concurrent, created=False)

    append_audit(
        session,
        actor_id=actor_id,
        event_type="workflow.continuation_created",
        aggregate_type="workflow_continuation",
        aggregate_id=continuation.id,
        payload={
            "trigger_event_id": str(event.id),
            "target_actor_id": target_actor_id,
            "action": action,
        },
    )
    session.commit()
    return ContinuationResult(continuation, created=True)


def transition_workflow_continuation(
    session: Session,
    *,
    continuation_id: uuid.UUID,
    new_status: str,
    actor_id: str,
    failure_code: str | None = None,
    now: datetime | None = None,
) -> WorkflowContinuation:
    continuation = session.get(WorkflowContinuation, continuation_id)
    if continuation is None:
        raise ContinuationError("continuation_not_found", "La continuación no existe")
    if new_status not in CONTINUATION_STATUSES:
        raise ContinuationError("invalid_continuation_status", "Estado de continuación inválido")
    if new_status not in CONTINUATION_TRANSITIONS.get(continuation.status, set()):
        raise ContinuationError("invalid_continuation_transition", "Transición no permitida")

    changed_at = now or utc_now()
    continuation.status = new_status
    if new_status == "dispatched":
        continuation.dispatched_at = changed_at
    if new_status == "failed":
        continuation.failed_at = changed_at
        continuation.failure_code = failure_code or "dispatch_failed"
    append_audit(
        session,
        actor_id=actor_id,
        event_type=f"workflow.continuation_{new_status}",
        aggregate_type="workflow_continuation",
        aggregate_id=continuation.id,
        payload={"failure_code": continuation.failure_code} if new_status == "failed" else {},
    )
    session.commit()
    return continuation

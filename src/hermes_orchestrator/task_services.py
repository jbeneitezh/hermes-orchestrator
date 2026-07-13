from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session, selectinload

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import (
    Approval,
    AuditEvent,
    Run,
    Task,
    TaskComment,
    TaskDependency,
)
from hermes_orchestrator.usage_services import (
    ControlViolation,
    enforce_dispatch_controls,
    ingest_run_usage,
    record_run_outcome,
)


class LifecycleError(Exception):
    def __init__(
        self,
        code: str,
        detail: str,
        status_code: int = 409,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code
        self.retry_after = retry_after


@dataclass(frozen=True)
class LifecycleResult:
    value: Task | Run | Approval | TaskComment
    replayed: bool = False


ACTIVE_RUN_STATUSES = {"dispatching", "running", "awaiting_approval"}
TERMINAL_RUN_STATUSES = {"completed", "failed", "cancelled", "timed_out"}
RUN_TRANSITIONS: dict[str, set[str]] = {
    "dispatching": {"running", "failed", "cancelled", "timed_out"},
    "running": {"awaiting_approval", "completed", "failed", "cancelled", "timed_out"},
    "awaiting_approval": {"dispatching", "failed", "cancelled", "timed_out"},
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def stable_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def append_audit(
    session: Session,
    *,
    actor_id: str,
    event_type: str,
    aggregate_type: str,
    aggregate_id: uuid.UUID,
    payload: dict[str, Any] | None = None,
) -> None:
    session.add(
        AuditEvent(
            actor_id=actor_id,
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=str(aggregate_id),
            payload=payload or {},
        )
    )


def task_query(task_id: uuid.UUID) -> Select[tuple[Task]]:
    return (
        select(Task)
        .where(Task.id == task_id)
        .options(
            selectinload(Task.runs).selectinload(Run.approvals),
            selectinload(Task.comments),
            selectinload(Task.dependency_links),
        )
    )


def get_task(session: Session, task_id: uuid.UUID) -> Task:
    task = session.scalar(task_query(task_id))
    if task is None:
        raise LifecycleError("not_found", "Tarea no encontrada", 404)
    return task


def get_run(session: Session, run_id: uuid.UUID) -> Run:
    run = session.scalar(select(Run).where(Run.id == run_id).options(selectinload(Run.approvals)))
    if run is None:
        raise LifecycleError("not_found", "Run no encontrado", 404)
    return run


def create_task(
    session: Session,
    *,
    actor_id: str,
    idempotency_key: str,
    payload: dict[str, Any],
) -> LifecycleResult:
    request_hash = stable_hash(payload)
    existing = session.scalar(select(Task).where(Task.idempotency_key == idempotency_key))
    if existing is not None:
        if existing.request_hash != request_hash or existing.requester_actor_id != actor_id:
            raise LifecycleError("idempotency_conflict", "La clave ya se usó con otra tarea")
        return LifecycleResult(get_task(session, existing.id), replayed=True)

    dependency_ids = payload.pop("dependency_ids", [])
    if dependency_ids:
        found_ids = set(session.scalars(select(Task.id).where(Task.id.in_(dependency_ids))).all())
        if found_ids != set(dependency_ids):
            raise LifecycleError("dependency_not_found", "Alguna dependencia no existe")

    task = Task(
        requester_actor_id=actor_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        **payload,
    )
    session.add(task)
    session.flush()
    session.add_all(
        [TaskDependency(task_id=task.id, depends_on_task_id=value) for value in dependency_ids]
    )
    append_audit(
        session,
        actor_id=actor_id,
        event_type="task.created",
        aggregate_type="task",
        aggregate_id=task.id,
        payload={"idempotency_key": idempotency_key},
    )
    session.commit()
    return LifecycleResult(get_task(session, task.id))


def unmet_dependencies(session: Session, task: Task) -> list[uuid.UUID]:
    unmet: list[uuid.UUID] = []
    for link in task.dependency_links:
        dependency_status = session.scalar(
            select(Task.status).where(Task.id == link.depends_on_task_id)
        )
        if dependency_status != link.required_status:
            unmet.append(link.depends_on_task_id)
    return unmet


def dispatch_task(
    session: Session,
    *,
    task_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
    payload: dict[str, Any],
    settings: Settings | None = None,
    now: datetime | None = None,
) -> LifecycleResult:
    effective_now = now or utc_now()
    dispatch_hash = stable_hash(payload)
    existing = session.scalar(select(Run).where(Run.dispatch_idempotency_key == idempotency_key))
    if existing is not None:
        if existing.task_id != task_id or existing.dispatch_hash != dispatch_hash:
            raise LifecycleError("idempotency_conflict", "La clave ya se usó con otro dispatch")
        return LifecycleResult(get_run(session, existing.id), replayed=True)

    task = get_task(session, task_id)
    if task.status not in {"pending", "blocked", "failed", "timed_out"}:
        raise LifecycleError("invalid_transition", "La tarea no admite dispatch")
    if task.independent_review and task.reviewer_actor_id == payload["worker_actor_id"]:
        raise LifecycleError("self_review_denied", "El ejecutor no puede ser reviewer", 403)
    unmet = unmet_dependencies(session, task)
    if unmet:
        task.status = "blocked"
        session.commit()
        raise LifecycleError(
            "dependency_unmet", f"Dependencias pendientes: {','.join(map(str, unmet))}"
        )
    active = session.scalar(
        select(func.count())
        .select_from(Run)
        .where(Run.task_id == task.id, Run.status.in_(ACTIVE_RUN_STATUSES))
    )
    if active:
        raise LifecycleError("run_already_active", "La tarea ya tiene un run activo")

    try:
        enforce_dispatch_controls(
            session,
            task=task,
            worker_actor_id=payload["worker_actor_id"],
            requested_profile_id=payload["requested_profile_id"],
            actor_id=actor_id,
            settings=settings or Settings(),
            now=effective_now,
        )
    except ControlViolation as error:
        raise LifecycleError(
            error.code,
            error.detail,
            error.status_code,
            error.retry_after,
        ) from error

    attempt = (
        session.scalar(select(func.max(Run.attempt_number)).where(Run.task_id == task.id)) or 0
    ) + 1
    timeout_at = effective_now + timedelta(seconds=payload["timeout_seconds"])
    if task.deadline_at is not None:
        timeout_at = min(timeout_at, ensure_aware(task.deadline_at))
    requires_approval = payload["requires_approval"]
    run = Run(
        task_id=task.id,
        operation_id=task.operation_id,
        attempt_number=attempt,
        worker_actor_id=payload["worker_actor_id"],
        requested_profile_id=payload["requested_profile_id"],
        dispatch_idempotency_key=idempotency_key,
        dispatch_hash=dispatch_hash,
        status="awaiting_approval" if requires_approval else "dispatching",
        timeout_at=timeout_at,
    )
    session.add(run)
    session.flush()
    if requires_approval:
        session.add(
            Approval(
                run_id=run.id,
                action=payload["approval_action"],
                context_snapshot={
                    "task_id": str(task.id),
                    "worker_actor_id": run.worker_actor_id,
                    "requested_profile_id": run.requested_profile_id,
                },
                requested_by_actor_id=actor_id,
                expires_at=effective_now + timedelta(seconds=payload["approval_ttl_seconds"]),
            )
        )
        task.status = "awaiting_approval"
    else:
        task.status = "dispatched"
    append_audit(
        session,
        actor_id=actor_id,
        event_type="task.dispatched",
        aggregate_type="run",
        aggregate_id=run.id,
        payload={"task_id": str(task.id), "attempt": attempt},
    )
    session.commit()
    return LifecycleResult(get_run(session, run.id))


def transition_run(
    session: Session,
    *,
    run_id: uuid.UUID,
    new_status: str,
    actor_id: str,
    error_code: str | None = None,
    summary: str | None = None,
    now: datetime | None = None,
    settings: Settings | None = None,
) -> Run:
    effective_now = now or utc_now()
    run = get_run(session, run_id)
    if new_status not in RUN_TRANSITIONS.get(run.status, set()):
        raise LifecycleError(
            "invalid_transition", f"Transición {run.status} -> {new_status} no permitida"
        )
    run.status = new_status
    run.error_code = error_code
    run.summary = summary
    if new_status == "running" and run.started_at is None:
        run.started_at = effective_now
    if new_status in TERMINAL_RUN_STATUSES:
        run.finished_at = effective_now
    task = get_task(session, run.task_id)
    task.status = {
        "dispatching": "dispatched",
        "running": "in_progress",
        "awaiting_approval": "awaiting_approval",
        "completed": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
        "timed_out": "timed_out",
    }[new_status]
    append_audit(
        session,
        actor_id=actor_id,
        event_type=f"run.{new_status}",
        aggregate_type="run",
        aggregate_id=run.id,
        payload={"task_id": str(task.id), "error_code": error_code},
    )
    if new_status in {"completed", "failed", "timed_out"}:
        record_run_outcome(
            session,
            run,
            settings=settings or Settings(),
            actor_id=actor_id,
            now=effective_now,
        )
    if new_status in TERMINAL_RUN_STATUSES:
        ingest_run_usage(
            session,
            run,
            settings=settings or Settings(),
            actor_id=actor_id,
        )
    session.commit()
    return get_run(session, run.id)


def add_comment(
    session: Session,
    *,
    task_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
    body: str,
) -> LifecycleResult:
    existing = session.scalar(
        select(TaskComment).where(TaskComment.idempotency_key == idempotency_key)
    )
    if existing is not None:
        if existing.task_id != task_id or existing.actor_id != actor_id or existing.body != body:
            raise LifecycleError("idempotency_conflict", "La clave ya se usó con otro comentario")
        return LifecycleResult(existing, replayed=True)
    get_task(session, task_id)
    comment = TaskComment(
        task_id=task_id,
        actor_id=actor_id,
        body=body,
        idempotency_key=idempotency_key,
    )
    session.add(comment)
    session.flush()
    append_audit(
        session,
        actor_id=actor_id,
        event_type="task.commented",
        aggregate_type="task",
        aggregate_id=task_id,
        payload={"comment_id": str(comment.id)},
    )
    session.commit()
    return LifecycleResult(comment)


def cancel_task(
    session: Session,
    *,
    task_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
    reason: str,
    now: datetime | None = None,
    settings: Settings | None = None,
) -> LifecycleResult:
    effective_now = now or utc_now()
    task = get_task(session, task_id)
    if task.cancel_idempotency_key is not None:
        if task.cancel_idempotency_key != idempotency_key:
            raise LifecycleError("already_cancelled", "La tarea ya está cancelada")
        return LifecycleResult(task, replayed=True)
    if task.status == "completed":
        raise LifecycleError("invalid_transition", "Una tarea completada no puede cancelarse")
    task.cancel_idempotency_key = idempotency_key
    task.status = "cancelled"
    run = session.scalar(
        select(Run)
        .where(Run.task_id == task.id, Run.status.in_(ACTIVE_RUN_STATUSES))
        .order_by(Run.attempt_number.desc())
    )
    if run is not None:
        run.status = "cancelled"
        run.error_code = "cancelled"
        run.summary = reason
        run.finished_at = effective_now
        ingest_run_usage(
            session,
            run,
            settings=settings or Settings(),
            actor_id=actor_id,
        )
    append_audit(
        session,
        actor_id=actor_id,
        event_type="task.cancelled",
        aggregate_type="task",
        aggregate_id=task.id,
        payload={"reason": reason},
    )
    session.commit()
    return LifecycleResult(get_task(session, task.id))


def resolve_approval(
    session: Session,
    *,
    run_id: uuid.UUID,
    actor_id: str,
    idempotency_key: str,
    decision: str,
    reason: str,
    now: datetime | None = None,
    settings: Settings | None = None,
) -> LifecycleResult:
    effective_now = now or utc_now()
    run = get_run(session, run_id)
    task = get_task(session, run.task_id)
    approval = session.scalar(
        select(Approval).where(Approval.run_id == run.id).order_by(Approval.created_at.desc())
    )
    if approval is None:
        raise LifecycleError("approval_not_found", "El run no tiene approval pendiente", 404)
    if approval.decision_idempotency_key is not None:
        if (
            approval.decision_idempotency_key == idempotency_key
            and approval.status == decision
            and approval.decided_by_actor_id == actor_id
            and approval.decision_reason == reason
        ):
            return LifecycleResult(approval, replayed=True)
        raise LifecycleError("idempotency_conflict", "La approval ya se resolvió con otra decisión")
    if actor_id == run.worker_actor_id:
        raise LifecycleError("self_review_denied", "El ejecutor no puede aprobar su run", 403)
    if task.independent_review and actor_id != task.reviewer_actor_id:
        raise LifecycleError("reviewer_mismatch", "Solo el reviewer asignado puede decidir", 403)
    if ensure_aware(approval.expires_at) <= effective_now:
        approval.status = "expired"
        run.status = "failed"
        run.error_code = "approval_expired"
        run.finished_at = effective_now
        task.status = "failed"
        ingest_run_usage(
            session,
            run,
            settings=settings or Settings(),
            actor_id="system:watchdog",
        )
        append_audit(
            session,
            actor_id="system:watchdog",
            event_type="approval.expired",
            aggregate_type="approval",
            aggregate_id=approval.id,
            payload={"run_id": str(run.id)},
        )
        session.commit()
        raise LifecycleError("approval_expired", "La approval ha caducado")

    approval.status = decision
    approval.decided_by_actor_id = actor_id
    approval.decision_reason = reason
    approval.decision_idempotency_key = idempotency_key
    approval.decided_at = effective_now
    if decision == "approved":
        run.status = "dispatching"
        task.status = "dispatched"
    else:
        run.status = "failed"
        run.error_code = "approval_rejected"
        run.finished_at = effective_now
        task.status = "failed"
        ingest_run_usage(
            session,
            run,
            settings=settings or Settings(),
            actor_id=actor_id,
        )
    append_audit(
        session,
        actor_id=actor_id,
        event_type=f"approval.{decision}",
        aggregate_type="approval",
        aggregate_id=approval.id,
        payload={"run_id": str(run.id)},
    )
    session.commit()
    return LifecycleResult(approval)


def expire_due_runs(
    session: Session,
    *,
    actor_id: str = "system:watchdog",
    now: datetime | None = None,
    settings: Settings | None = None,
) -> list[uuid.UUID]:
    effective_now = now or utc_now()
    due_runs = list(
        session.scalars(
            select(Run).where(Run.status.in_(ACTIVE_RUN_STATUSES), Run.timeout_at <= effective_now)
        )
    )
    expired_ids: list[uuid.UUID] = []
    for run in due_runs:
        run.status = "timed_out"
        run.error_code = "timeout"
        run.finished_at = effective_now
        task = get_task(session, run.task_id)
        task.status = "timed_out"
        append_audit(
            session,
            actor_id=actor_id,
            event_type="run.timed_out",
            aggregate_type="run",
            aggregate_id=run.id,
        )
        record_run_outcome(
            session,
            run,
            settings=settings or Settings(),
            actor_id=actor_id,
            now=effective_now,
        )
        ingest_run_usage(
            session,
            run,
            settings=settings or Settings(),
            actor_id=actor_id,
        )
        expired_ids.append(run.id)
    session.commit()
    return expired_ids

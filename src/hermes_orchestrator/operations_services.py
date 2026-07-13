from __future__ import annotations

import base64
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from hermes_orchestrator.config import Settings
from hermes_orchestrator.fleet_runner import FleetRunner
from hermes_orchestrator.models import (
    AgentRequestRecord,
    Approval,
    AuditEvent,
    Run,
    RunEvent,
    Task,
    WorkflowContinuation,
)
from hermes_orchestrator.usage_services import control_status, ensure_aware, summarize_usage

ACTIVE_TASK_STATUSES = {
    "pending",
    "ready",
    "dispatching",
    "running",
    "awaiting_approval",
    "blocked",
}
ACTIONABLE_TASK_STATUSES = ACTIVE_TASK_STATUSES - {"blocked"}
ACTIVE_RUN_STATUSES = {"dispatching", "running", "awaiting_approval"}


class OperationsReadError(Exception):
    def __init__(self, code: str, detail: str, status_code: int) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code


def utc_now() -> datetime:
    return datetime.now(UTC)


def fleet_view(runner: FleetRunner) -> dict[str, Any]:
    try:
        observed = runner.status()
    except Exception as error:
        raise OperationsReadError(
            "fleet_runner_unavailable", "Fleet reconciler no disponible", 503
        ) from error
    services = list(observed.get("services", []))
    unhealthy = [
        item
        for item in services
        if item.get("state") != "running" or item.get("health") not in {"healthy", None, ""}
    ]
    return {
        "status": "ready" if not unhealthy else "degraded",
        "compose_digest": observed.get("compose_digest"),
        "services": services,
        "service_count": len(services),
        "unhealthy_count": len(unhealthy),
        "observed_at": utc_now(),
    }


def _latest_activity(session: Session, task: Task) -> datetime:
    event_at = session.scalar(
        select(RunEvent.created_at)
        .join(Run, Run.id == RunEvent.run_id)
        .where(Run.task_id == task.id)
        .order_by(RunEvent.created_at.desc())
        .limit(1)
    )
    return ensure_aware(event_at or task.updated_at)


def task_view(
    session: Session,
    *,
    status: str | None = None,
    assignee: str | None = None,
    operation_id: uuid.UUID | None = None,
    active_only: bool = False,
    stale_after_seconds: int,
    now: datetime | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    effective_now = now or utc_now()
    statement = select(Task).options(selectinload(Task.runs)).order_by(Task.updated_at.desc())
    if status is not None:
        statement = statement.where(Task.status == status)
    if assignee is not None:
        statement = statement.where(Task.assignee_actor_id == assignee)
    if operation_id is not None:
        statement = statement.where(Task.operation_id == operation_id)
    if active_only:
        statement = statement.where(Task.status.in_(ACTIVE_TASK_STATUSES))
    tasks = list(session.scalars(statement.limit(limit)).unique())
    rendered: list[dict[str, Any]] = []
    stale_count = 0
    for task in tasks:
        latest_run = task.runs[-1] if task.runs else None
        last_activity_at = _latest_activity(session, task)
        stale = (
            task.status in ACTIVE_TASK_STATUSES
            and (effective_now - last_activity_at).total_seconds() > stale_after_seconds
        )
        stale_count += int(stale)
        rendered.append(
            {
                "id": str(task.id),
                "operation_id": str(task.operation_id),
                "objective": task.objective,
                "status": task.status,
                "priority": task.priority,
                "requester_actor_id": task.requester_actor_id,
                "assignee_actor_id": task.assignee_actor_id,
                "reviewer_actor_id": task.reviewer_actor_id,
                "workflow_ref": task.workflow_ref,
                "latest_run_id": str(latest_run.id) if latest_run else None,
                "latest_run_status": latest_run.status if latest_run else None,
                "updated_at": task.updated_at,
                "last_activity_at": last_activity_at,
                "stale": stale,
            }
        )
    counts: dict[str, int] = {}
    for item in rendered:
        item_status = str(item["status"])
        counts[item_status] = counts.get(item_status, 0) + 1
    return {
        "items": rendered,
        "count": len(rendered),
        "counts_by_status": counts,
        "stale_count": stale_count,
        "active_count": sum(item["status"] in ACTIONABLE_TASK_STATUSES for item in rendered),
        "blocked_count": sum(item["status"] == "blocked" for item in rendered),
        "observed_at": effective_now,
    }


def _encode_cursor(created_at: datetime, event_id: uuid.UUID) -> str:
    raw = f"{ensure_aware(created_at).isoformat()}|{event_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        timestamp, event_id = base64.urlsafe_b64decode(padded).decode().split("|", 1)
        return ensure_aware(datetime.fromisoformat(timestamp)), event_id
    except (ValueError, UnicodeDecodeError) as error:
        raise OperationsReadError("invalid_cursor", "Cursor de timeline inválido", 422) from error


def timeline_view(
    session: Session,
    *,
    operation_id: uuid.UUID | None = None,
    cursor: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    run_statement = (
        select(RunEvent, Run)
        .join(Run, Run.id == RunEvent.run_id)
        .order_by(RunEvent.created_at.desc())
        .limit(500)
    )
    if operation_id is not None:
        run_statement = run_statement.where(Run.operation_id == operation_id)
    events: list[dict[str, Any]] = [
        {
            "id": str(event.id),
            "source": "run",
            "event_type": event.event_type,
            "aggregate_type": "run",
            "aggregate_id": str(run.id),
            "operation_id": str(run.operation_id),
            "task_id": str(run.task_id),
            "payload": event.payload,
            "terminal": event.terminal,
            "created_at": ensure_aware(event.created_at),
            "_uuid": event.id,
        }
        for event, run in session.execute(run_statement)
    ]
    if operation_id is None:
        audits = list(
            session.scalars(select(AuditEvent).order_by(AuditEvent.created_at.desc()).limit(500))
        )
        events.extend(
            {
                "id": str(event.id),
                "source": "audit",
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": event.aggregate_id,
                "operation_id": event.payload.get("operation_id"),
                "task_id": event.payload.get("task_id"),
                "payload": event.payload,
                "terminal": False,
                "created_at": ensure_aware(event.created_at),
                "_uuid": event.id,
            }
            for event in audits
        )
    events.sort(key=lambda item: (item["created_at"], item["id"]))
    if cursor is not None:
        cursor_at, cursor_id = _decode_cursor(cursor)
        events = [
            item for item in events if (item["created_at"], item["id"]) > (cursor_at, cursor_id)
        ]
    selected = events[:limit]
    next_cursor = (
        _encode_cursor(selected[-1]["created_at"], selected[-1]["_uuid"]) if selected else cursor
    )
    for item in selected:
        item.pop("_uuid", None)
    return {
        "items": selected,
        "count": len(selected),
        "next_cursor": next_cursor,
        "reconnect": {"cursor": next_cursor, "duplicates": 0},
    }


def usage_view(
    session: Session,
    *,
    group_by: Literal["operation", "agent", "profile", "day"],
    operation_id: uuid.UUID | None,
) -> dict[str, Any]:
    return summarize_usage(session, group_by=group_by, operation_id=operation_id)


def _lease_state(run: Run, now: datetime) -> str:
    if run.status not in ACTIVE_RUN_STATUSES:
        return "released"
    if run.lease_owner is None or run.lease_expires_at is None:
        return "unclaimed"
    if ensure_aware(run.lease_expires_at) <= now:
        return "expired"
    return "claimed"


def _runtime_fields(payload: dict[str, Any], *, observed: bool = False) -> dict[str, Any]:
    return {
        "model": payload.get("model") or payload.get("model_alias"),
        "provider": payload.get("provider"),
        "reasoning_effort": payload.get("reasoning_effort"),
        **({"requested_model": payload.get("requested_model")} if observed else {}),
    }


def _fallback_fields(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = (
        "applied",
        "reason",
        "from_model",
        "to_model",
        "from_provider",
        "to_provider",
    )
    return {key: payload.get(key) for key in allowed if key in payload}


def autonomy_view(
    session: Session,
    *,
    operation_id: uuid.UUID | None = None,
    now: datetime | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    effective_now = now or utc_now()
    run_statement = select(Run).order_by(Run.created_at.desc())
    continuation_statement = select(WorkflowContinuation).order_by(
        WorkflowContinuation.created_at.desc()
    )
    if operation_id is not None:
        run_statement = run_statement.where(Run.operation_id == operation_id)
        continuation_statement = continuation_statement.where(
            WorkflowContinuation.operation_id == operation_id
        )
    runs = list(session.scalars(run_statement.limit(limit)))
    continuations = list(session.scalars(continuation_statement.limit(limit)))
    rendered_runs: list[dict[str, Any]] = []
    for run in runs:
        requested = run.requested_runtime if isinstance(run.requested_runtime, dict) else {}
        observed = run.observed_runtime if isinstance(run.observed_runtime, dict) else {}
        fallback = run.runtime_fallback if isinstance(run.runtime_fallback, dict) else {}
        rendered_runs.append(
            {
                "id": str(run.id),
                "task_id": str(run.task_id),
                "operation_id": str(run.operation_id),
                "worker_actor_id": run.worker_actor_id,
                "status": run.status,
                "dispatch_attempts": run.dispatch_attempts,
                "next_attempt_at": run.next_attempt_at,
                "lease_owner": run.lease_owner,
                "lease_expires_at": run.lease_expires_at,
                "lease_state": _lease_state(run, effective_now),
                "requested": {
                    "profile_id": run.requested_profile_id,
                    **_runtime_fields(requested),
                },
                "observed": {
                    "profile_id": run.effective_profile_id,
                    **_runtime_fields(observed, observed=True),
                },
                "fallback": _fallback_fields(fallback),
                "error_code": run.error_code,
                "created_at": run.created_at,
            }
        )
    rendered_continuations = [
        {
            "id": str(item.id),
            "operation_id": str(item.operation_id),
            "parent_task_id": str(item.parent_task_id),
            "target_actor_id": item.target_actor_id,
            "action": item.action,
            "status": item.status,
            "failure_code": item.failure_code,
            "created_at": item.created_at,
            "dispatched_at": item.dispatched_at,
            "failed_at": item.failed_at,
        }
        for item in continuations
    ]
    continuation_counts: dict[str, int] = {}
    for item in rendered_continuations:
        item_status = str(item["status"])
        continuation_counts[item_status] = continuation_counts.get(item_status, 0) + 1
    return {
        "dispatcher": {
            "queued": sum(
                item["status"] == "dispatching"
                and item["lease_state"] == "unclaimed"
                and ensure_aware(item["next_attempt_at"]) <= effective_now
                for item in rendered_runs
            ),
            "retry_waiting": sum(
                item["status"] == "dispatching"
                and ensure_aware(item["next_attempt_at"]) > effective_now
                for item in rendered_runs
            ),
            "claimed": sum(item["lease_state"] == "claimed" for item in rendered_runs),
            "running": sum(item["status"] == "running" for item in rendered_runs),
            "expired_leases": sum(item["lease_state"] == "expired" for item in rendered_runs),
        },
        "routing": {
            "fallbacks": sum(bool(item["fallback"].get("applied")) for item in rendered_runs),
            "unverified_terminal": sum(
                item["status"] in {"completed", "failed"} and item["observed"]["profile_id"] is None
                for item in rendered_runs
            ),
        },
        "continuations": {
            "counts_by_status": continuation_counts,
            "pending": continuation_counts.get("pending", 0),
            "dispatched": continuation_counts.get("dispatched", 0),
            "failed": continuation_counts.get("failed", 0),
            "items": rendered_continuations,
        },
        "runs": rendered_runs,
        "run_count": len(rendered_runs),
        "observed_at": effective_now,
    }


def provisioning_view(
    session: Session,
    *,
    status: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    statement = select(AgentRequestRecord).order_by(AgentRequestRecord.updated_at.desc())
    if status is not None:
        statement = statement.where(AgentRequestRecord.status == status)
    requests = list(session.scalars(statement.limit(limit)))
    items: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for request in requests:
        payload = request.payload if isinstance(request.payload, dict) else {}
        counts[request.status] = counts.get(request.status, 0) + 1
        items.append(
            {
                "id": str(request.id),
                "request_type": request.request_type,
                "status": request.status,
                "slug": payload.get("slug"),
                "role": payload.get("role"),
                "requested_by_actor_id": request.requested_by_actor_id,
                "decided_by_actor_id": request.decided_by_actor_id,
                "applied_by_actor_id": request.applied_by_actor_id,
                "retired_by_actor_id": request.retired_by_actor_id,
                "application_error_code": request.application_error_code,
                "created_at": request.created_at,
                "updated_at": request.updated_at,
                "decided_at": request.decided_at,
                "applied_at": request.applied_at,
                "retired_at": request.retired_at,
            }
        )
    return {
        "items": items,
        "count": len(items),
        "counts_by_status": counts,
        "pending_count": counts.get("pending", 0),
        "ready_to_apply_count": counts.get("approved", 0),
        "failed_count": counts.get("apply_failed", 0),
        "observed_at": utc_now(),
    }


def approvals_view(
    session: Session,
    *,
    status: str | None = None,
    operation_id: uuid.UUID | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    effective_now = now or utc_now()
    statement = (
        select(Approval, Run, Task)
        .join(Run, Run.id == Approval.run_id)
        .join(Task, Task.id == Run.task_id)
        .order_by(Approval.created_at.desc())
    )
    if operation_id is not None:
        statement = statement.where(Run.operation_id == operation_id)
    rows = list(session.execute(statement.limit(200)))
    items: list[dict[str, Any]] = []
    for approval, run, task in rows:
        effective_status = approval.status
        if approval.status == "pending" and ensure_aware(approval.expires_at) <= effective_now:
            effective_status = "expired"
        if status is not None and effective_status != status:
            continue
        items.append(
            {
                "id": str(approval.id),
                "run_id": str(run.id),
                "task_id": str(task.id),
                "operation_id": str(run.operation_id),
                "objective": task.objective,
                "action": approval.action,
                "status": effective_status,
                "requested_by_actor_id": approval.requested_by_actor_id,
                "decided_by_actor_id": approval.decided_by_actor_id,
                "decision_reason": approval.decision_reason,
                "expires_at": approval.expires_at,
                "created_at": approval.created_at,
            }
        )
    return {
        "items": items,
        "count": len(items),
        "pending_count": sum(item["status"] == "pending" for item in items),
        "observed_at": effective_now,
    }


def read_watchdog_state(path: str, *, now: datetime | None = None) -> dict[str, Any]:
    state_path = Path(path)
    if not state_path.exists():
        return {
            "status": "not_started",
            "model_calls": 0,
            "summary_count": 0,
            "idle_checks": 0,
            "active_checks": 0,
        }
    try:
        payload = cast(dict[str, Any], json.loads(state_path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError):
        return {"status": "invalid", "model_calls": 0}
    checked_at_raw = payload.get("checked_at")
    payload["activity_status"] = payload.get("status", "unknown")
    effective_now = now or utc_now()
    if isinstance(checked_at_raw, str):
        try:
            checked_at = ensure_aware(datetime.fromisoformat(checked_at_raw))
            payload["status"] = (
                "fresh" if (effective_now - checked_at).total_seconds() <= 900 else "stale"
            )
        except ValueError:
            payload["status"] = "invalid"
    else:
        payload["status"] = "invalid"
    payload["model_calls"] = 0
    return payload


def quota_view(session: Session, settings: Settings) -> dict[str, Any]:
    controls = control_status(session, settings)
    return {
        "budgets": controls["budgets"],
        "quota": controls["quota"],
        "circuits": controls["circuits"],
        "watchdog": read_watchdog_state(settings.operations_watchdog_state_path),
        "observed_at": utc_now(),
    }

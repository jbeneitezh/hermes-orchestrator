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
from hermes_orchestrator.models import Approval, AuditEvent, Run, RunEvent, Task
from hermes_orchestrator.usage_services import control_status, ensure_aware, summarize_usage

ACTIVE_TASK_STATUSES = {
    "pending",
    "ready",
    "dispatching",
    "running",
    "awaiting_approval",
    "blocked",
}


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
        "active_count": sum(item["status"] in ACTIVE_TASK_STATUSES for item in rendered),
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

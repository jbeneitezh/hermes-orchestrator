from __future__ import annotations

import hashlib
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import (
    Agent,
    AuditEvent,
    Budget,
    CircuitBreaker,
    ExecutionProfile,
    Run,
    Task,
    UsageLedger,
)

ACTIVE_RUN_STATUSES = {"dispatching", "running", "awaiting_approval"}
TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "reasoning_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
)


class ControlViolation(Exception):
    def __init__(
        self,
        code: str,
        detail: str,
        *,
        status_code: int = 409,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code
        self.retry_after = retry_after


@dataclass(frozen=True)
class ControlLimits:
    window_seconds: int
    soft_token_limit: int | None
    hard_token_limit: int | None
    max_concurrent_runs: int
    max_fan_out: int
    max_retries: int
    circuit_failure_threshold: int
    circuit_cooldown_seconds: int


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "unknown" or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _quota_reset(details: dict[str, Any], now: datetime) -> datetime | None:
    raw_reset = details.get("quota_reset_at")
    if isinstance(raw_reset, str):
        try:
            return ensure_aware(datetime.fromisoformat(raw_reset.replace("Z", "+00:00")))
        except ValueError:
            pass
    retry_after = _integer(details.get("retry_after"))
    return now + timedelta(seconds=retry_after) if retry_after is not None else None


def _parent_run_id(session: Session, task: Task) -> uuid.UUID | None:
    if task.parent_task_id is None:
        return None
    return session.scalar(
        select(Run.id)
        .where(Run.task_id == task.parent_task_id)
        .order_by(Run.attempt_number.desc())
        .limit(1)
    )


def ingest_run_usage(
    session: Session,
    run: Run,
    *,
    settings: Settings,
    actor_id: str = "system:usage-ledger",
) -> tuple[UsageLedger, bool]:
    task = session.get(Task, run.task_id)
    if task is None:
        raise ControlViolation("not_found", "La tarea del run no existe", status_code=404)
    profile_id = run.effective_profile_id or run.requested_profile_id
    requested_profile = session.get(ExecutionProfile, run.requested_profile_id)
    profile = session.get(ExecutionProfile, profile_id)
    usage = run.usage_snapshot if isinstance(run.usage_snapshot, dict) else {}
    details = run.error_details if isinstance(run.error_details, dict) else {}
    requested_runtime = run.requested_runtime if isinstance(run.requested_runtime, dict) else {}
    observed_runtime = run.observed_runtime if isinstance(run.observed_runtime, dict) else {}
    runtime_fallback = run.runtime_fallback if isinstance(run.runtime_fallback, dict) else {}
    effective_now = run.finished_at or utc_now()
    quota_exhausted = run.error_code in {"quota_exhausted", "rate_limited"}
    actual_cost = _decimal(usage.get("actual_cost"))
    estimated_cost = _decimal(usage.get("estimated_cost"))
    cost_status = (
        "known" if actual_cost is not None else "estimated" if estimated_cost else "unknown"
    )
    payload = {
        "run_id": str(run.id),
        "usage": usage,
        "error_code": run.error_code,
        "error_details": details,
        "status": run.status,
        "effective_profile": profile_id,
        "requested_runtime": requested_runtime,
        "observed_runtime": observed_runtime,
        "runtime_fallback": runtime_fallback,
    }
    payload_hash = _stable_hash(payload)
    existing = session.scalar(select(UsageLedger).where(UsageLedger.run_id == run.id))
    if existing is not None:
        if existing.payload_hash != payload_hash:
            raise ControlViolation(
                "usage_ingest_conflict",
                "El run ya tiene un asiento distinto en el ledger",
            )
        return existing, True

    started_at = run.started_at or run.created_at
    finished_at = run.finished_at
    duration_ms = None
    if finished_at is not None:
        duration_ms = max(
            0,
            int((ensure_aware(finished_at) - ensure_aware(started_at)).total_seconds() * 1000),
        )
    entry = UsageLedger(
        run_id=run.id,
        payload_hash=payload_hash,
        operation_id=run.operation_id,
        task_id=task.id,
        parent_run_id=_parent_run_id(session, task),
        project_id=settings.usage_project_id,
        category=task.workflow_ref or "uncategorized",
        requesting_agent_id=task.requester_actor_id,
        executing_agent_id=run.worker_actor_id,
        requested_profile=run.requested_profile_id,
        effective_profile=run.effective_profile_id,
        requested_model=str(
            requested_runtime.get("model")
            or (requested_profile.model if requested_profile is not None else "")
        )
        or None,
        requested_provider=str(
            requested_runtime.get("provider")
            or (requested_profile.provider if requested_profile is not None else "")
        )
        or None,
        requested_reasoning_effort=str(
            requested_runtime.get("reasoning_effort")
            or (requested_profile.reasoning_effort if requested_profile is not None else "")
        )
        or None,
        model=str(observed_runtime.get("model") or "")
        or (profile.model if profile is not None else None),
        provider=str(observed_runtime.get("provider") or "")
        or (profile.provider if profile is not None else None),
        reasoning_effort=str(observed_runtime.get("reasoning_effort") or "")
        or (profile.reasoning_effort if profile is not None else None),
        runtime_fallback=runtime_fallback,
        input_tokens=_integer(usage.get("input_tokens")),
        output_tokens=_integer(usage.get("output_tokens")),
        reasoning_tokens=_integer(usage.get("reasoning_tokens")),
        cache_read_tokens=_integer(usage.get("cache_read_tokens")),
        cache_write_tokens=_integer(usage.get("cache_write_tokens")),
        api_calls=_integer(usage.get("api_calls")),
        estimated_cost=estimated_cost,
        actual_cost=actual_cost,
        currency=str(usage["currency"]) if usage.get("currency") else None,
        cost_status=cost_status,
        cost_source=str(usage["cost_source"]) if usage.get("cost_source") else None,
        quota_status="exhausted" if quota_exhausted else "available",
        quota_reset_at=(
            _quota_reset(details, ensure_aware(effective_now)) if quota_exhausted else None
        ),
        started_at=run.started_at,
        finished_at=run.finished_at,
        duration_ms=duration_ms,
        outcome=run.status,
        retry_number=max(0, run.attempt_number - 1),
    )
    session.add(entry)
    session.flush()
    session.add(
        AuditEvent(
            actor_id=actor_id,
            event_type="usage.ingested",
            aggregate_type="usage_ledger",
            aggregate_id=str(entry.id),
            payload={
                "run_id": str(run.id),
                "operation_id": str(run.operation_id),
                "cost_status": cost_status,
                "quota_status": entry.quota_status,
            },
        )
    )
    return entry, False


def _min_limit(current: int | None, candidate: int | None) -> int | None:
    if candidate is None:
        return current
    return candidate if current is None else min(current, candidate)


def resolve_limits(
    session: Session,
    task: Task,
    worker: str,
    profile: str,
    settings: Settings,
) -> ControlLimits:
    values: dict[str, int | None] = {
        "window_seconds": settings.usage_window_seconds,
        "soft_token_limit": settings.usage_soft_token_limit,
        "hard_token_limit": settings.usage_hard_token_limit,
        "max_concurrent_runs": settings.usage_max_concurrent_runs,
        "max_fan_out": settings.usage_max_fan_out,
        "max_retries": settings.usage_max_retries,
        "circuit_failure_threshold": settings.usage_circuit_failure_threshold,
        "circuit_cooldown_seconds": settings.usage_circuit_cooldown_seconds,
    }
    matching_scopes = {
        ("global", "*"),
        ("project", settings.usage_project_id),
        ("agent", worker),
        ("profile", profile),
        ("category", task.workflow_ref or "uncategorized"),
    }
    for budget in session.scalars(select(Budget).where(Budget.enabled.is_(True))):
        if (budget.scope_type, budget.scope_key) not in matching_scopes:
            continue
        for field in values:
            candidate = getattr(budget, field)
            values[field] = _min_limit(values[field], candidate)
    for field in values:
        candidate = _integer(task.budget.get(field))
        values[field] = _min_limit(values[field], candidate)
    return ControlLimits(
        window_seconds=values["window_seconds"] or settings.usage_window_seconds,
        soft_token_limit=values["soft_token_limit"],
        hard_token_limit=values["hard_token_limit"],
        max_concurrent_runs=values["max_concurrent_runs"] or 1,
        max_fan_out=values["max_fan_out"] or 1,
        max_retries=values["max_retries"] if values["max_retries"] is not None else 0,
        circuit_failure_threshold=values["circuit_failure_threshold"] or 1,
        circuit_cooldown_seconds=values["circuit_cooldown_seconds"] or 1,
    )


def _audit_control(
    session: Session,
    *,
    actor_id: str,
    event_type: str,
    task: Task,
    payload: dict[str, Any],
) -> None:
    session.add(
        AuditEvent(
            actor_id=actor_id,
            event_type=event_type,
            aggregate_type="task",
            aggregate_id=str(task.id),
            payload=payload,
        )
    )


def _deny(
    session: Session,
    *,
    actor_id: str,
    task: Task,
    code: str,
    detail: str,
    status_code: int = 409,
    retry_after: int | None = None,
) -> None:
    _audit_control(
        session,
        actor_id=actor_id,
        event_type="dispatch.control_denied",
        task=task,
        payload={"code": code, "retry_after": retry_after},
    )
    session.commit()
    raise ControlViolation(
        code,
        detail,
        status_code=status_code,
        retry_after=retry_after,
    )


def enforce_dispatch_controls(
    session: Session,
    *,
    task: Task,
    worker_actor_id: str,
    requested_profile_id: str,
    actor_id: str,
    settings: Settings,
    now: datetime | None = None,
) -> ControlLimits:
    effective_now = ensure_aware(now or utc_now())
    profile = session.get(ExecutionProfile, requested_profile_id)
    if profile is not None and not profile.enabled:
        _deny(
            session,
            actor_id=actor_id,
            task=task,
            code="execution_profile_unavailable",
            detail="El perfil solicitado está deshabilitado",
            status_code=422,
        )
    worker = session.scalar(
        select(Agent).where(
            Agent.slug == worker_actor_id.removeprefix("agent:"),
            Agent.desired_state == "active",
        )
    )
    allowed_profiles = worker.policy_set.get("allowed_profiles") if worker is not None else None
    if isinstance(allowed_profiles, list) and requested_profile_id not in allowed_profiles:
        _deny(
            session,
            actor_id=actor_id,
            task=task,
            code="execution_profile_denied",
            detail="El perfil solicitado no está permitido para el agente",
            status_code=403,
        )
    if profile is not None and profile.reasoning_effort in {"xhigh", "max", "ultra"}:
        _deny(
            session,
            actor_id=actor_id,
            task=task,
            code="reasoning_effort_denied",
            detail="La política del programa limita el esfuerzo máximo a high",
            status_code=403,
        )
    limits = resolve_limits(session, task, worker_actor_id, requested_profile_id, settings)
    circuit = session.scalar(
        select(CircuitBreaker).where(
            CircuitBreaker.worker_actor_id == worker_actor_id,
            CircuitBreaker.profile_id == requested_profile_id,
            CircuitBreaker.state == "open",
        )
    )
    if circuit is not None:
        retry_after = None
        if circuit.reset_eligible_at is not None:
            retry_after = max(
                0,
                math.ceil(
                    (ensure_aware(circuit.reset_eligible_at) - effective_now).total_seconds()
                ),
            )
        _deny(
            session,
            actor_id=actor_id,
            task=task,
            code="circuit_open",
            detail="El circuito del worker/perfil está abierto",
            retry_after=retry_after,
        )

    quota = session.scalar(
        select(UsageLedger)
        .where(
            UsageLedger.project_id == settings.usage_project_id,
            UsageLedger.quota_status == "exhausted",
            UsageLedger.quota_reset_at.is_not(None),
            UsageLedger.quota_reset_at > effective_now,
        )
        .order_by(UsageLedger.quota_reset_at.desc())
        .limit(1)
    )
    if quota is not None and quota.quota_reset_at is not None:
        retry_after = max(
            1,
            math.ceil((ensure_aware(quota.quota_reset_at) - effective_now).total_seconds()),
        )
        _deny(
            session,
            actor_id=actor_id,
            task=task,
            code="quota_exhausted",
            detail="La cuota compartida está agotada hasta su reset",
            status_code=429,
            retry_after=retry_after,
        )

    attempts = session.scalar(select(func.count()).select_from(Run).where(Run.task_id == task.id))
    if (attempts or 0) > limits.max_retries:
        _deny(
            session,
            actor_id=actor_id,
            task=task,
            code="retry_limit_exceeded",
            detail="La tarea agotó sus reintentos automáticos",
        )

    if task.parent_task_id is not None:
        active_siblings = session.scalar(
            select(func.count())
            .select_from(Run)
            .join(Task, Task.id == Run.task_id)
            .where(
                Task.parent_task_id == task.parent_task_id,
                Run.status.in_(ACTIVE_RUN_STATUSES),
            )
        )
        if (active_siblings or 0) >= limits.max_fan_out:
            _deny(
                session,
                actor_id=actor_id,
                task=task,
                code="fan_out_limit_exceeded",
                detail="La operación alcanzó el fan-out máximo",
            )

    active_runs = session.scalar(
        select(func.count()).select_from(Run).where(Run.status.in_(ACTIVE_RUN_STATUSES))
    )
    if (active_runs or 0) >= limits.max_concurrent_runs:
        _deny(
            session,
            actor_id=actor_id,
            task=task,
            code="concurrency_limit_exceeded",
            detail="La cuenta alcanzó el máximo de runs concurrentes",
        )

    cutoff = effective_now - timedelta(seconds=limits.window_seconds)
    entries = list(
        session.scalars(
            select(UsageLedger).where(
                UsageLedger.project_id == settings.usage_project_id,
                UsageLedger.created_at >= cutoff,
            )
        )
    )
    consumed = sum(
        value
        for entry in entries
        for field in TOKEN_FIELDS
        if (value := getattr(entry, field)) is not None
    )
    projected = consumed + (_integer(task.budget.get("estimated_tokens")) or 0)
    if limits.hard_token_limit is not None and projected >= limits.hard_token_limit:
        _deny(
            session,
            actor_id=actor_id,
            task=task,
            code="budget_hard_exceeded",
            detail="El presupuesto hard impide nuevos despachos",
        )
    if limits.soft_token_limit is not None and projected >= limits.soft_token_limit:
        _audit_control(
            session,
            actor_id=actor_id,
            event_type="budget.soft_exceeded",
            task=task,
            payload={
                "consumed_tokens": consumed,
                "projected_tokens": projected,
                "soft_token_limit": limits.soft_token_limit,
            },
        )
    return limits


def record_run_outcome(
    session: Session,
    run: Run,
    *,
    settings: Settings,
    actor_id: str,
    now: datetime | None = None,
) -> CircuitBreaker:
    effective_now = ensure_aware(now or utc_now())
    profile_id = run.effective_profile_id or run.requested_profile_id
    circuit = session.scalar(
        select(CircuitBreaker).where(
            CircuitBreaker.worker_actor_id == run.worker_actor_id,
            CircuitBreaker.profile_id == profile_id,
        )
    )
    if circuit is None:
        circuit = CircuitBreaker(worker_actor_id=run.worker_actor_id, profile_id=profile_id)
        session.add(circuit)
        session.flush()
    task = session.get(Task, run.task_id)
    if task is None:
        raise ControlViolation("not_found", "La tarea del run no existe", status_code=404)
    limits = resolve_limits(session, task, run.worker_actor_id, profile_id, settings)
    if run.status == "completed":
        circuit.state = "closed"
        circuit.consecutive_failures = 0
        circuit.last_error_code = None
        circuit.opened_at = None
        circuit.reset_eligible_at = None
    elif run.status in {"failed", "timed_out"}:
        if circuit.last_error_code == run.error_code:
            circuit.consecutive_failures += 1
        else:
            circuit.consecutive_failures = 1
            circuit.last_error_code = run.error_code or run.status
        if circuit.consecutive_failures >= limits.circuit_failure_threshold:
            circuit.state = "open"
            circuit.opened_at = effective_now
            circuit.reset_eligible_at = effective_now + timedelta(
                seconds=limits.circuit_cooldown_seconds
            )
            session.add(
                AuditEvent(
                    actor_id=actor_id,
                    event_type="circuit.opened",
                    aggregate_type="circuit_breaker",
                    aggregate_id=str(circuit.id),
                    payload={
                        "worker_actor_id": run.worker_actor_id,
                        "profile_id": profile_id,
                        "error_code": circuit.last_error_code,
                        "consecutive_failures": circuit.consecutive_failures,
                    },
                )
            )
    return circuit


def reset_circuit(
    session: Session,
    circuit_id: uuid.UUID,
    *,
    actor_id: str,
    reason: str,
) -> CircuitBreaker:
    circuit = session.get(CircuitBreaker, circuit_id)
    if circuit is None:
        raise ControlViolation("not_found", "Circuit breaker no encontrado", status_code=404)
    circuit.state = "closed"
    circuit.consecutive_failures = 0
    circuit.last_error_code = None
    circuit.opened_at = None
    circuit.reset_eligible_at = None
    circuit.reset_by_actor_id = actor_id
    circuit.reset_reason = reason
    session.add(
        AuditEvent(
            actor_id=actor_id,
            event_type="circuit.reset",
            aggregate_type="circuit_breaker",
            aggregate_id=str(circuit.id),
            payload={"reason": reason},
        )
    )
    session.commit()
    return circuit


def get_usage_detail(session: Session, run_id: uuid.UUID) -> UsageLedger:
    entry = session.scalar(select(UsageLedger).where(UsageLedger.run_id == run_id))
    if entry is None:
        raise ControlViolation("not_found", "Asiento de usage no encontrado", status_code=404)
    return entry


def summarize_usage(
    session: Session,
    *,
    group_by: Literal["operation", "agent", "profile", "day"],
    operation_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    statement = select(UsageLedger)
    if operation_id is not None:
        statement = statement.where(UsageLedger.operation_id == operation_id)
    entries = list(session.scalars(statement.order_by(UsageLedger.created_at)))
    groups: dict[str, dict[str, Any]] = {}
    for entry in entries:
        key = {
            "operation": str(entry.operation_id),
            "agent": entry.executing_agent_id,
            "profile": entry.effective_profile or entry.requested_profile,
            "day": entry.created_at.date().isoformat(),
        }[group_by]
        group = groups.setdefault(
            key,
            {
                "key": key,
                "runs": 0,
                "outcomes": {},
                "known_tokens": {field: 0 for field in TOKEN_FIELDS},
                "unknown_tokens": {field: 0 for field in TOKEN_FIELDS},
                "known_actual_cost": Decimal("0"),
                "unknown_cost_entries": 0,
                "duration_ms": 0,
                "api_calls": 0,
                "api_calls_unknown": 0,
            },
        )
        group["runs"] += 1
        group["outcomes"][entry.outcome] = group["outcomes"].get(entry.outcome, 0) + 1
        for field in TOKEN_FIELDS:
            value = getattr(entry, field)
            if value is None:
                group["unknown_tokens"][field] += 1
            else:
                group["known_tokens"][field] += value
        if entry.actual_cost is None:
            group["unknown_cost_entries"] += 1
        else:
            group["known_actual_cost"] += entry.actual_cost
        if entry.duration_ms is not None:
            group["duration_ms"] += entry.duration_ms
        if entry.api_calls is None:
            group["api_calls_unknown"] += 1
        else:
            group["api_calls"] += entry.api_calls
    rendered: list[dict[str, Any]] = []
    for group in groups.values():
        totals = {
            field: (None if group["unknown_tokens"][field] else group["known_tokens"][field])
            for field in TOKEN_FIELDS
        }
        group["tokens"] = totals
        group["actual_cost"] = None if group["unknown_cost_entries"] else group["known_actual_cost"]
        group["cost_status"] = (
            "unknown"
            if group["unknown_cost_entries"] == group["runs"]
            else "partial"
            if group["unknown_cost_entries"]
            else "known"
        )
        rendered.append(group)
    return {
        "group_by": group_by,
        "groups": rendered,
        "entries": len(entries),
        "cost_status": "ledger",
    }


def control_status(session: Session, settings: Settings) -> dict[str, Any]:
    budgets = [
        {
            "source": "default",
            "scope_type": "project",
            "scope_key": settings.usage_project_id,
            **asdict(
                ControlLimits(
                    window_seconds=settings.usage_window_seconds,
                    soft_token_limit=settings.usage_soft_token_limit,
                    hard_token_limit=settings.usage_hard_token_limit,
                    max_concurrent_runs=settings.usage_max_concurrent_runs,
                    max_fan_out=settings.usage_max_fan_out,
                    max_retries=settings.usage_max_retries,
                    circuit_failure_threshold=settings.usage_circuit_failure_threshold,
                    circuit_cooldown_seconds=settings.usage_circuit_cooldown_seconds,
                )
            ),
        }
    ]
    budgets.extend(
        {
            "source": "database",
            "id": str(budget.id),
            "scope_type": budget.scope_type,
            "scope_key": budget.scope_key,
            "window_seconds": budget.window_seconds,
            "soft_token_limit": budget.soft_token_limit,
            "hard_token_limit": budget.hard_token_limit,
            "max_concurrent_runs": budget.max_concurrent_runs,
            "max_fan_out": budget.max_fan_out,
            "max_retries": budget.max_retries,
            "circuit_failure_threshold": budget.circuit_failure_threshold,
            "circuit_cooldown_seconds": budget.circuit_cooldown_seconds,
        }
        for budget in session.scalars(select(Budget).where(Budget.enabled.is_(True)))
    )
    quotas = list(
        session.scalars(
            select(UsageLedger)
            .where(UsageLedger.quota_status == "exhausted")
            .order_by(UsageLedger.created_at.desc())
            .limit(20)
        )
    )
    circuits = list(
        session.scalars(select(CircuitBreaker).order_by(CircuitBreaker.updated_at.desc()))
    )
    audits = list(
        session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.event_type.in_(
                    {
                        "usage.ingested",
                        "budget.soft_exceeded",
                        "dispatch.control_denied",
                        "circuit.opened",
                        "circuit.reset",
                    }
                )
            )
            .order_by(AuditEvent.created_at.desc())
            .limit(50)
        )
    )
    return {
        "budgets": budgets,
        "quota": [
            {
                "run_id": str(entry.run_id),
                "status": entry.quota_status,
                "reset_at": entry.quota_reset_at,
            }
            for entry in quotas
        ],
        "circuits": [
            {
                "id": str(circuit.id),
                "worker_actor_id": circuit.worker_actor_id,
                "profile_id": circuit.profile_id,
                "state": circuit.state,
                "consecutive_failures": circuit.consecutive_failures,
                "last_error_code": circuit.last_error_code,
                "opened_at": circuit.opened_at,
                "reset_eligible_at": circuit.reset_eligible_at,
            }
            for circuit in circuits
        ],
        "audit": [
            {
                "id": str(event.id),
                "actor_id": event.actor_id,
                "event_type": event.event_type,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": event.aggregate_id,
                "payload": event.payload,
                "created_at": event.created_at,
            }
            for event in audits
        ],
    }

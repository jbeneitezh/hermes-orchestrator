from __future__ import annotations

import argparse
import signal
import socket
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import create_database_engine, create_session_factory
from hermes_orchestrator.models import (
    Agent,
    AuditEvent,
    CommunicationEdge,
    Run,
    RunEvent,
    Task,
    WorkflowContinuation,
)
from hermes_orchestrator.policy import communication_is_allowed
from hermes_orchestrator.task_services import LifecycleError, create_task, dispatch_task
from hermes_orchestrator.usage_services import resolve_limits
from hermes_orchestrator.workflow_services import (
    ContinuationError,
    create_workflow_continuation,
    transition_workflow_continuation,
)


class StopFlag(Protocol):
    def is_set(self) -> bool: ...


class CoordinatorError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class ContinuationDirective:
    outcome: str
    target_actor_id: str
    action: str
    requested_profile_id: str
    retry_source: bool
    scope: str
    task_class: str | None


@dataclass(frozen=True)
class CoordinatorResult:
    continuation_id: uuid.UUID
    action: str
    status: str
    task_id: uuid.UUID | None = None
    run_id: uuid.UUID | None = None
    code: str | None = None


def _event_outcome(event: RunEvent) -> str:
    run = event.run
    handoff = run.error_details.get("agent_handoff")
    if run.status in {"failed", "timed_out"} or event.event_type in {
        "run.failed",
        "run.timed_out",
    }:
        return "failed"
    if run.task.status == "blocked" or (
        isinstance(handoff, dict) and handoff.get("outcome") == "blocked"
    ):
        return "blocked"
    return "completed"


def _workflow_enabled(task: Task) -> bool:
    return task.workflow_ref is not None and task.budget.get("workflow_auto_continue") is True


def _agent(session: Session, actor_id: str) -> Agent:
    slug = actor_id.removeprefix("agent:")
    if not actor_id.startswith("agent:"):
        raise CoordinatorError("workflow_target_invalid", "El target no es un agente")
    agent = session.scalar(select(Agent).where(Agent.slug == slug, Agent.desired_state == "active"))
    if agent is None:
        raise CoordinatorError("workflow_target_unavailable", "El agente target no está activo")
    return agent


def _parent_target(session: Session, task: Task) -> tuple[Task, str] | None:
    if task.parent_task_id is None:
        return None
    parent = session.get(Task, task.parent_task_id)
    if parent is None:
        raise CoordinatorError("workflow_parent_missing", "La tarea padre no existe")
    candidates = (parent.assignee_actor_id, parent.requester_actor_id)
    target = next(
        (value for value in candidates if value is not None and value.startswith("agent:")),
        None,
    )
    if target is None:
        raise CoordinatorError("workflow_parent_actor_missing", "La tarea padre no tiene actor")
    return parent, target


def _latest_profile(task: Task) -> str | None:
    return task.runs[-1].requested_profile_id if task.runs else None


def _directive(
    session: Session, event: RunEvent, settings: Settings
) -> ContinuationDirective | None:
    task = event.run.task
    if not _workflow_enabled(task):
        return None
    outcome = _event_outcome(event)
    configured_profile = task.budget.get("workflow_profile_id")
    if outcome == "failed":
        limits = resolve_limits(
            session,
            task,
            event.run.worker_actor_id,
            event.run.requested_profile_id,
            settings,
        )
        attempts = len(task.runs)
        retry_enabled = task.budget.get("workflow_retry_failed", True) is True
        if retry_enabled and attempts <= limits.max_retries:
            return ContinuationDirective(
                outcome=outcome,
                target_actor_id=event.run.worker_actor_id,
                action="retry_failed",
                requested_profile_id=event.run.requested_profile_id,
                retry_source=True,
                scope=str(task.budget.get("workflow_scope", "tradix")),
                task_class=None,
            )

    parent_target = _parent_target(session, task)
    if parent_target is None:
        return None
    parent, target_actor_id = parent_target
    requested_profile_id = (
        str(configured_profile)
        if isinstance(configured_profile, str) and configured_profile
        else _latest_profile(parent) or event.run.requested_profile_id
    )
    task_class = task.budget.get("workflow_handoff_class")
    return ContinuationDirective(
        outcome=outcome,
        target_actor_id=target_actor_id,
        action=f"resume_{outcome}",
        requested_profile_id=requested_profile_id,
        retry_source=False,
        scope=str(task.budget.get("workflow_scope", "tradix")),
        task_class=str(task_class) if isinstance(task_class, str) and task_class else None,
    )


def _persisted_directive(
    continuation: WorkflowContinuation,
) -> ContinuationDirective:
    value = continuation.context_snapshot.get("directive")
    if not isinstance(value, dict):
        raise CoordinatorError("workflow_directive_missing", "Falta la directiva persistida")
    try:
        return ContinuationDirective(
            outcome=str(value["outcome"]),
            target_actor_id=str(value["target_actor_id"]),
            action=str(value["action"]),
            requested_profile_id=str(value["requested_profile_id"]),
            retry_source=value["retry_source"] is True,
            scope=str(value["scope"]),
            task_class=str(value["task_class"]) if value.get("task_class") else None,
        )
    except KeyError as error:
        raise CoordinatorError(
            "workflow_directive_invalid", "La directiva persistida está incompleta"
        ) from error


def _directive_snapshot(directive: ContinuationDirective) -> dict[str, Any]:
    return {
        "outcome": directive.outcome,
        "target_actor_id": directive.target_actor_id,
        "action": directive.action,
        "requested_profile_id": directive.requested_profile_id,
        "retry_source": directive.retry_source,
        "scope": directive.scope,
        "task_class": directive.task_class,
    }


def _has_handoff_edge(
    session: Session,
    *,
    source: Agent,
    target: Agent,
    scope: str,
    task_class: str | None,
) -> bool:
    edges = list(
        session.scalars(
            select(CommunicationEdge).where(
                CommunicationEdge.source_agent_id == source.id,
                CommunicationEdge.target_agent_id == target.id,
            )
        )
    )
    candidate_classes = (
        [task_class]
        if task_class is not None
        else [value for edge in edges for value in edge.task_classes if value != "visibility"]
    )
    return any(
        communication_is_allowed(session, source.id, target.id, value, scope)
        for value in candidate_classes
    )


def _workflow_depth(session: Session, task: Task) -> int:
    depth = 0
    current = task
    visited = {task.id}
    while current.parent_task_id is not None:
        parent = session.get(Task, current.parent_task_id)
        if parent is None:
            raise CoordinatorError("workflow_parent_missing", "La cadena de tareas está rota")
        if parent.id in visited:
            raise CoordinatorError("workflow_cycle_detected", "La cadena de tareas contiene ciclo")
        visited.add(parent.id)
        depth += 1
        current = parent
    return depth


def _max_depth(task: Task, settings: Settings) -> int:
    configured = task.budget.get("workflow_max_depth")
    if isinstance(configured, int) and not isinstance(configured, bool):
        return max(0, min(settings.workflow_max_depth, configured))
    return settings.workflow_max_depth


def _already_seen(session: Session, event_id: uuid.UUID) -> bool:
    continuation = session.scalar(
        select(WorkflowContinuation.id).where(WorkflowContinuation.trigger_event_id == event_id)
    )
    if continuation is not None:
        return True
    rejection = session.scalar(
        select(AuditEvent.id).where(
            AuditEvent.aggregate_type == "run_event",
            AuditEvent.aggregate_id == str(event_id),
            AuditEvent.event_type == "workflow.continuation_skipped",
        )
    )
    return rejection is not None


def _successor_payload(
    continuation: WorkflowContinuation,
    event: RunEvent,
    directive: ContinuationDirective,
    *,
    depth: int,
) -> dict[str, Any]:
    source = event.run.task
    snapshot = continuation.context_snapshot
    result = snapshot.get("result", {})
    summary = result.get("summary") or event.run.summary or "Sin resumen terminal"
    artifacts = result.get("artifacts", [])
    artifact_refs = [
        item["uri"]
        for item in artifacts
        if isinstance(item, dict) and isinstance(item.get("uri"), str)
    ]
    budget = {key: value for key, value in source.budget.items() if not key.startswith("workflow_")}
    budget.update(
        {
            "workflow_depth": depth,
            "workflow_source_continuation_id": str(continuation.id),
            "workflow_auto_continue": True,
        }
    )
    return {
        "operation_id": source.operation_id,
        "parent_task_id": source.id,
        "workflow_ref": source.workflow_ref,
        "assignee_actor_id": directive.target_actor_id,
        "reviewer_actor_id": None,
        "independent_review": False,
        "priority": source.priority,
        "objective": (
            f"Reanuda el workflow {source.workflow_ref} tras resultado {directive.outcome} "
            f"de la tarea {source.id}. Contexto terminal: {str(summary)[:2000]}"
        ),
        "acceptance_criteria": [
            "Revisar el contexto terminal y la tarea padre",
            "Decidir y registrar el siguiente paso durable del workflow",
        ],
        "budget": budget,
        "references": list(dict.fromkeys([*source.references, *artifact_refs]))[:20],
        "dependency_ids": [],
        "deadline_at": source.deadline_at,
    }


class WorkflowCoordinator:
    def __init__(self, session_factory: sessionmaker[Session], settings: Settings) -> None:
        self.session_factory = session_factory
        self.settings = settings
        self.actor_id = f"system:{settings.workflow_coordinator_id}:{socket.gethostname()}"

    def run_once(self, stop: StopFlag | None = None) -> list[CoordinatorResult]:
        if stop is not None and stop.is_set():
            return []
        self._discover()
        with self.session_factory() as session:
            ids = list(
                session.scalars(
                    select(WorkflowContinuation.id)
                    .where(WorkflowContinuation.status == "pending")
                    .order_by(WorkflowContinuation.created_at, WorkflowContinuation.id)
                    .limit(self.settings.workflow_coordinator_batch_size)
                )
            )
        return [self._process(value) for value in ids]

    def _discover(self) -> None:
        with self.session_factory() as session:
            events = list(
                session.scalars(
                    select(RunEvent)
                    .join(Run, Run.id == RunEvent.run_id)
                    .join(Task, Task.id == Run.task_id)
                    .where(
                        RunEvent.terminal.is_(True),
                        Task.workflow_ref.is_not(None),
                        Task.budget["workflow_auto_continue"].as_boolean().is_(True),
                    )
                    .order_by(RunEvent.created_at, RunEvent.id)
                    .limit(self.settings.workflow_coordinator_batch_size * 4)
                )
            )
            for event in events:
                if _already_seen(session, event.id):
                    continue
                directive = _directive(session, event, self.settings)
                if directive is None:
                    continue
                try:
                    created = create_workflow_continuation(
                        session,
                        trigger_event_id=event.id,
                        target_actor_id=directive.target_actor_id,
                        action=directive.action,
                        actor_id=self.actor_id,
                        settings=self.settings,
                    )
                    created.continuation.context_snapshot = (
                        created.continuation.context_snapshot
                        | {"directive": _directive_snapshot(directive)}
                    )
                    session.commit()
                except ContinuationError:
                    continue

    def _process(self, continuation_id: uuid.UUID) -> CoordinatorResult:
        with self.session_factory() as session:
            continuation = session.get(WorkflowContinuation, continuation_id)
            if continuation is None or continuation.status != "pending":
                return CoordinatorResult(continuation_id, "noop", "ignored")
            event = session.get(RunEvent, continuation.trigger_event_id)
            if event is None:
                return self._fail(session, continuation, "event_not_found")
            try:
                directive = _persisted_directive(continuation)
                if directive.retry_source:
                    return self._retry(session, continuation, event, directive)
                return self._resume(session, continuation, event, directive)
            except (CoordinatorError, LifecycleError) as error:
                return self._fail(session, continuation, error.code)

    def _retry(
        self,
        session: Session,
        continuation: WorkflowContinuation,
        event: RunEvent,
        directive: ContinuationDirective,
    ) -> CoordinatorResult:
        task = event.run.task
        depth = _workflow_depth(session, task)
        if depth > _max_depth(task, self.settings):
            raise CoordinatorError("workflow_depth_exceeded", "Profundidad excedida")
        _agent(session, directive.target_actor_id)
        dispatched = dispatch_task(
            session,
            task_id=task.id,
            actor_id=self.actor_id,
            idempotency_key=f"workflow-dispatch:{continuation.id}",
            payload={
                "worker_actor_id": directive.target_actor_id,
                "requested_profile_id": directive.requested_profile_id,
                "timeout_seconds": self.settings.workflow_dispatch_timeout_seconds,
                "requires_approval": False,
                "approval_action": "dispatch",
                "approval_ttl_seconds": 900,
            },
            settings=self.settings,
        )
        run = dispatched.value
        continuation.context_snapshot = continuation.context_snapshot | {
            "dispatch": {"task_id": str(task.id), "run_id": str(run.id), "retry": True}
        }
        transition_workflow_continuation(
            session,
            continuation_id=continuation.id,
            new_status="dispatched",
            actor_id=self.actor_id,
        )
        return CoordinatorResult(
            continuation.id,
            "retry_dispatched",
            "dispatched",
            task.id,
            run.id,
        )

    def _resume(
        self,
        session: Session,
        continuation: WorkflowContinuation,
        event: RunEvent,
        directive: ContinuationDirective,
    ) -> CoordinatorResult:
        source_task = event.run.task
        depth = _workflow_depth(session, source_task) + 1
        if depth > _max_depth(source_task, self.settings):
            raise CoordinatorError("workflow_depth_exceeded", "Profundidad excedida")
        source_agent = _agent(session, event.run.worker_actor_id)
        target_agent = _agent(session, directive.target_actor_id)
        if not _has_handoff_edge(
            session,
            source=source_agent,
            target=target_agent,
            scope=directive.scope,
            task_class=directive.task_class,
        ):
            raise CoordinatorError("workflow_communication_denied", "Handoff no permitido")
        task_result = create_task(
            session,
            actor_id=self.actor_id,
            idempotency_key=f"workflow-task:{continuation.id}",
            payload=_successor_payload(
                continuation,
                event,
                directive,
                depth=depth,
            ),
        )
        task = task_result.value
        dispatched = dispatch_task(
            session,
            task_id=task.id,
            actor_id=self.actor_id,
            idempotency_key=f"workflow-dispatch:{continuation.id}",
            payload={
                "worker_actor_id": directive.target_actor_id,
                "requested_profile_id": directive.requested_profile_id,
                "timeout_seconds": self.settings.workflow_dispatch_timeout_seconds,
                "requires_approval": False,
                "approval_action": "dispatch",
                "approval_ttl_seconds": 900,
            },
            settings=self.settings,
        )
        run = dispatched.value
        continuation.context_snapshot = continuation.context_snapshot | {
            "dispatch": {"task_id": str(task.id), "run_id": str(run.id), "retry": False}
        }
        transition_workflow_continuation(
            session,
            continuation_id=continuation.id,
            new_status="dispatched",
            actor_id=self.actor_id,
        )
        return CoordinatorResult(
            continuation.id,
            "successor_dispatched",
            "dispatched",
            task.id,
            run.id,
        )

    def _fail(
        self,
        session: Session,
        continuation: WorkflowContinuation,
        code: str,
    ) -> CoordinatorResult:
        if continuation.status == "pending":
            transition_workflow_continuation(
                session,
                continuation_id=continuation.id,
                new_status="failed",
                actor_id=self.actor_id,
                failure_code=code,
            )
        return CoordinatorResult(
            continuation.id,
            "failed",
            "failed",
            code=code,
        )


def main() -> None:  # pragma: no cover - el proceso real se valida en Compose
    parser = argparse.ArgumentParser(description="Coordinador durable de workflows Hermes")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    settings = Settings()
    engine = create_database_engine(settings.database_url)
    coordinator = WorkflowCoordinator(create_session_factory(engine), settings)
    stop = threading.Event()

    def request_stop(*_: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        while not stop.is_set():
            coordinator.run_once(stop)
            if args.once:
                break
            stop.wait(settings.workflow_coordinator_poll_seconds)
    finally:
        engine.dispose()


if __name__ == "__main__":  # pragma: no cover
    main()

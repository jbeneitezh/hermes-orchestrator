from __future__ import annotations

import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from hermes_orchestrator.hermes_adapter import (
    HermesAdapterError,
    HermesEvent,
    HermesRunsAdapter,
)
from hermes_orchestrator.models import Run, RunEvent
from hermes_orchestrator.task_services import LifecycleError, get_run, transition_run

WORKER_TERMINAL_TO_LOCAL = {
    "completed": "completed",
    "failed": "failed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
}


def list_run_events(session: Session, run_id: uuid.UUID) -> list[RunEvent]:
    get_run(session, run_id)
    return list(
        session.scalars(
            select(RunEvent).where(RunEvent.run_id == run_id).order_by(RunEvent.sequence)
        )
    )


def _persist_event(session: Session, run_id: uuid.UUID, event: HermesEvent) -> RunEvent:
    sequence = (
        session.scalar(select(func.max(RunEvent.sequence)).where(RunEvent.run_id == run_id)) or 0
    ) + 1
    stored = RunEvent(
        run_id=run_id,
        sequence=sequence,
        worker_event_id=event.event_id,
        event_type=event.event_type,
        payload=event.payload,
        terminal=event.terminal,
    )
    session.add(stored)
    session.flush()
    return stored


def _persist_terminal_if_missing(
    session: Session,
    run_id: uuid.UUID,
    *,
    status: str,
    payload: dict[str, object],
) -> None:
    terminal_exists = session.scalar(
        select(func.count())
        .select_from(RunEvent)
        .where(RunEvent.run_id == run_id, RunEvent.terminal.is_(True))
    )
    if not terminal_exists:
        _persist_event(
            session,
            run_id,
            HermesEvent(
                event_id=None,
                event_type=f"run.{status}",
                payload=payload,
                terminal=True,
            ),
        )


def execute_run_via_hermes(
    session: Session,
    *,
    run_id: uuid.UUID,
    adapter: HermesRunsAdapter,
    input_text: str,
    actor_id: str = "system:hermes-adapter",
) -> Run:
    run = get_run(session, run_id)
    if run.status != "dispatching":
        raise LifecycleError(
            "invalid_transition", "El run debe estar en dispatching para enviarlo a Hermes"
        )

    try:
        adapter.discover()
        worker_run_id = adapter.start_run(input_text)
        run.worker_run_id = worker_run_id
        session.commit()
        transition_run(session, run_id=run_id, new_status="running", actor_id=actor_id)

        for event in adapter.stream_events(worker_run_id):
            _persist_event(session, run_id, event)
        session.commit()

        worker_state = adapter.get_run(worker_run_id)
        local_status = WORKER_TERMINAL_TO_LOCAL.get(worker_state.status)
        if local_status is None:
            raise HermesAdapterError(
                "invalid_worker_response",
                f"Hermes terminó el stream con estado {worker_state.status}",
            )

        run = get_run(session, run_id)
        run.usage_snapshot = worker_state.usage
        run.error_details = worker_state.error
        run.effective_profile_id = run.requested_profile_id
        _persist_terminal_if_missing(
            session,
            run_id,
            status=local_status,
            payload={
                "status": worker_state.status,
                "output": worker_state.output,
                "usage": worker_state.usage,
                "error": worker_state.error,
            },
        )
        session.commit()
        return transition_run(
            session,
            run_id=run_id,
            new_status=local_status,
            actor_id=actor_id,
            error_code=worker_state.error.get("code") if worker_state.error else None,
            summary=worker_state.output or worker_state.error.get("message"),
        )
    except HermesAdapterError as error:
        details = adapter.redact(error.as_dict())
        run = get_run(session, run_id)
        run.error_details = details
        _persist_terminal_if_missing(
            session,
            run_id,
            status="failed",
            payload={"status": "failed", "error": details},
        )
        session.commit()
        if run.status in {"dispatching", "running"}:
            return transition_run(
                session,
                run_id=run_id,
                new_status="failed",
                actor_id=actor_id,
                error_code=str(details["code"]),
                summary=str(details["message"]),
            )
        raise

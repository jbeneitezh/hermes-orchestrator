from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.hermes_adapter import (
    HermesAdapterError,
    HermesEvent,
    HermesRunsAdapter,
    HermesRunState,
)
from hermes_orchestrator.models import ExecutionProfile, Run, RunEvent, Task
from hermes_orchestrator.task_services import LifecycleError, get_run, transition_run

WORKER_TERMINAL_TO_LOCAL = {
    "completed": "completed",
    "failed": "failed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
}


def prepare_requested_runtime(
    session: Session,
    *,
    run: Run,
    task: Task,
    instructions: str | None = None,
) -> dict[str, Any]:
    """Fija una solicitud de runtime estable y reutilizable en replays."""

    default_instructions = (
        f"Actua como {run.worker_actor_id}. Ejecuta el objetivo, respeta los criterios "
        "de aceptacion y usa solo las referencias proporcionadas."
    )
    session_id = f"{run.operation_id}:{run.task_id}:{run.id}"
    existing = run.requested_runtime if isinstance(run.requested_runtime, dict) else {}
    if existing:
        required = {
            "profile_id",
            "model_alias",
            "model",
            "provider",
            "reasoning_effort",
            "instructions",
            "session_id",
        }
        if required.issubset(existing):
            return existing
        raise HermesAdapterError(
            "requested_runtime_invalid",
            "El runtime solicitado persistido esta incompleto",
            human_action_required=True,
        )

    profile = session.get(ExecutionProfile, run.requested_profile_id)
    if profile is None or not profile.enabled:
        raise HermesAdapterError(
            "execution_profile_unavailable",
            f"Perfil de ejecucion no disponible: {run.requested_profile_id}",
            human_action_required=True,
        )
    requested = {
        "profile_id": profile.id,
        # F6 registra como aliases los nombres canonicos de modelo.
        "model_alias": profile.model,
        "model": profile.model,
        "provider": profile.provider,
        "reasoning_effort": profile.reasoning_effort,
        "instructions": instructions or default_instructions,
        "session_id": session_id,
    }
    run.requested_runtime = requested
    session.commit()
    return requested


def _fallback_is_explicit(
    fallback: dict[str, Any],
    *,
    requested: dict[str, Any],
    observed: dict[str, Any],
) -> bool:
    if fallback.get("applied") is not True:
        return False
    if not str(fallback.get("reason") or "").strip():
        return False
    if fallback.get("from_model") != requested.get("model"):
        return False
    if fallback.get("to_model") != observed.get("model"):
        return False
    if requested.get("provider") != observed.get("provider"):
        return fallback.get("from_provider") == requested.get("provider") and fallback.get(
            "to_provider"
        ) == observed.get("provider")
    return True


def _persist_and_verify_runtime(
    session: Session,
    *,
    run: Run,
    worker_state: HermesRunState,
) -> str:
    requested = run.requested_runtime if isinstance(run.requested_runtime, dict) else {}
    fallback = (
        worker_state.runtime_fallback if isinstance(worker_state.runtime_fallback, dict) else {}
    )
    observed = {
        "requested_model": worker_state.requested_model,
        "requested_reasoning_effort": worker_state.requested_reasoning_effort,
        "model": worker_state.effective_model,
        "provider": worker_state.effective_provider,
        "reasoning_effort": worker_state.effective_reasoning_effort,
    }
    run.observed_runtime = observed
    run.runtime_fallback = fallback
    session.commit()

    if not requested:
        raise HermesAdapterError(
            "requested_runtime_missing",
            "No existe runtime solicitado para verificar el Run",
            human_action_required=True,
        )
    if not observed["model"] or not observed["provider"]:
        raise HermesAdapterError(
            "model_effective_unverified",
            "Hermes no informo modelo/provider efectivos",
            human_action_required=True,
        )
    if not observed["reasoning_effort"]:
        raise HermesAdapterError(
            "reasoning_effective_unverified",
            "Hermes no informo el esfuerzo efectivo",
            human_action_required=True,
        )
    if observed["reasoning_effort"] != requested.get("reasoning_effort"):
        raise HermesAdapterError(
            "reasoning_effective_unverified",
            "El esfuerzo efectivo no coincide con el solicitado",
            human_action_required=True,
        )

    model_or_provider_mismatch = observed["model"] != requested.get("model") or observed[
        "provider"
    ] != requested.get("provider")
    if model_or_provider_mismatch and not _fallback_is_explicit(
        fallback,
        requested=requested,
        observed=observed,
    ):
        raise HermesAdapterError(
            "model_effective_unverified",
            "El modelo/provider efectivo no coincide y no hay fallback explicito",
            human_action_required=True,
        )

    if not model_or_provider_mismatch:
        return str(requested["profile_id"])

    effective_profile = session.scalar(
        select(ExecutionProfile).where(
            ExecutionProfile.enabled.is_(True),
            ExecutionProfile.model == observed["model"],
            ExecutionProfile.provider == observed["provider"],
            ExecutionProfile.reasoning_effort == observed["reasoning_effort"],
        )
    )
    if effective_profile is None:
        raise HermesAdapterError(
            "effective_profile_unresolved",
            "El fallback observado no corresponde a un perfil habilitado",
            human_action_required=True,
        )
    return effective_profile.id


def list_run_events(session: Session, run_id: uuid.UUID) -> list[RunEvent]:
    get_run(session, run_id)
    return list(
        session.scalars(
            select(RunEvent).where(RunEvent.run_id == run_id).order_by(RunEvent.sequence)
        )
    )


def _persist_event(session: Session, run_id: uuid.UUID, event: HermesEvent) -> RunEvent:
    if event.event_id is not None:
        existing = session.scalar(
            select(RunEvent).where(
                RunEvent.run_id == run_id,
                RunEvent.worker_event_id == event.event_id,
            )
        )
        if existing is not None:
            return existing
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


def persist_worker_events(
    session: Session, run_id: uuid.UUID, events: list[HermesEvent]
) -> list[RunEvent]:
    """Persiste eventos Hermes de forma idempotente por ID remoto."""

    stored = [_persist_event(session, run_id, event) for event in events]
    session.commit()
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


def finalize_run_from_worker_state(
    session: Session,
    *,
    run_id: uuid.UUID,
    worker_state: HermesRunState,
    actor_id: str,
    settings: Settings | None = None,
) -> Run:
    """Cierra un Run local a partir del estado terminal observado en Hermes."""

    local_status = WORKER_TERMINAL_TO_LOCAL.get(worker_state.status)
    if local_status is None:
        raise HermesAdapterError(
            "invalid_worker_response",
            f"Hermes devolvió estado no terminal {worker_state.status}",
            retryable=True,
        )
    run = get_run(session, run_id)
    if run.status == "dispatching" and local_status == "completed":
        run = transition_run(
            session,
            run_id=run_id,
            new_status="running",
            actor_id=actor_id,
            settings=settings,
        )
    effective_profile_id = _persist_and_verify_runtime(
        session,
        run=run,
        worker_state=worker_state,
    )
    run.usage_snapshot = worker_state.usage
    agent_handoff = run.error_details.get("agent_handoff")
    run.error_details = worker_state.error
    if isinstance(agent_handoff, dict):
        run.error_details = run.error_details | {"agent_handoff": agent_handoff}
    run.effective_profile_id = effective_profile_id
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
        settings=settings,
    )


def execute_run_via_hermes(
    session: Session,
    *,
    run_id: uuid.UUID,
    adapter: HermesRunsAdapter,
    input_text: str,
    actor_id: str = "system:hermes-adapter",
    instructions: str | None = None,
) -> Run:
    run = get_run(session, run_id)
    if run.status != "dispatching":
        raise LifecycleError(
            "invalid_transition", "El run debe estar en dispatching para enviarlo a Hermes"
        )

    task = session.get(Task, run.task_id)
    if task is None:
        raise LifecycleError("not_found", "La tarea del run no existe", 404)

    try:
        requested_runtime = prepare_requested_runtime(
            session,
            run=run,
            task=task,
            instructions=instructions,
        )
        adapter.discover()
        worker_run_id = adapter.start_run(
            input_text,
            model_alias=str(requested_runtime["model_alias"]),
            reasoning_effort=str(requested_runtime["reasoning_effort"]),
            instructions=str(requested_runtime["instructions"]),
            session_id=str(requested_runtime["session_id"]),
        )
        run.worker_run_id = worker_run_id
        session.commit()
        transition_run(session, run_id=run_id, new_status="running", actor_id=actor_id)

        persist_worker_events(session, run_id, adapter.stream_events(worker_run_id))

        worker_state = adapter.get_run(worker_run_id)
        return finalize_run_from_worker_state(
            session,
            run_id=run_id,
            worker_state=worker_state,
            actor_id=actor_id,
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

from __future__ import annotations

import argparse
import signal
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import create_database_engine, create_session_factory
from hermes_orchestrator.hermes_adapter import (
    TERMINAL_STATUSES,
    HermesAdapterError,
    HermesRunsAdapter,
    HermesRunState,
)
from hermes_orchestrator.hermes_execution import (
    finalize_run_from_worker_state,
    persist_worker_events,
)
from hermes_orchestrator.models import Agent, AgentInstance, Task
from hermes_orchestrator.task_services import (
    LifecycleError,
    claim_dispatching_runs,
    get_run,
    heartbeat_run_lease,
    release_run_lease,
    transition_run,
)

AdapterFactory = Callable[[str, str], HermesRunsAdapter]
WORKER_SECRET_PREFIX = "secret://hermes/api-server/"


class DispatchError(Exception):
    def __init__(self, code: str, detail: str, *, retryable: bool) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.retryable = retryable


class LeaseLostError(DispatchError):
    def __init__(self) -> None:
        super().__init__("dispatch_lease_lost", "El dispatcher perdió el lease", retryable=True)


@dataclass(frozen=True)
class WorkerTarget:
    endpoint: str
    token: str


@dataclass(frozen=True)
class DispatchResult:
    run_id: uuid.UUID
    action: str
    status: str


class StopFlag(Protocol):
    def is_set(self) -> bool: ...


class WorkerResolver:
    def __init__(self, secrets: dict[str, str]) -> None:
        self._secrets = secrets

    def resolve(self, session: Session, worker_actor_id: str) -> WorkerTarget:
        slug = worker_actor_id.removeprefix("agent:")
        row = session.execute(
            select(Agent, AgentInstance)
            .join(AgentInstance, AgentInstance.agent_id == Agent.id)
            .where(
                Agent.slug == slug,
                Agent.desired_state == "active",
                AgentInstance.health == "healthy",
                AgentInstance.internal_endpoint.is_not(None),
            )
            .order_by(AgentInstance.last_heartbeat_at.desc())
            .limit(1)
        ).one_or_none()
        if row is None:
            raise DispatchError(
                "worker_unavailable",
                f"No existe instancia Hermes healthy para {worker_actor_id}",
                retryable=True,
            )
        agent, instance = row
        secret_ref = next(
            (ref for ref in agent.secret_refs if ref.startswith(WORKER_SECRET_PREFIX)), None
        )
        if secret_ref is None:
            raise DispatchError(
                "worker_secret_ref_missing",
                f"El agente {slug} no declara una referencia de API Hermes",
                retryable=False,
            )
        token = self._secrets.get(secret_ref)
        if not token:
            raise DispatchError(
                "worker_secret_unresolved",
                f"No se pudo resolver la referencia de API Hermes para {slug}",
                retryable=False,
            )
        return WorkerTarget(endpoint=str(instance.internal_endpoint), token=token)


class LeaseKeeper:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        run_id: uuid.UUID,
        owner: str,
        lease_duration: timedelta,
        heartbeat_seconds: float,
    ) -> None:
        self.session_factory = session_factory
        self.run_id = run_id
        self.owner = owner
        self.lease_duration = lease_duration
        self.heartbeat_seconds = heartbeat_seconds
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def __enter__(self) -> LeaseKeeper:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self.heartbeat_seconds * 2))

    def _loop(self) -> None:
        while not self._stop.wait(self.heartbeat_seconds):
            try:
                self.renew()
            except Exception:
                self._lost.set()
                return

    def renew(self) -> None:
        if self._lost.is_set():
            raise LeaseLostError()
        try:
            with self.session_factory() as session:
                heartbeat_run_lease(
                    session,
                    run_id=self.run_id,
                    lease_owner=self.owner,
                    lease_duration=self.lease_duration,
                )
        except LifecycleError as error:
            self._lost.set()
            raise LeaseLostError() from error


def build_run_input(task: Task) -> str:
    criteria = "\n".join(f"- {item}" for item in task.acceptance_criteria) or "- No definidos"
    references = "\n".join(f"- {item}" for item in task.references) or "- Ninguna"
    return (
        f"Objetivo:\n{task.objective}\n\n"
        f"Criterios de aceptación:\n{criteria}\n\n"
        f"Referencias:\n{references}"
    )


class RunDispatcher:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        settings: Settings,
        *,
        adapter_factory: AdapterFactory | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.settings = settings
        self.owner = f"system:{settings.run_dispatcher_id}"
        self.lease_duration = timedelta(seconds=settings.run_dispatcher_lease_seconds)
        self.retry_delay = timedelta(seconds=settings.run_dispatcher_retry_seconds)
        self.resolver = WorkerResolver(settings.run_dispatcher_worker_secrets)
        self.adapter_factory = adapter_factory or (
            lambda endpoint, token: HermesRunsAdapter(endpoint, token)
        )

    def run_once(self, stop: StopFlag | None = None) -> list[DispatchResult]:
        if stop is not None and stop.is_set():
            return []
        with self.session_factory() as session:
            claimed = claim_dispatching_runs(
                session,
                lease_owner=self.owner,
                lease_duration=self.lease_duration,
                limit=self.settings.run_dispatcher_batch_size,
            )
            run_ids = [run.id for run in claimed]
        results: list[DispatchResult] = []
        for run_id in run_ids:
            if stop is not None and stop.is_set():
                self._release(run_id)
                results.append(DispatchResult(run_id, "shutdown_released", "dispatching"))
                continue
            results.append(self._process(run_id))
        return results

    def _process(self, run_id: uuid.UUID) -> DispatchResult:
        keeper = LeaseKeeper(
            self.session_factory,
            run_id=run_id,
            owner=self.owner,
            lease_duration=self.lease_duration,
            heartbeat_seconds=self.settings.run_dispatcher_heartbeat_seconds,
        )
        try:
            with self.session_factory() as session:
                run = get_run(session, run_id)
                task = session.get(Task, run.task_id)
                if task is None:
                    raise DispatchError(
                        "task_not_found", "La tarea del Run no existe", retryable=False
                    )
                target = self.resolver.resolve(session, run.worker_actor_id)
                input_text = build_run_input(task)
                worker_run_id = run.worker_run_id
                dispatch_idempotency_key = run.dispatch_idempotency_key

            with keeper, self.adapter_factory(target.endpoint, target.token) as adapter:
                adapter.discover()
                keeper.renew()
                if worker_run_id is None:
                    worker_run_id = adapter.start_run(
                        input_text, idempotency_key=dispatch_idempotency_key
                    )
                    keeper.renew()
                    with self.session_factory() as session:
                        run = get_run(session, run_id)
                        run.worker_run_id = worker_run_id
                        session.commit()
                else:
                    state = adapter.get_run(worker_run_id)
                    keeper.renew()
                    if state.status in TERMINAL_STATUSES:
                        return self._finalize(run_id, state)

                with self.session_factory() as session:
                    run = get_run(session, run_id)
                    if run.status == "dispatching":
                        transition_run(
                            session,
                            run_id=run_id,
                            new_status="running",
                            actor_id=self.owner,
                            settings=self.settings,
                        )
                keeper.renew()
                events = adapter.stream_events(worker_run_id)
                keeper.renew()
                with self.session_factory() as session:
                    persist_worker_events(session, run_id, events)
                state = adapter.get_run(worker_run_id)
                keeper.renew()
                return self._finalize(run_id, state)
        except LeaseLostError:
            return DispatchResult(run_id, "lease_lost", self._status(run_id))
        except (DispatchError, HermesAdapterError) as error:
            return self._handle_error(run_id, error)

    def _finalize(self, run_id: uuid.UUID, state: HermesRunState) -> DispatchResult:
        with self.session_factory() as session:
            run = finalize_run_from_worker_state(
                session,
                run_id=run_id,
                worker_state=state,
                actor_id=self.owner,
                settings=self.settings,
            )
            return DispatchResult(run_id, "terminal", run.status)

    def _handle_error(
        self, run_id: uuid.UUID, error: DispatchError | HermesAdapterError
    ) -> DispatchResult:
        retryable = error.retryable
        with self.session_factory() as session:
            run = get_run(session, run_id)
            attempts = run.dispatch_attempts
        max_attempts = self.settings.usage_max_retries + 1
        if retryable and attempts < max_attempts:
            self._release(run_id)
            return DispatchResult(run_id, "retry_scheduled", self._status(run_id))
        with self.session_factory() as session:
            run = get_run(session, run_id)
            run.error_details = {
                "code": error.code,
                "message": error.detail if isinstance(error, DispatchError) else error.message,
                "retryable": retryable,
            }
            session.commit()
            if run.status in {"dispatching", "running"}:
                run = transition_run(
                    session,
                    run_id=run_id,
                    new_status="failed",
                    actor_id=self.owner,
                    error_code=error.code,
                    summary=run.error_details["message"],
                    settings=self.settings,
                )
            return DispatchResult(run_id, "failed", run.status)

    def _release(self, run_id: uuid.UUID) -> None:
        with self.session_factory() as session:
            release_run_lease(
                session,
                run_id=run_id,
                lease_owner=self.owner,
                next_attempt_at=datetime.now(UTC) + self.retry_delay,
            )

    def _status(self, run_id: uuid.UUID) -> str:
        with self.session_factory() as session:
            return get_run(session, run_id).status


def main() -> None:  # pragma: no cover - el proceso real se valida en F4
    parser = argparse.ArgumentParser(description="Dispatcher durable de Runs Hermes")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    settings = Settings()
    engine = create_database_engine(settings.database_url)
    dispatcher = RunDispatcher(create_session_factory(engine), settings)
    stop = threading.Event()

    def request_stop(*_: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        while not stop.is_set():
            dispatcher.run_once(stop)
            if args.once:
                break
            stop.wait(settings.run_dispatcher_poll_seconds)
    finally:
        engine.dispose()


if __name__ == "__main__":  # pragma: no cover
    main()

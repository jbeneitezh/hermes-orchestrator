from __future__ import annotations

import threading
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.hermes_adapter import HermesRunsAdapter
from hermes_orchestrator.models import (
    Agent,
    AgentInstance,
    Base,
    ExecutionProfile,
    Run,
    RunEvent,
    Task,
    UsageLedger,
)
from hermes_orchestrator.run_dispatcher import WORKER_SECRET_PREFIX, RunDispatcher, build_run_input
from tests.fakes.hermes_server import FakeHermesServer, FakeHermesState

SECRET_REF = f"{WORKER_SECRET_PREFIX}developer"


@pytest.fixture
def session_factory(tmp_path: Path) -> sessionmaker[Session]:
    engine = create_engine(f"sqlite+pysqlite:///{(tmp_path / 'dispatcher.db').as_posix()}")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as session:
        agent = Agent(
            slug="developer",
            role="developer",
            description="Worker de prueba",
            desired_state="active",
            owner_actor_id="user:owner",
            secret_refs=[SECRET_REF],
        )
        session.add(agent)
        session.flush()
        session.add(
            AgentInstance(
                agent_id=agent.id,
                internal_endpoint="http://placeholder",
                health="healthy",
                last_heartbeat_at=datetime.now(UTC),
                reconciliation_state="in_sync",
            )
        )
        session.add(
            ExecutionProfile(
                id="spark-low",
                provider="openai-api",
                model="gpt-5.3-codex-spark",
                reasoning_effort="low",
                max_iterations=8,
                timeout_seconds=300,
                relative_cost=1,
            )
        )
        session.commit()
    return factory


def create_run(session_factory: sessionmaker[Session], *, worker_run_id: str | None = None) -> Run:
    with session_factory() as session:
        task = Task(
            requester_actor_id="agent:leader",
            idempotency_key=str(uuid.uuid4()),
            request_hash="a" * 64,
            objective="Entregar una respuesta verificable",
            acceptance_criteria=["Existe resultado terminal"],
            references=["docs/agents/index.md"],
        )
        session.add(task)
        session.flush()
        run = Run(
            task_id=task.id,
            operation_id=task.operation_id,
            attempt_number=1,
            worker_actor_id="agent:developer",
            requested_profile_id="spark-low",
            dispatch_idempotency_key=str(uuid.uuid4()),
            dispatch_hash="b" * 64,
            status="dispatching",
            timeout_at=datetime.now(UTC) + timedelta(minutes=5),
            worker_run_id=worker_run_id,
        )
        session.add(run)
        session.commit()
        return run


def settings() -> Settings:
    return Settings(
        environment="test",
        run_dispatcher_id="test-dispatcher",
        run_dispatcher_lease_seconds=60,
        run_dispatcher_heartbeat_seconds=120,
        run_dispatcher_retry_seconds=0,
        run_dispatcher_worker_secrets={SECRET_REF: "test-token"},
        usage_max_retries=1,
    )


def dispatcher(session_factory: sessionmaker[Session], server: FakeHermesServer) -> RunDispatcher:
    def adapter_factory(endpoint: str, token: str) -> HermesRunsAdapter:
        assert endpoint == server.url
        return HermesRunsAdapter(server.url, token, max_reconnects=0)

    with session_factory() as session:
        instance = session.scalar(select(AgentInstance))
        assert instance is not None
        instance.internal_endpoint = server.url
        session.commit()
    return RunDispatcher(session_factory, settings(), adapter_factory=adapter_factory)


def test_build_run_input_includes_durable_task_context(session_factory) -> None:
    run = create_run(session_factory)
    with session_factory() as session:
        task = session.get(Task, run.task_id)
        assert task is not None
        rendered = build_run_input(task)

    assert f"- task_id: {task.id}" in rendered
    assert f"- operation_id: {task.operation_id}" in rendered
    assert "- parent_task_id: Ninguna" in rendered
    assert "Objetivo:\nEntregar una respuesta verificable" in rendered
    assert "- docs/agents/index.md" in rendered


def test_completed_closes_run_events_and_usage(session_factory) -> None:
    state = FakeHermesState()
    with FakeHermesServer(state) as server:
        run = create_run(session_factory)
        result = dispatcher(session_factory, server).run_once()[0]
    with session_factory() as session:
        stored = session.get(Run, run.id)
        events = list(session.scalars(select(RunEvent).where(RunEvent.run_id == run.id)))
        ledger = session.scalar(select(UsageLedger).where(UsageLedger.run_id == run.id))

    assert result.action == "terminal"
    assert stored is not None and stored.status == "completed"
    assert stored.worker_run_id == "fake-run"
    assert stored.usage_snapshot["input_tokens"] == 11
    assert events[-1].terminal is True
    assert ledger is not None and ledger.outcome == "completed"
    assert state.idempotency_keys == [run.dispatch_idempotency_key]


def test_failed_closes_run_and_usage_with_normalized_error(session_factory) -> None:
    state = FakeHermesState(status="failed")
    with FakeHermesServer(state) as server:
        run = create_run(session_factory)
        result = dispatcher(session_factory, server).run_once()[0]
    with session_factory() as session:
        stored = session.get(Run, run.id)
        ledger = session.scalar(select(UsageLedger).where(UsageLedger.run_id == run.id))

    assert result.status == "failed"
    assert stored is not None and stored.error_code == "provider_failed"
    assert ledger is not None and ledger.outcome == "failed"


def test_unhealthy_worker_is_rescheduled_without_starting_remote_run(session_factory) -> None:
    state = FakeHermesState(healthy=False)
    with FakeHermesServer(state) as server:
        run = create_run(session_factory)
        result = dispatcher(session_factory, server).run_once()[0]
    with session_factory() as session:
        stored = session.get(Run, run.id)

    assert result.action == "retry_scheduled"
    assert stored is not None and stored.status == "dispatching"
    assert stored.lease_owner is None
    assert state.start_requests == 0

    with session_factory() as session:
        deferred = session.get(Run, run.id)
        assert deferred is not None
        deferred.next_attempt_at = datetime.now(UTC) + timedelta(hours=1)
        session.commit()
    missing_secret_run = create_run(session_factory)
    unresolved = settings().model_copy(update={"run_dispatcher_worker_secrets": {}})
    with FakeHermesServer():
        result = RunDispatcher(session_factory, unresolved).run_once()[0]
    with session_factory() as session:
        failed = session.get(Run, missing_secret_run.id)
    assert result.action == "failed"
    assert failed is not None and failed.error_code == "worker_secret_unresolved"


def test_shutdown_after_claim_releases_run_without_contacting_worker(session_factory) -> None:
    class StopAfterClaim:
        calls = 0

        def is_set(self) -> bool:
            self.calls += 1
            return self.calls > 1

    state = FakeHermesState()
    with FakeHermesServer(state) as server:
        run = create_run(session_factory)
        service = dispatcher(session_factory, server)
        already_stopped = threading.Event()
        already_stopped.set()
        assert service.run_once(already_stopped) == []
        result = service.run_once(StopAfterClaim())[0]
    with session_factory() as session:
        stored = session.get(Run, run.id)

    assert result.action == "shutdown_released"
    assert stored is not None and stored.lease_owner is None
    assert state.start_requests == 0


def test_remote_active_run_is_resumed_without_second_post(session_factory) -> None:
    state = FakeHermesState(status="running", scenario="active_then_completed")
    with FakeHermesServer(state) as server:
        create_run(session_factory, worker_run_id="fake-run")
        result = dispatcher(session_factory, server).run_once()[0]

    assert result.status == "completed"
    assert state.start_requests == 0
    assert state.event_requests == 1


def test_remote_terminal_run_is_imported_without_stream_or_post(session_factory) -> None:
    state = FakeHermesState(status="completed")
    with FakeHermesServer(state) as server:
        run = create_run(session_factory, worker_run_id="fake-run")
        result = dispatcher(session_factory, server).run_once()[0]
    with session_factory() as session:
        stored = session.get(Run, run.id)

    assert result.status == "completed"
    assert stored is not None and stored.summary == "F8_OK"
    assert state.start_requests == 0
    assert state.event_requests == 0


def test_retry_reuses_worker_run_id_and_does_not_post_twice(session_factory) -> None:
    state = FakeHermesState(status="running", scenario="disconnect")
    with FakeHermesServer(state) as server:
        run = create_run(session_factory)
        service = dispatcher(session_factory, server)
        first = service.run_once()[0]
        state.scenario = "completed"
        state.status = "completed"
        second = service.run_once()[0]
    with session_factory() as session:
        events = list(session.scalars(select(RunEvent).where(RunEvent.run_id == run.id)))

    assert first.action == "retry_scheduled"
    assert second.status == "completed"
    assert state.start_requests == 1
    assert len([event for event in events if event.worker_event_id == "1"]) == 0


def test_lease_lost_fails_safe_before_persisting_remote_identity(session_factory) -> None:
    state = FakeHermesState()
    with FakeHermesServer(state) as server:
        run = create_run(session_factory)

        def steal_lease() -> None:
            with session_factory() as session:
                stored = session.get(Run, run.id)
                assert stored is not None
                stored.lease_owner = "system:other-dispatcher"
                session.commit()

        state.on_start = steal_lease
        result = dispatcher(session_factory, server).run_once()[0]
    with session_factory() as session:
        stored = session.get(Run, run.id)

    assert result.action == "lease_lost"
    assert stored is not None and stored.status == "dispatching"
    assert stored.worker_run_id is None
    assert state.start_requests == 1
    assert state.idempotency_keys == [run.dispatch_idempotency_key]

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
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
from hermes_orchestrator.provisioning import ProvisioningError
from hermes_orchestrator.run_dispatcher import (
    WORKER_SECRET_PREFIX,
    DispatchError,
    RunDispatcher,
    WorkerResolver,
    build_run_input,
)
from tests.fakes.hermes_server import FEATURES, FakeHermesServer, FakeHermesState

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


def test_worker_resolver_usa_broker_con_cache_y_preserva_fallback_estatico(
    session_factory,
) -> None:
    calls: list[str] = []

    def dynamic(secret_ref: str) -> str:
        calls.append(secret_ref)
        return "dynamic-token"

    with session_factory() as session:
        resolver = WorkerResolver({}, dynamic_resolver=dynamic, cache_seconds=60)
        first = resolver.resolve(session, "agent:developer")
        second = resolver.resolve(session, "agent:developer")
        static = WorkerResolver({SECRET_REF: "static-token"}, dynamic_resolver=dynamic).resolve(
            session, "agent:developer"
        )

    assert first.token == second.token == "dynamic-token"
    assert static.token == "static-token"
    assert calls == [SECRET_REF]


def test_worker_resolver_falla_cerrado_si_broker_no_resuelve(session_factory) -> None:
    def unavailable(_: str) -> str:
        raise ProvisioningError("provisioner_unavailable", "No disponible", 503)

    with session_factory() as session, pytest.raises(DispatchError) as error:
        WorkerResolver({}, dynamic_resolver=unavailable).resolve(session, "agent:developer")

    assert getattr(error.value, "code", None) == "worker_secret_unresolved"
    assert "No disponible" not in str(error.value)


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


@pytest.mark.parametrize("status_code", [403, 429, 503])
def test_stream_http_error_is_handled_without_losing_remote_run(
    session_factory, status_code
) -> None:
    state = FakeHermesState(status="running", events_status_code=status_code)
    with FakeHermesServer(state) as server:
        run = create_run(session_factory)
        service = dispatcher(session_factory, server)
        first = service.run_once()[0]
        with session_factory() as session:
            stored = session.get(Run, run.id)
            assert stored is not None
            assert stored.worker_run_id == "fake-run"
            assert stored.lease_owner is None
            if status_code == 403:
                assert first.action == "failed"
                assert stored.status == "failed"
                assert stored.error_code == "stream_rejected"
                assert stored.error_details["message"] == "SSE no disponible"
            else:
                assert first.action == "retry_scheduled"
                assert stored.status == "running"

        state.events_status_code = 200
        state.status = "completed"
        resumed = service.run_once()

    if status_code == 403:
        assert resumed == []
    else:
        assert len(resumed) == 1
        assert resumed[0].status == "completed"
    assert state.start_requests == 1
    assert state.event_requests == 1
    with session_factory() as session:
        ledgers = list(session.scalars(select(UsageLedger).where(UsageLedger.run_id == run.id)))
        assert len(ledgers) == 1
        assert ledgers[0].outcome == ("failed" if status_code == 403 else "completed")


@pytest.mark.parametrize("retry_after", ["Thu, 10 Sep 2026 12:02:00 GMT", "invalid-date"])
def test_retry_after_does_not_abort_batch_and_reopens_each_remote_stream(
    session_factory, retry_after
) -> None:
    remote_statuses: dict[str, str] = {}
    event_requests: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/health":
            return httpx.Response(200, json={"status": "ok"})
        if path == "/v1/capabilities":
            return httpx.Response(200, json={"features": FEATURES})
        if request.method == "POST" and path == "/v1/runs":
            remote_id = f"batch-run-{len(remote_statuses) + 1}"
            remote_statuses[remote_id] = "running"
            return httpx.Response(202, json={"run_id": remote_id})
        remote_id = path.split("/")[3]
        assert remote_id in remote_statuses
        if path.endswith("/events"):
            event_requests[remote_id] = event_requests.get(remote_id, 0) + 1
            if event_requests[remote_id] == 1:
                return httpx.Response(
                    429,
                    headers={"Retry-After": retry_after},
                    stream=httpx.ByteStream(b'{"error":{"code":"rate_limited"}}'),
                )
            remote_statuses[remote_id] = "completed"
            return httpx.Response(
                200,
                stream=httpx.ByteStream(
                    b'id: 1\nevent: run.completed\ndata: {"status":"completed"}\n\n'
                ),
            )
        return httpx.Response(
            200,
            json={
                "run_id": remote_id,
                "status": remote_statuses[remote_id],
                "output": "BATCH_OK",
                "effective_model": "gpt-5.3-codex-spark",
                "effective_provider": "openai-api",
                "effective_reasoning_effort": "low",
                "usage": {"prompt_tokens": 7},
            },
        )

    runs = [create_run(session_factory), create_run(session_factory)]
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        service = RunDispatcher(
            session_factory,
            settings().model_copy(update={"run_dispatcher_batch_size": 2}),
            adapter_factory=lambda endpoint, token: HermesRunsAdapter(
                endpoint, token, client=client, max_reconnects=0
            ),
        )
        assert [result.action for result in service.run_once()] == ["retry_scheduled"] * 2
        with session_factory() as session:
            for run in runs:
                stored = session.get(Run, run.id)
                assert stored is not None and stored.status == "running"
                assert stored.worker_run_id in remote_statuses
                assert stored.lease_owner is None
                assert stored.next_attempt_at is not None
        assert [result.status for result in service.run_once()] == ["completed"] * 2
        assert service.run_once() == []

    assert len(remote_statuses) == 2
    assert list(event_requests.values()) == [2, 2]
    with session_factory() as session:
        for run in runs:
            stored = session.get(Run, run.id)
            assert stored is not None and stored.lease_owner is None
            assert stored.summary == "BATCH_OK"
            events = list(session.scalars(select(RunEvent).where(RunEvent.run_id == run.id)))
            ledger = list(session.scalars(select(UsageLedger).where(UsageLedger.run_id == run.id)))
            assert len(events) == len(ledger) == 1
            assert events[0].terminal
            assert ledger[0].outcome == "completed"


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


@pytest.mark.parametrize("status_code", [403, 503])
@pytest.mark.parametrize("body_kind", ["json", "text", "html", "malformed", "unexpected_json"])
def test_running_run_with_24_attempts_fails_safely_and_preserves_http_diagnostic(
    session_factory, status_code, body_kind
) -> None:
    private = "datos-privados-del-worker"
    bodies = {
        "json": json.dumps(
            {
                "error": {
                    "code": "worker_http_rejected",
                    "message": "Acceso Bearer test-token",
                    "private": private,
                    "http_status": 200,
                }
            }
        ),
        "text": private,
        "html": f"<html>{private}</html>",
        "malformed": '{"error":{"message":"' + private,
        "unexpected_json": json.dumps(
            {"error": {"code": [private], "message": {"private": private}}}
        ),
    }
    calls: list[tuple[str, str]] = []
    stream_response = httpx.Response(
        status_code, stream=httpx.ByteStream(bodies[body_kind].encode())
    )

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        assert request.method == "GET", "No se debe iniciar ni detener ningún worker"
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok"})
        if request.url.path == "/v1/capabilities":
            return httpx.Response(200, json={"features": FEATURES})
        if request.url.path == "/v1/runs/existing-run":
            return httpx.Response(200, json={"run_id": "existing-run", "status": "running"})
        assert request.url.path == "/v1/runs/existing-run/events"
        return stream_response

    run = create_run(session_factory, worker_run_id="existing-run")
    with session_factory() as session:
        stored = session.get(Run, run.id)
        assert stored is not None
        stored.status = "running"
        stored.dispatch_attempts = 24
        session.commit()
    exhausted_settings = settings()
    assert exhausted_settings.usage_max_retries == 1
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        service = RunDispatcher(
            session_factory,
            exhausted_settings,
            adapter_factory=lambda endpoint, token: HermesRunsAdapter(
                endpoint, token, client=client, max_reconnects=0
            ),
        )
        result = service.run_once()[0]
        assert result.action == result.status == "failed"
        assert service.run_once() == []

    assert calls == [
        ("GET", "/health"),
        ("GET", "/v1/capabilities"),
        ("GET", "/v1/runs/existing-run"),
        ("GET", "/v1/runs/existing-run/events"),
    ]
    assert stream_response.is_closed
    with session_factory() as session:
        stored = session.get(Run, run.id)
        assert stored is not None and stored.status == "failed"
        assert stored.worker_run_id == "existing-run"
        # El claim suma un intento; no se reinicia ni se reduce el contador agotado.
        assert stored.dispatch_attempts == 25
        assert stored.lease_owner is stored.lease_acquired_at is stored.lease_expires_at is None
        assert stored.heartbeat_at is None
        assert stored.error_details["http_status"] == status_code
        assert stored.error_details["retryable"] is (status_code == 503)
        expected_message = (
            "Acceso Bearer [REDACTED]" if body_kind == "json" else f"Hermes HTTP {status_code}"
        )
        assert stored.summary == stored.error_details["message"] == expected_message
        assert stored.error_code == (
            "worker_http_rejected" if body_kind == "json" else "transient_provider_error"
        )
        assert private not in json.dumps(stored.error_details)
        assert "test-token" not in json.dumps(stored.error_details)
        ledgers = list(session.scalars(select(UsageLedger).where(UsageLedger.run_id == run.id)))
        assert len(ledgers) == 1
        assert ledgers[0].outcome == "failed"

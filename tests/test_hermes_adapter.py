from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.hermes_adapter import (
    CapabilityMissingError,
    HermesAdapterError,
    HermesRunsAdapter,
    WorkerUnhealthyError,
)
from hermes_orchestrator.hermes_execution import execute_run_via_hermes, list_run_events
from hermes_orchestrator.models import Base, ExecutionProfile, Run, RunEvent, Task
from tests.fakes.hermes_server import FakeHermesServer, FakeHermesState


@pytest.fixture
def session_factory(tmp_path: Path) -> sessionmaker[Session]:
    engine = create_engine(f"sqlite+pysqlite:///{(tmp_path / 'adapter.db').as_posix()}")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def create_run(session: Session) -> Run:
    if session.get(ExecutionProfile, "spark-low") is None:
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
    task = Task(
        requester_actor_id="agent:leader",
        idempotency_key=str(uuid.uuid4()),
        request_hash="a" * 64,
        objective="Cerrar una ejecución real mediante Hermes",
        acceptance_criteria=["Hay evento terminal"],
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
    )
    session.add(run)
    session.commit()
    return run


def test_complete_closes_local_run_and_persists_terminal_event(session_factory) -> None:
    with FakeHermesServer() as server, session_factory() as session:
        run = create_run(session)
        with HermesRunsAdapter(server.url, "test-token") as adapter:
            result = execute_run_via_hermes(
                session, run_id=run.id, adapter=adapter, input_text="Responde F8_OK"
            )
        events = list(session.scalars(select(RunEvent).where(RunEvent.run_id == run.id)))

    assert result.status == "completed"
    assert result.effective_profile_id == "spark-low"
    assert result.summary == "F8_OK"
    assert result.usage_snapshot["input_tokens"] == 11
    assert events[-1].terminal is True


def test_failed_run_normalizes_error_and_closes_local_run(session_factory) -> None:
    state = FakeHermesState(status="failed")
    with FakeHermesServer(state) as server, session_factory() as session:
        run = create_run(session)
        with HermesRunsAdapter(server.url, "test-token") as adapter:
            result = execute_run_via_hermes(
                session, run_id=run.id, adapter=adapter, input_text="Falla de forma controlada"
            )

    assert result.status == "failed"
    assert result.error_code == "provider_failed"
    assert result.error_details["message"] == "Proveedor rechazó la petición"


def test_sse_disconnect_reconnects_with_cursor_and_deduplicates() -> None:
    state = FakeHermesState(scenario="reconnect")
    with FakeHermesServer(state) as server, HermesRunsAdapter(server.url, "test-token") as adapter:
        events = adapter.stream_events("fake-run")

    assert [event.event_id for event in events] == ["1", "2"]
    assert state.event_requests == 2
    assert state.last_event_ids == [None, "1"]


def test_stop_returns_cancelled_state() -> None:
    with FakeHermesServer() as server, HermesRunsAdapter(server.url, "test-token") as adapter:
        assert adapter.stop_run("fake-run").status == "cancelled"


def test_approval_is_forwarded_and_acknowledged() -> None:
    with FakeHermesServer() as server, HermesRunsAdapter(server.url, "test-token") as adapter:
        assert adapter.respond_approval("fake-run", "approve") == {"accepted": True}


def test_unhealthy_worker_fails_closed(session_factory) -> None:
    state = FakeHermesState(healthy=False)
    with FakeHermesServer(state) as server, session_factory() as session:
        run = create_run(session)
        with HermesRunsAdapter(server.url, "test-token") as adapter:
            result = execute_run_via_hermes(
                session, run_id=run.id, adapter=adapter, input_text="No debe enviarse"
            )

    assert result.status == "failed"
    assert result.error_code == "worker_unhealthy"


def test_missing_capability_is_rejected() -> None:
    state = FakeHermesState()
    state.features["run_stop"] = False
    with (
        FakeHermesServer(state) as server,
        HermesRunsAdapter(server.url, "test-token") as adapter,
        pytest.raises(CapabilityMissingError) as captured,
    ):
        adapter.discover()

    assert captured.value.missing == ["run_stop"]


def test_secrets_are_redacted_from_errors_and_nested_payloads() -> None:
    token = "very-secret-token"
    adapter = HermesRunsAdapter(
        "http://worker.invalid",
        token,
        client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200))),
    )
    redacted = adapter.redact(
        {
            "password": "do-not-store",
            "nested": {"message": f"Authorization: Bearer {token} https://x?api_key=abc&ok=1"},
        }
    )
    adapter.client.close()

    assert redacted["password"] == "[REDACTED]"
    serialized = str(redacted)
    assert token not in serialized
    assert "abc" not in serialized


def test_defensive_protocol_normalization_and_invalid_local_state(session_factory) -> None:
    responses = iter(
        [
            httpx.Response(500, json={"error": {"message": "temporal"}}),
            httpx.Response(202, json={}),
        ]
    )
    client = httpx.Client(transport=httpx.MockTransport(lambda _: next(responses)))
    adapter = HermesRunsAdapter("http://worker.invalid", "token", client=client)

    with pytest.raises(HermesAdapterError) as provider_error:
        adapter.start_run("uno")
    with pytest.raises(HermesAdapterError) as invalid_response:
        adapter.start_run("dos")
    assert provider_error.value.retryable is True
    assert invalid_response.value.code == "invalid_worker_response"
    assert adapter.normalize_usage({"usage": "unknown"}) == {}
    assert adapter.normalize_usage({"usage": {"input_tokens": 4, "api_calls": 1}}) == {
        "input_tokens": 4,
        "output_tokens": 0,
        "reasoning_tokens": 0,
        "cache_read_tokens": 0,
        "api_calls": 1,
    }
    assert adapter.normalize_error({"error": "timeout"})["retryable"] is True
    assert adapter.normalize_error({}) == {}
    assert adapter.redact(["Bearer token", {"token": "token"}]) == [
        "Bearer [REDACTED]",
        {"token": "[REDACTED]"},
    ]
    assert adapter._parse_frame({}) is None
    assert adapter._parse_frame({"data": "not-json"}).payload == {"message": "not-json"}
    assert adapter._parse_frame({"data": "[1,2]"}).payload == {"data": [1, 2]}

    with session_factory() as session:
        run = create_run(session)
        run.status = "running"
        session.commit()
        with pytest.raises(Exception, match="dispatching"):
            execute_run_via_hermes(
                session, run_id=run.id, adapter=adapter, input_text="estado inválido"
            )
        assert list_run_events(session, run.id) == []
    client.close()


def test_transport_and_stream_exhaustion_are_normalized() -> None:
    def disconnected(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("token=never-store", request=request)

    failing_client = httpx.Client(transport=httpx.MockTransport(disconnected))
    failing_adapter = HermesRunsAdapter(
        "http://worker.invalid", "never-store", client=failing_client
    )
    with pytest.raises(WorkerUnhealthyError) as unhealthy:
        failing_adapter.discover()
    assert "never-store" not in unhealthy.value.message
    failing_client.close()

    stream_client = httpx.Client(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text='event: message.delta\ndata: {"delta":"partial"}\n\n',
            )
        )
    )
    stream_adapter = HermesRunsAdapter(
        "http://worker.invalid", "token", client=stream_client, max_reconnects=0
    )
    with pytest.raises(HermesAdapterError) as disconnected_error:
        stream_adapter.stream_events("run")
    assert disconnected_error.value.code == "worker_disconnected"
    stream_client.close()

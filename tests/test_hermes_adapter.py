from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

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


@pytest.mark.parametrize(
    ("status_code", "retryable", "human_action_required"),
    [
        (400, False, False),
        (401, False, True),
        (403, False, True),
        (429, True, False),
        (500, True, False),
        (503, True, False),
    ],
)
@pytest.mark.parametrize("json_body", [True, False])
def test_stream_http_errors_are_read_normalized_and_redacted(
    status_code: int, retryable: bool, human_action_required: bool, json_body: bool
) -> None:
    message = "Fallo Bearer synthetic-stream-credential api_key=synthetic-query-credential"
    body = (
        json.dumps({"error": {"code": "stream_rejected", "message": message}})
        if json_body
        else message
    )
    response = httpx.Response(
        status_code,
        headers={"Retry-After": "2.5"},
        stream=httpx.ByteStream(body.encode()),
    )
    assert not response.is_stream_consumed
    with httpx.Client(transport=httpx.MockTransport(lambda _: response)) as client:
        adapter = HermesRunsAdapter(
            "http://worker.invalid", "synthetic-stream-credential", client=client
        )
        with pytest.raises(HermesAdapterError) as captured:
            adapter.stream_events("run")

    error = captured.value
    assert error.code == ("stream_rejected" if json_body else "transient_provider_error")
    assert error.retryable is retryable
    assert error.human_action_required is human_action_required
    assert error.retry_after == 2.5
    assert error.message == (
        "Fallo Bearer [REDACTED] api_key=[REDACTED]" if json_body else f"Hermes HTTP {status_code}"
    )
    assert error.http_status == error.as_dict()["http_status"] == status_code
    assert response.is_stream_consumed
    assert response.is_closed


def test_successful_stream_returns_at_terminal_without_reading_remaining_body() -> None:
    class OpenEndedStream(httpx.SyncByteStream):
        def __iter__(self) -> Iterator[bytes]:
            yield b'id: 1\nevent: run.completed\ndata: {"status":"completed"}\n\n'
            raise AssertionError("El stream no debe consumirse tras el evento terminal")

    response = httpx.Response(200, stream=OpenEndedStream())
    with httpx.Client(transport=httpx.MockTransport(lambda _: response)) as client:
        adapter = HermesRunsAdapter("http://worker.invalid", "test-token", client=client)
        events = adapter.stream_events("run")

    assert [event.event_type for event in events] == ["run.completed"]
    assert response.is_closed


@pytest.mark.parametrize("status_code", [403, 503])
@pytest.mark.parametrize("operation", ["start", "stream"])
@pytest.mark.parametrize(
    "body",
    [
        "datos-privados-del-worker",
        "<html>datos-privados-del-worker</html>",
        '{"error":{"message":"datos-privados-del-worker',
        '{"error":"datos-privados-del-worker"}',
        '{"error":{"message":{"private":"datos-privados-del-worker"},"code":["privado"]}}',
        '["datos-privados-del-worker"]',
        '"datos-privados-del-worker"',
        '{"error":{"message":123,"code":456}}',
        "",
    ],
)
def test_http_diagnostic_discards_arbitrary_bodies_and_unexpected_json_fields(
    status_code, operation, body
) -> None:
    response = httpx.Response(status_code, stream=httpx.ByteStream(body.encode()))
    with httpx.Client(transport=httpx.MockTransport(lambda _: response)) as client:
        adapter = HermesRunsAdapter("http://worker.invalid", "test-token", client=client)
        with pytest.raises(HermesAdapterError) as captured:
            if operation == "start":
                adapter.start_run("test")
            else:
                adapter.stream_events("existing-run")

    error = captured.value
    assert error.message == str(error) == f"Hermes HTTP {status_code}"
    assert error.code == "transient_provider_error"
    assert error.http_status == error.as_dict()["http_status"] == status_code
    assert error.retryable is (status_code == 503)
    assert error.human_action_required is (status_code == 403)
    assert "privado" not in json.dumps(error.as_dict())
    assert response.is_closed


@pytest.mark.parametrize("wrapped", [False, True])
def test_http_json_diagnostic_redacts_and_bounds_only_expected_strings(wrapped) -> None:
    error_payload = {
        "code": "synthetic-credential-" + "C" * 500,
        "message": "Bearer synthetic-credential " + "M" * 1000,
        "private": "datos-privados-del-worker",
        "http_status": 200,
    }
    payload = {"error": error_payload} if wrapped else error_payload
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(503, json=payload))
    ) as client:
        adapter = HermesRunsAdapter("http://worker.invalid", "synthetic-credential", client=client)
        with pytest.raises(HermesAdapterError) as captured:
            adapter.stream_events("existing-run")

    error = captured.value
    assert error.http_status == error.as_dict()["http_status"] == 503
    assert len(error.code) == 100
    assert len(error.message) == 512
    assert error.code.startswith("[REDACTED]-")
    assert error.message.startswith("Bearer [REDACTED] ")
    assert "synthetic-credential" not in json.dumps(error.as_dict())
    assert "datos-privados-del-worker" not in json.dumps(error.as_dict())


def test_non_http_adapter_error_does_not_invent_status() -> None:
    error = HermesAdapterError("worker_disconnected", "SSE interrumpido", retryable=True)
    assert error.http_status is None
    assert "http_status" not in error.as_dict()


@pytest.mark.parametrize("operation", ["start", "stream"])
@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("", None),
        ("120", 120.0),
        (" 2.5 ", 2.5),
        ("0", 0.0),
        ("Thu, 10 Sep 2026 12:02:00 GMT", 120.0),
        ("Thu, 10 Sep 2026 11:59:00 GMT", 0.0),
        ("Thursday, 10-Sep-26 12:02:00 GMT", 120.0),
        ("Thu Sep 10 12:02:00 2026", 120.0),
        ("invalid-date", None),
        ("Thu, 99 Sep 2026 12:02:00 GMT", None),
        ("Thu, 10 Sep 999999999 12:02:00 GMT", None),
        ("-1", None),
        ("NaN", None),
        ("Infinity", None),
        ("-Infinity", None),
        ("1e9999", None),
    ],
)
def test_retry_after_is_normalized_without_masking_http_error(operation, header, expected) -> None:
    response = httpx.Response(
        429,
        headers={"Retry-After": header} if header is not None else {},
        stream=httpx.ByteStream(b'{"error":{"code":"rate_limited","message":"Ocupado"}}'),
    )
    with (
        patch("hermes_orchestrator.hermes_adapter.datetime") as clock,
        httpx.Client(transport=httpx.MockTransport(lambda _: response)) as client,
    ):
        clock.now.return_value = datetime(2026, 9, 10, 12, tzinfo=UTC)
        adapter = HermesRunsAdapter("http://worker.invalid", "test-token", client=client)
        with pytest.raises(HermesAdapterError) as captured:
            if operation == "start":
                adapter.start_run("test")
            else:
                adapter.stream_events("run")

    assert captured.value.code == "rate_limited"
    assert captured.value.message == "Ocupado"
    assert captured.value.retryable is True
    assert captured.value.retry_after == expected
    assert response.is_closed

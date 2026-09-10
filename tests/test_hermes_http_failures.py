"""Regresiones H1/H2 derivadas de los probes independientes de infra-review-final."""

from __future__ import annotations

import json
from collections.abc import Iterator

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.hermes_adapter import HermesAdapterError, HermesRunsAdapter
from hermes_orchestrator.models import AuditEvent, Run, RunEvent, Task, UsageLedger
from hermes_orchestrator.run_dispatcher import RunDispatcher
from tests.fakes.hermes_server import FEATURES
from tests.test_run_dispatcher import create_run, settings
from tests.test_run_dispatcher import session_factory as session_factory

HANDOFF = {"reference": "synthetic-handoff"}
PRIVATE = "synthetic-private-body"


class Body(httpx.SyncByteStream):
    def __init__(self, data: bytes, failure: type[httpx.TransportError] | None = None) -> None:
        self.data = data
        self.failure = failure
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield self.data
        if self.failure:
            raise self.failure("synthetic-private-read-detail")

    def close(self) -> None:
        self.closed = True


def failed_body_response(status_code: int, mode: str) -> httpx.Response:
    failures = {"timeout": httpx.ReadTimeout, "protocol": httpx.RemoteProtocolError}
    return httpx.Response(
        status_code,
        stream=Body(PRIVATE.encode(), failures.get(mode)),
        headers={"Content-Encoding": "gzip"} if mode == "invalid_gzip" else {},
    )


def existing_run(factory: sessionmaker[Session], attempts: int) -> Run:
    run = create_run(factory, worker_run_id="review-existing")
    with factory() as session:
        stored = session.get(Run, run.id)
        assert stored is not None
        stored.status = "running"
        stored.dispatch_attempts = attempts
        stored.error_details = {"agent_handoff": HANDOFF}
        task = session.get(Task, stored.task_id)
        assert task is not None
        task.budget = {"max_retries": 0}
        session.commit()
    return run


@pytest.mark.parametrize("status_code", [401, 403, 503])
@pytest.mark.parametrize("mode", ["timeout", "protocol", "invalid_gzip"])
def test_failed_http_body_read_preserves_auth_classification(status_code, mode) -> None:
    responses: list[httpx.Response] = []

    def handler(_: httpx.Request) -> httpx.Response:
        response = failed_body_response(status_code, mode)
        assert not response.is_stream_consumed
        responses.append(response)
        return response

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(HermesAdapterError) as captured,
    ):
        HermesRunsAdapter("http://worker.invalid", "synthetic", client=client).stream_events(
            "existing"
        )
    observed = captured.value.as_dict()
    assert len(responses) == 1
    assert responses[0].is_closed
    assert observed["http_status"] == status_code
    assert observed["retryable"] is (status_code == 503)
    assert observed["human_action_required"] is (status_code in {401, 403})
    assert observed["message"] == f"Hermes HTTP {status_code}"
    assert observed["code"] == "transient_provider_error"
    assert "synthetic-private" not in json.dumps(observed)


@pytest.mark.parametrize("mode", ["timeout", "protocol", "invalid_gzip"])
def test_running24_body_read_failure_is_controlled_and_keeps_status(session_factory, mode) -> None:
    run = existing_run(session_factory, 24)
    response = failed_body_response(503, mode)
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        assert request.method == "GET", "No se admite start ni stop"
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok"})
        if request.url.path == "/v1/capabilities":
            return httpx.Response(200, json={"features": FEATURES})
        if request.url.path == "/v1/runs/review-existing":
            return httpx.Response(200, json={"status": "running"})
        assert request.url.path == "/v1/runs/review-existing/events"
        assert not response.is_stream_consumed
        return response

    configured = settings()
    assert configured.usage_max_retries == 1
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        dispatcher = RunDispatcher(
            session_factory,
            configured,
            adapter_factory=lambda endpoint, token: HermesRunsAdapter(
                endpoint, token, client=client
            ),
        )
        result = dispatcher.run_once()[0]
        assert result.status == result.action == "failed"
        assert dispatcher.run_once() == []

    assert response.is_closed
    assert len(calls) == 4
    with session_factory() as session:
        stored = session.get(Run, run.id)
        assert stored is not None and stored.status == "failed"
        assert stored.dispatch_attempts == 25
        assert stored.worker_run_id == "review-existing"
        assert stored.lease_owner is stored.lease_expires_at is None
        assert stored.error_details["http_status"] == 503
        assert stored.error_details["agent_handoff"] == HANDOFF
        assert "synthetic-private" not in json.dumps(stored.error_details)
        claims = list(
            session.scalars(
                select(AuditEvent).where(
                    AuditEvent.event_type == "run.claimed", AuditEvent.aggregate_id == str(run.id)
                )
            )
        )
        assert len(claims) == 1 and claims[0].payload["dispatch_attempt"] == 25
        ledgers = list(session.scalars(select(UsageLedger).where(UsageLedger.run_id == run.id)))
        assert len(ledgers) == 1 and ledgers[0].outcome == "failed"


def test_retryable_error_keeps_http_diagnostic_before_retry_then_same_worker_completes(
    session_factory,
) -> None:
    run = existing_run(session_factory, 0)
    response = httpx.Response(
        503,
        stream=Body(
            json.dumps(
                {
                    "error": {
                        "code": "busy",
                        "message": "Bearer test-token",
                        "private": PRIVATE,
                    }
                }
            ).encode()
        ),
    )
    events_requested = 0
    remote_status = "running"
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal events_requested, remote_status
        calls.append((request.method, request.url.path))
        assert request.method == "GET", "No se admite start ni stop"
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "ok"})
        if request.url.path == "/v1/capabilities":
            return httpx.Response(200, json={"features": FEATURES})
        if request.url.path == "/v1/runs/review-existing":
            return httpx.Response(
                200,
                json={
                    "run_id": "review-existing",
                    "status": remote_status,
                    "output": "RECOVERED",
                    "effective_model": "gpt-5.3-codex-spark",
                    "effective_provider": "openai-api",
                    "effective_reasoning_effort": "low",
                    "usage": {"prompt_tokens": 7},
                },
            )
        assert request.url.path == "/v1/runs/review-existing/events"
        events_requested += 1
        if events_requested == 1:
            assert not response.is_stream_consumed
            return response
        remote_status = "completed"
        return httpx.Response(
            200, stream=Body(b'id: 1\nevent: run.completed\ndata: {"status":"completed"}\n\n')
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        dispatcher = RunDispatcher(
            session_factory,
            settings(),
            adapter_factory=lambda endpoint, token: HermesRunsAdapter(
                endpoint, token, client=client
            ),
        )
        assert dispatcher.run_once()[0].action == "retry_scheduled"
        with session_factory() as session:
            stored = session.get(Run, run.id)
            assert stored is not None and stored.status == "running"
            assert stored.dispatch_attempts == 1
            assert stored.worker_run_id == "review-existing"
            assert stored.lease_owner is None and stored.next_attempt_at is not None
            assert stored.error_details == {
                "code": "busy",
                "message": "Bearer [REDACTED]",
                "retryable": True,
                "http_status": 503,
                "agent_handoff": HANDOFF,
            }
            assert session.scalar(select(UsageLedger).where(UsageLedger.run_id == run.id)) is None
        assert dispatcher.run_once()[0].status == "completed"
        assert dispatcher.run_once() == []

    assert response.is_closed and events_requested == 2
    assert len(calls) == 9
    with session_factory() as session:
        stored = session.get(Run, run.id)
        assert stored is not None and stored.status == "completed"
        assert stored.dispatch_attempts == 2 and stored.worker_run_id == "review-existing"
        assert stored.lease_owner is None and stored.summary == "RECOVERED"
        # El estado terminal sustituye el diagnóstico transitorio; conserva el handoff.
        assert stored.error_details == {"agent_handoff": HANDOFF}
        assert stored.error_code is None
        task = session.get(Task, stored.task_id)
        assert task is not None and task.budget == {"max_retries": 0}
        ledger = list(session.scalars(select(UsageLedger).where(UsageLedger.run_id == run.id)))
        events = list(session.scalars(select(RunEvent).where(RunEvent.run_id == run.id)))
        assert len(ledger) == len(events) == 1
        assert ledger[0].outcome == "completed" and events[0].terminal

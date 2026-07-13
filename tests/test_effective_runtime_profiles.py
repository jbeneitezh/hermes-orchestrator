from __future__ import annotations

import json
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
    Task,
    UsageLedger,
)
from hermes_orchestrator.run_dispatcher import WORKER_SECRET_PREFIX, RunDispatcher
from tests.fakes.hermes_server import FakeHermesServer, FakeHermesState

SECRET_REF = f"{WORKER_SECRET_PREFIX}developer"


@pytest.fixture
def session_factory(tmp_path: Path) -> sessionmaker[Session]:
    engine = create_engine(f"sqlite+pysqlite:///{(tmp_path / 'runtime.db').as_posix()}")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as session:
        agent = Agent(
            slug="developer",
            role="developer",
            description="Worker de matriz F7",
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
        session.add_all(
            [
                ExecutionProfile(
                    id="spark-low",
                    provider="openai-api",
                    model="gpt-5.3-codex-spark",
                    reasoning_effort="low",
                    max_iterations=8,
                    timeout_seconds=300,
                    relative_cost=1,
                ),
                ExecutionProfile(
                    id="luna-low",
                    provider="openai-api",
                    model="gpt-5.6-luna",
                    reasoning_effort="low",
                    max_iterations=8,
                    timeout_seconds=300,
                    relative_cost=2,
                ),
            ]
        )
        session.commit()
    return factory


def create_run(
    session_factory: sessionmaker[Session],
    *,
    worker_run_id: str | None = None,
) -> Run:
    with session_factory() as session:
        task = Task(
            requester_actor_id="agent:leader",
            idempotency_key=str(uuid.uuid4()),
            request_hash="a" * 64,
            objective="Demostrar runtime efectivo",
            acceptance_criteria=["Modelo y esfuerzo quedan observados"],
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
        run_dispatcher_id="f7-matrix",
        run_dispatcher_lease_seconds=60,
        run_dispatcher_heartbeat_seconds=120,
        run_dispatcher_retry_seconds=0,
        run_dispatcher_worker_secrets={SECRET_REF: "test-token"},
        usage_max_retries=0,
    )


def execute(
    session_factory: sessionmaker[Session],
    server: FakeHermesServer,
) -> Run:
    with session_factory() as session:
        instance = session.scalar(select(AgentInstance))
        assert instance is not None
        instance.internal_endpoint = server.url
        session.commit()

    def adapter_factory(endpoint: str, token: str) -> HermesRunsAdapter:
        return HermesRunsAdapter(endpoint, token, max_reconnects=0)

    result = RunDispatcher(
        session_factory,
        settings(),
        adapter_factory=adapter_factory,
    ).run_once()[0]
    with session_factory() as session:
        run = session.get(Run, result.run_id)
        assert run is not None
        return run


def test_match_persists_requested_and_observed_runtime(session_factory) -> None:
    create_run(session_factory)
    state = FakeHermesState()
    with FakeHermesServer(state) as server:
        run = execute(session_factory, server)

    assert run.status == "completed"
    assert run.effective_profile_id == "spark-low"
    assert run.requested_runtime["model_alias"] == "gpt-5.3-codex-spark"
    assert run.observed_runtime == {
        "requested_model": "gpt-5.3-codex-spark",
        "requested_reasoning_effort": "low",
        "model": "gpt-5.3-codex-spark",
        "provider": "openai-api",
        "reasoning_effort": "low",
    }
    payload = state.start_payloads[0]
    assert payload["model"] == "gpt-5.3-codex-spark"
    assert payload["reasoning_effort"] == "low"
    assert payload["instructions"].startswith("Actua como agent:developer")
    assert payload["session_id"] == run.requested_runtime["session_id"]


def test_declared_fallback_maps_to_effective_profile(session_factory) -> None:
    create_run(session_factory)
    state = FakeHermesState(
        effective_model="gpt-5.6-luna",
        runtime_fallback={
            "applied": True,
            "reason": "capacidad temporal",
            "from_model": "gpt-5.3-codex-spark",
            "to_model": "gpt-5.6-luna",
        },
    )
    with FakeHermesServer(state) as server:
        run = execute(session_factory, server)

    assert run.status == "completed"
    assert run.effective_profile_id == "luna-low"
    assert run.runtime_fallback["applied"] is True


def test_model_mismatch_without_fallback_fails_closed(session_factory) -> None:
    create_run(session_factory)
    state = FakeHermesState(effective_model="gpt-5.6-luna")
    with FakeHermesServer(state) as server:
        run = execute(session_factory, server)

    assert run.status == "failed"
    assert run.error_code == "model_effective_unverified"
    assert run.effective_profile_id is None
    assert run.observed_runtime["model"] == "gpt-5.6-luna"


def test_effort_not_observed_fails_closed(session_factory) -> None:
    create_run(session_factory)
    state = FakeHermesState(effective_reasoning_effort=None)
    with FakeHermesServer(state) as server:
        run = execute(session_factory, server)

    assert run.status == "failed"
    assert run.error_code == "reasoning_effective_unverified"


def test_alias_unavailable_is_persisted_without_effective_profile(session_factory) -> None:
    create_run(session_factory)
    state = FakeHermesState(scenario="alias_unavailable")
    with FakeHermesServer(state) as server:
        run = execute(session_factory, server)

    assert run.status == "failed"
    assert run.error_code == "unsupported_model_route"
    assert run.effective_profile_id is None
    assert state.start_payloads[0]["model"] == "gpt-5.3-codex-spark"


def test_usage_ledger_is_self_contained_for_requested_and_observed(session_factory) -> None:
    created = create_run(session_factory)
    with FakeHermesServer() as server:
        execute(session_factory, server)
    with session_factory() as session:
        ledger = session.scalar(select(UsageLedger).where(UsageLedger.run_id == created.id))

    assert ledger is not None
    assert ledger.requested_profile == "spark-low"
    assert ledger.effective_profile == "spark-low"
    assert ledger.requested_model == "gpt-5.3-codex-spark"
    assert ledger.requested_provider == "openai-api"
    assert ledger.requested_reasoning_effort == "low"
    assert ledger.model == "gpt-5.3-codex-spark"
    assert ledger.provider == "openai-api"
    assert ledger.reasoning_effort == "low"
    assert ledger.input_tokens == 11


def test_runtime_fallback_is_redacted_before_persistence(session_factory) -> None:
    create_run(session_factory)
    state = FakeHermesState(runtime_fallback={"applied": False, "secret": "never-store"})
    with FakeHermesServer(state) as server:
        run = execute(session_factory, server)

    serialized = json.dumps(run.runtime_fallback)
    assert "never-store" not in serialized
    assert "[REDACTED]" in serialized


def test_replay_reuses_worker_run_and_runtime_without_second_post(session_factory) -> None:
    run = create_run(session_factory, worker_run_id="fake-run")
    with session_factory() as session:
        stored = session.get(Run, run.id)
        task = session.get(Task, stored.task_id) if stored is not None else None
        assert stored is not None and task is not None
        profile = session.get(ExecutionProfile, "spark-low")
        assert profile is not None
        stored.requested_runtime = {
            "profile_id": profile.id,
            "model_alias": profile.model,
            "model": profile.model,
            "provider": profile.provider,
            "reasoning_effort": profile.reasoning_effort,
            "instructions": "Instrucciones persistidas",
            "session_id": f"{stored.operation_id}:{stored.task_id}:{stored.id}",
        }
        session.commit()
    state = FakeHermesState(status="completed")
    with FakeHermesServer(state) as server:
        completed = execute(session_factory, server)

    assert completed.status == "completed"
    assert state.start_requests == 0
    assert completed.requested_runtime["instructions"] == "Instrucciones persistidas"

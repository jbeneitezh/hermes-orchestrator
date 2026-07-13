from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings, get_settings
from hermes_orchestrator.models import (
    Approval,
    Artifact,
    AuditEvent,
    Base,
    Run,
    RunEvent,
    Task,
    UsageLedger,
    WorkflowContinuation,
)
from hermes_orchestrator.workflow_services import (
    ContinuationError,
    create_workflow_continuation,
    transition_workflow_continuation,
)


@pytest.fixture
def session_factory(tmp_path: Path) -> sessionmaker[Session]:
    engine = create_engine(f"sqlite+pysqlite:///{(tmp_path / 'workflow.db').as_posix()}")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def create_event(
    factory: sessionmaker[Session],
    *,
    terminal: bool = True,
    run_status: str = "completed",
    task_status: str = "in_progress",
    event_type: str = "run.completed",
    budget: dict[str, int] | None = None,
    with_context: bool = False,
) -> uuid.UUID:
    with factory() as session:
        task = Task(
            requester_actor_id="agent:leader",
            idempotency_key=str(uuid.uuid4()),
            request_hash="a" * 64,
            objective="Decidir la siguiente tarea de Tradix",
            acceptance_criteria=["Continuación durable"],
            budget=budget or {},
            references=["docs/agents/index.md"],
            status=task_status,
            workflow_ref="tradix-autonomous-loop",
        )
        session.add(task)
        session.flush()
        run = Run(
            task_id=task.id,
            operation_id=task.operation_id,
            attempt_number=1,
            worker_actor_id="agent:researcher",
            requested_profile_id="spark-low",
            dispatch_idempotency_key=str(uuid.uuid4()),
            dispatch_hash="b" * 64,
            status=run_status,
            timeout_at=datetime.now(UTC) + timedelta(minutes=5),
            summary="Resultado compacto listo para continuar",
            usage_snapshot={"input_tokens": 11, "output_tokens": 7, "secret": "no-copiar"},
            error_details={"transcript": "no-copiar", "token": "no-copiar"},
        )
        session.add(run)
        session.flush()
        if with_context:
            session.add(
                Artifact(
                    task_id=task.id,
                    run_id=run.id,
                    kind="report",
                    uri="repo://tradix/docs/report.md",
                    sha256="c" * 64,
                    producer_actor_id="agent:researcher",
                )
            )
            session.add(
                Approval(
                    run_id=run.id,
                    action="review",
                    status="approved",
                    requested_by_actor_id="agent:researcher",
                    expires_at=datetime.now(UTC) + timedelta(hours=1),
                )
            )
            session.add(
                UsageLedger(
                    run_id=run.id,
                    payload_hash="d" * 64,
                    operation_id=task.operation_id,
                    task_id=task.id,
                    project_id="tradix",
                    category="research",
                    requesting_agent_id="agent:leader",
                    executing_agent_id="agent:researcher",
                    requested_profile="spark-low",
                    effective_profile="spark-low",
                    input_tokens=11,
                    output_tokens=7,
                    reasoning_tokens=3,
                    api_calls=1,
                    outcome="completed",
                    retry_number=0,
                )
            )
        event = RunEvent(
            run_id=run.id,
            sequence=1,
            event_type=event_type,
            payload={"transcript": "no-copiar", "api_key": "sk-no-copiar"},
            terminal=terminal,
        )
        session.add(event)
        session.commit()
        return event.id


def create_continuation(
    factory: sessionmaker[Session],
    event_id: uuid.UUID,
    *,
    target: str = "agent:developer",
    action: str = "implement",
    settings: Settings | None = None,
):
    with factory() as session:
        return create_workflow_continuation(
            session,
            trigger_event_id=event_id,
            target_actor_id=target,
            action=action,
            settings=settings,
        )


def test_creacion_persiste_snapshot_compacto_y_estados(session_factory) -> None:
    event_id = create_event(session_factory, with_context=True)
    result = create_continuation(session_factory, event_id)

    assert result.created is True
    assert result.continuation.status == "pending"
    snapshot = result.continuation.context_snapshot
    assert snapshot["result"]["usage"]["input_tokens"] == 11
    assert snapshot["result"]["artifacts"][0]["kind"] == "report"
    assert snapshot["result"]["approvals"] == [{"action": "review", "status": "approved"}]
    serialized = json.dumps(snapshot)
    assert "no-copiar" not in serialized
    assert "transcript" not in serialized
    assert snapshot["budget"]["remaining_continuations"] == 2

    with session_factory() as session:
        dispatched = transition_workflow_continuation(
            session,
            continuation_id=result.continuation.id,
            new_status="dispatched",
            actor_id="system:coordinator",
        )
        assert dispatched.dispatched_at is not None
        failed = transition_workflow_continuation(
            session,
            continuation_id=result.continuation.id,
            new_status="failed",
            failure_code="dispatcher_unavailable",
            actor_id="system:coordinator",
        )
        assert failed.status == "failed"
        assert failed.failure_code == "dispatcher_unavailable"
        assert failed.failed_at is not None


def test_duplicado_reutiliza_continuacion_y_audita_replay(session_factory) -> None:
    event_id = create_event(session_factory)
    first = create_continuation(session_factory, event_id)
    replay = create_continuation(session_factory, event_id)

    with session_factory() as session:
        rows = session.scalars(select(WorkflowContinuation)).all()
        audits = session.scalars(
            select(AuditEvent).where(AuditEvent.event_type == "workflow.continuation_replayed")
        ).all()
    assert replay.created is False
    assert replay.continuation.id == first.continuation.id
    assert len(rows) == 1
    assert len(audits) == 1


def test_actor_distinto_admite_otra_continuacion_y_valida_transiciones(session_factory) -> None:
    event_id = create_event(session_factory)
    first = create_continuation(session_factory, event_id, target="agent:developer")
    second = create_continuation(session_factory, event_id, target="agent:validator")
    assert first.continuation.id != second.continuation.id

    with session_factory() as session:
        with pytest.raises(ContinuationError, match="Estado") as invalid_status:
            transition_workflow_continuation(
                session,
                continuation_id=second.continuation.id,
                new_status="unknown",
                actor_id="system:test",
            )
        with pytest.raises(ContinuationError, match="Transición") as invalid_transition:
            transition_workflow_continuation(
                session,
                continuation_id=second.continuation.id,
                new_status="pending",
                actor_id="system:test",
            )
        with pytest.raises(ContinuationError, match="no existe") as missing:
            transition_workflow_continuation(
                session,
                continuation_id=uuid.uuid4(),
                new_status="failed",
                actor_id="system:test",
            )
    assert invalid_status.value.code == "invalid_continuation_status"
    assert invalid_transition.value.code == "invalid_continuation_transition"
    assert missing.value.code == "continuation_not_found"


def test_evento_no_terminal_no_crea_continuacion(session_factory) -> None:
    event_id = create_event(session_factory, terminal=False, run_status="running")
    with pytest.raises(ContinuationError, match="no es terminal") as denied:
        create_continuation(session_factory, event_id)
    with session_factory() as session:
        assert session.scalar(select(WorkflowContinuation)) is None
        audit = session.scalar(select(AuditEvent))
        assert audit is not None
        assert audit.payload["reason"] == "event_not_terminal"
        with pytest.raises(ContinuationError, match="no existe") as missing:
            create_workflow_continuation(
                session,
                trigger_event_id=uuid.uuid4(),
                target_actor_id="agent:developer",
                action="implement",
            )
    assert denied.value.code == "event_not_terminal"
    assert missing.value.code == "event_not_found"


def test_budget_agotado_bloquea_nuevo_fan_out(session_factory) -> None:
    event_id = create_event(session_factory, budget={"max_fan_out": 1})
    create_continuation(session_factory, event_id, target="agent:developer")
    with pytest.raises(ContinuationError, match="agotado") as denied:
        create_continuation(session_factory, event_id, target="agent:validator")

    with session_factory() as session:
        rows = session.scalars(select(WorkflowContinuation)).all()
        audit = session.scalar(
            select(AuditEvent).where(AuditEvent.event_type == "workflow.continuation_skipped")
        )
    assert denied.value.code == "workflow_budget_exhausted"
    assert len(rows) == 1
    assert audit is not None
    assert audit.payload["reason"] == "workflow_budget_exhausted"


def test_cancelacion_no_genera_continuacion(session_factory) -> None:
    for task_status, run_status, event_type in (
        ("cancelled", "completed", "run.completed"),
        ("in_progress", "cancelled", "run.failed"),
        ("in_progress", "completed", "run.cancelled"),
    ):
        event_id = create_event(
            session_factory,
            task_status=task_status,
            run_status=run_status,
            event_type=event_type,
        )
        with pytest.raises(ContinuationError, match="cancelado") as denied:
            create_continuation(session_factory, event_id)
        assert denied.value.code == "workflow_cancelled"
    with session_factory() as session:
        assert session.scalar(select(WorkflowContinuation)) is None


def test_migracion_admite_upgrade_downgrade_upgrade(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "alembic-workflow.db"
    database_url = f"sqlite+pysqlite:///{database_path.as_posix()}"
    monkeypatch.setenv("HERMES_ORCHESTRATOR_DATABASE_URL", database_url)
    get_settings.cache_clear()
    config = Config(str(Path(__file__).parents[1] / "alembic.ini"))
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    WorkflowContinuation.__table__.drop(engine)
    command.stamp(config, "20260713_0009")

    command.upgrade(config, "head")
    assert "workflow_continuations" in inspect(engine).get_table_names()
    unique = inspect(engine).get_unique_constraints("workflow_continuations")
    assert any(item["name"] == "uq_workflow_continuations_trigger_actor_action" for item in unique)

    command.downgrade(config, "20260713_0009")
    assert "workflow_continuations" not in inspect(engine).get_table_names()
    command.upgrade(config, "head")
    assert "workflow_continuations" in inspect(engine).get_table_names()
    engine.dispose()
    get_settings.cache_clear()

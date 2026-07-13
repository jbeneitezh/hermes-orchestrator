from __future__ import annotations

import threading
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.models import (
    Agent,
    AuditEvent,
    Base,
    CircuitBreaker,
    CommunicationEdge,
    ExecutionProfile,
    Run,
    RunEvent,
    Task,
    UsageLedger,
    WorkflowContinuation,
)
from hermes_orchestrator.workflow_coordinator import (
    CoordinatorError,
    WorkflowCoordinator,
    _agent,
    _directive,
    _max_depth,
    _parent_target,
    _persisted_directive,
    _workflow_depth,
)


@pytest.fixture
def coordinator_context(tmp_path: Path):
    engine = create_engine(f"sqlite+pysqlite:///{(tmp_path / 'coordinator.db').as_posix()}")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as session:
        leader = Agent(
            slug="leader",
            role="leader",
            description="Líder de pruebas",
            desired_state="active",
            owner_actor_id="user:owner",
        )
        researcher = Agent(
            slug="researcher",
            role="researcher",
            description="Investigador de pruebas",
            desired_state="active",
            owner_actor_id="user:owner",
        )
        developer = Agent(
            slug="developer",
            role="developer",
            description="Desarrollador sin edge de retorno",
            desired_state="active",
            owner_actor_id="user:owner",
        )
        session.add_all([leader, researcher, developer])
        session.flush()
        session.add(
            CommunicationEdge(
                source_agent_id=researcher.id,
                target_agent_id=leader.id,
                task_classes=["visibility", "research_handoff"],
                scopes=["read", "tradix"],
                approved_by_actor_id="user:owner",
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
    settings = Settings(
        environment="test",
        workflow_coordinator_id="test-coordinator",
        workflow_coordinator_batch_size=20,
        workflow_max_depth=2,
        usage_max_concurrent_runs=20,
        usage_max_fan_out=3,
        usage_max_retries=1,
    )
    yield factory, settings
    engine.dispose()


def create_chain(
    factory: sessionmaker[Session],
    *,
    outcome: str = "completed",
    source_actor: str = "agent:researcher",
    budget: dict[str, object] | None = None,
    parent: Task | None = None,
) -> tuple[Task, Task, RunEvent]:
    with factory() as session:
        root = parent
        if root is None:
            root = Task(
                requester_actor_id="user:owner",
                assignee_actor_id="agent:leader",
                idempotency_key=str(uuid.uuid4()),
                request_hash="a" * 64,
                objective="Dirigir el workflow Tradix",
                acceptance_criteria=["Delegar y consolidar"],
                workflow_ref="tradix-autonomous-loop",
                status="completed",
                references=["docs/agents/index.md"],
            )
            session.add(root)
            session.flush()
            session.add(
                Run(
                    task_id=root.id,
                    operation_id=root.operation_id,
                    attempt_number=1,
                    worker_actor_id="agent:leader",
                    requested_profile_id="spark-low",
                    effective_profile_id="spark-low",
                    dispatch_idempotency_key=str(uuid.uuid4()),
                    dispatch_hash="b" * 64,
                    status="completed",
                    timeout_at=datetime.now(UTC) + timedelta(minutes=5),
                    finished_at=datetime.now(UTC),
                    summary="Delegación creada",
                )
            )
        else:
            root = session.merge(root)
        task_status = "blocked" if outcome == "blocked" else outcome
        child_budget: dict[str, object] = {
            "workflow_auto_continue": True,
            "workflow_scope": "tradix",
            "workflow_handoff_class": "research_handoff",
        }
        child_budget.update(budget or {})
        child = Task(
            operation_id=root.operation_id,
            parent_task_id=root.id,
            requester_actor_id="agent:leader",
            assignee_actor_id=source_actor,
            idempotency_key=str(uuid.uuid4()),
            request_hash="c" * 64,
            objective="Investigar una hipótesis reproducible",
            acceptance_criteria=["Entregar evidencia"],
            workflow_ref="tradix-autonomous-loop",
            status=task_status,
            budget=child_budget,
            references=["docs/contracts/research.md"],
        )
        session.add(child)
        session.flush()
        error_details = (
            {
                "agent_handoff": {
                    "outcome": "blocked",
                    "summary": "Falta confirmar el universo",
                    "needed_action": "Resolver alcance",
                }
            }
            if outcome == "blocked"
            else {}
        )
        run_status = "completed" if outcome == "blocked" else outcome
        run = Run(
            task_id=child.id,
            operation_id=child.operation_id,
            attempt_number=1,
            worker_actor_id=source_actor,
            requested_profile_id="spark-low",
            effective_profile_id="spark-low",
            dispatch_idempotency_key=str(uuid.uuid4()),
            dispatch_hash="d" * 64,
            status=run_status,
            timeout_at=datetime.now(UTC) + timedelta(minutes=5),
            finished_at=datetime.now(UTC),
            summary=f"Resultado terminal {outcome}",
            error_code="provider_failed" if outcome == "failed" else None,
            error_details=error_details,
            usage_snapshot={"input_tokens": 5, "output_tokens": 3},
        )
        session.add(run)
        session.flush()
        event = RunEvent(
            run_id=run.id,
            sequence=1,
            event_type=f"run.{run_status}",
            payload={"transcript": "no debe propagarse"},
            terminal=True,
        )
        session.add(event)
        session.commit()
        return root, child, event


def coordinator(factory, settings) -> WorkflowCoordinator:
    return WorkflowCoordinator(factory, settings)


def test_completed_crea_y_despacha_sucesor_compacto(coordinator_context) -> None:
    factory, settings = coordinator_context
    _, child, _ = create_chain(factory)
    result = coordinator(factory, settings).run_once()

    assert len(result) == 1
    assert result[0].action == "successor_dispatched"
    with factory() as session:
        continuation = session.scalar(select(WorkflowContinuation))
        successor = session.get(Task, result[0].task_id)
        run = session.get(Run, result[0].run_id)
    assert continuation is not None and continuation.status == "dispatched"
    assert successor is not None and successor.parent_task_id == child.id
    assert successor.operation_id == child.operation_id
    assert successor.assignee_actor_id == "agent:leader"
    assert successor.budget["workflow_depth"] == 2
    assert successor.budget["workflow_auto_continue"] is True
    assert run is not None and run.status == "dispatching"
    assert run.worker_actor_id == "agent:leader"
    assert "transcript" not in str(continuation.context_snapshot)


def test_blocked_reactiva_lider_con_contexto_de_bloqueo(coordinator_context) -> None:
    factory, settings = coordinator_context
    _, _, _ = create_chain(factory, outcome="blocked")
    result = coordinator(factory, settings).run_once()[0]

    with factory() as session:
        continuation = session.get(WorkflowContinuation, result.continuation_id)
        successor = session.get(Task, result.task_id)
    assert continuation is not None and continuation.action == "resume_blocked"
    assert successor is not None and "blocked" in successor.objective
    assert "Resultado terminal blocked" in successor.objective


def test_failed_reintenta_la_misma_task_una_sola_vez(coordinator_context) -> None:
    factory, settings = coordinator_context
    _, child, _ = create_chain(factory, outcome="failed")
    result = coordinator(factory, settings).run_once()[0]

    with factory() as session:
        runs = list(select_runs(session, child.id))
        continuation = session.get(WorkflowContinuation, result.continuation_id)
    assert result.action == "retry_dispatched"
    assert result.task_id == child.id
    assert [run.attempt_number for run in runs] == [1, 2]
    assert runs[-1].status == "dispatching"
    assert continuation is not None and continuation.action == "retry_failed"

    with factory() as session:
        retry = runs[-1]
        retry.status = "failed"
        retry.finished_at = datetime.now(UTC)
        child_row = session.get(Task, child.id)
        assert child_row is not None
        child_row.status = "failed"
        session.add(
            RunEvent(
                run_id=retry.id,
                sequence=1,
                event_type="run.failed",
                payload={"error": {"code": "provider_failed"}},
                terminal=True,
            )
        )
        session.commit()
    recovery = coordinator(factory, settings).run_once()[0]
    assert recovery.action == "successor_dispatched"
    assert recovery.task_id != child.id


def select_runs(session: Session, task_id: uuid.UUID):
    return session.scalars(select(Run).where(Run.task_id == task_id).order_by(Run.attempt_number))


def test_duplicate_event_y_restart_no_duplican_task_ni_run(
    coordinator_context, monkeypatch
) -> None:
    factory, settings = coordinator_context
    create_chain(factory)
    process = coordinator(factory, settings)
    from hermes_orchestrator import workflow_coordinator

    real_transition = workflow_coordinator.transition_workflow_continuation

    def simulate_crash(*args, **kwargs):
        raise RuntimeError("crash después del dispatch")

    monkeypatch.setattr(workflow_coordinator, "transition_workflow_continuation", simulate_crash)
    with pytest.raises(RuntimeError, match="después del dispatch"):
        process.run_once()
    monkeypatch.setattr(workflow_coordinator, "transition_workflow_continuation", real_transition)
    recovered = process.run_once()
    idle = process.run_once()

    with factory() as session:
        continuations = session.scalar(select(func.count(WorkflowContinuation.id)))
        generated_tasks = session.scalar(
            select(func.count(Task.id)).where(Task.requester_actor_id.like("system:%"))
        )
        generated_runs = session.scalar(
            select(func.count(Run.id)).join(Task).where(Task.requester_actor_id.like("system:%"))
        )
    assert len(recovered) == 1
    assert recovered[0].status == "dispatched"
    assert idle == []
    assert continuations == 1
    assert generated_tasks == 1
    assert generated_runs == 1


def test_depth_excedida_falla_antes_de_crear_task_o_run(coordinator_context) -> None:
    factory, settings = coordinator_context
    root, middle, _ = create_chain(factory)
    with factory() as session:
        middle = session.get(Task, middle.id)
        assert middle is not None
        middle.budget["workflow_auto_continue"] = False
        session.commit()
    _, source, _ = create_chain(factory, parent=middle, budget={"workflow_max_depth": 2})
    create_chain(
        factory,
        parent=source,
        outcome="failed",
        budget={"workflow_max_depth": 2},
    )
    before = counts(factory)
    result = coordinator(factory, settings).run_once()

    failed = [item for item in result if item.code == "workflow_depth_exceeded"]
    after = counts(factory)
    assert len(failed) == 2
    assert all(item.status == "failed" for item in failed)
    assert after[0] == before[0]
    assert after[1] == before[1]
    assert source.parent_task_id == middle.id
    assert root.id != middle.id


def test_fan_out_falla_antes_de_crear_otro_run(coordinator_context) -> None:
    factory, settings = coordinator_context
    _, source, _ = create_chain(factory, budget={"max_fan_out": 1})
    with factory() as session:
        active_child = Task(
            operation_id=source.operation_id,
            parent_task_id=source.id,
            requester_actor_id="agent:leader",
            assignee_actor_id="agent:leader",
            idempotency_key=str(uuid.uuid4()),
            request_hash="e" * 64,
            objective="Trabajo hermano activo",
            acceptance_criteria=["Permanecer activo"],
            workflow_ref=source.workflow_ref,
            budget={"max_fan_out": 1},
        )
        session.add(active_child)
        session.flush()
        session.add(
            Run(
                task_id=active_child.id,
                operation_id=active_child.operation_id,
                attempt_number=1,
                worker_actor_id="agent:leader",
                requested_profile_id="spark-low",
                dispatch_idempotency_key=str(uuid.uuid4()),
                dispatch_hash="f" * 64,
                status="running",
                timeout_at=datetime.now(UTC) + timedelta(minutes=5),
            )
        )
        session.commit()
    before_runs = counts(factory)[1]
    result = coordinator(factory, settings).run_once()[0]

    assert result.code == "fan_out_limit_exceeded"
    assert counts(factory)[1] == before_runs


def test_budget_circuito_y_edge_denegados_antes_del_run(coordinator_context) -> None:
    factory, settings = coordinator_context
    _, budget_task, _ = create_chain(
        factory,
        budget={"hard_token_limit": 1, "workflow_profile_id": "luna-low"},
    )
    _, circuit_task, _ = create_chain(factory)
    _, edge_task, _ = create_chain(factory, source_actor="agent:developer")
    create_chain(factory, budget={"max_fan_out": 0})
    _, no_directive_task, no_directive_event = create_chain(
        factory,
        budget={"workflow_auto_continue": False},
    )
    with factory() as session:
        budget_run = session.scalar(select(Run).where(Run.task_id == budget_task.id))
        assert budget_run is not None
        session.add(
            UsageLedger(
                run_id=budget_run.id,
                payload_hash="1" * 64,
                operation_id=budget_task.operation_id,
                task_id=budget_task.id,
                project_id="tradix",
                category="tradix-autonomous-loop",
                requesting_agent_id="agent:leader",
                executing_agent_id="agent:researcher",
                requested_profile="spark-low",
                effective_profile="spark-low",
                input_tokens=1,
                output_tokens=0,
                outcome="completed",
                retry_number=0,
            )
        )
        session.add(
            CircuitBreaker(
                worker_actor_id="agent:leader",
                profile_id="spark-low",
                state="open",
                consecutive_failures=3,
                reset_eligible_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
        session.add_all(
            [
                WorkflowContinuation(
                    operation_id=budget_task.operation_id,
                    parent_task_id=budget_task.id,
                    trigger_event_id=uuid.uuid4(),
                    target_actor_id="agent:leader",
                    action="resume_completed",
                    status="pending",
                    idempotency_key=str(uuid.uuid4()),
                    context_snapshot={},
                ),
                WorkflowContinuation(
                    operation_id=no_directive_task.operation_id,
                    parent_task_id=no_directive_task.id,
                    trigger_event_id=no_directive_event.id,
                    target_actor_id="agent:leader",
                    action="resume_completed",
                    status="pending",
                    idempotency_key=str(uuid.uuid4()),
                    context_snapshot={},
                ),
            ]
        )
        session.commit()
    before_runs = counts(factory)[1]
    results = coordinator(factory, settings).run_once()
    failures = {item.code for item in results if item.code is not None}

    assert "budget_hard_exceeded" in failures
    assert "circuit_open" in failures
    assert "workflow_communication_denied" in failures
    assert "event_not_found" in failures
    assert "workflow_directive_missing" in failures
    assert counts(factory)[1] == before_runs
    assert budget_task.id != circuit_task.id != edge_task.id


def test_idle_no_crea_tasks_runs_audits_ni_model_calls(coordinator_context) -> None:
    factory, settings = coordinator_context
    process = coordinator(factory, settings)
    stop = threading.Event()
    stop.set()
    assert process.run_once(stop) == []
    assert process._process(uuid.uuid4()).status == "ignored"

    with factory() as session:
        root = Task(
            requester_actor_id="user:owner",
            idempotency_key=str(uuid.uuid4()),
            request_hash="9" * 64,
            objective="Task defensiva",
            acceptance_criteria=["No activar modelos"],
            budget={"workflow_max_depth": 1},
        )
        session.add(root)
        session.flush()
        assert _parent_target(session, root) is None
        assert _max_depth(root, settings) == 1
        root_run = Run(
            task_id=root.id,
            operation_id=root.operation_id,
            attempt_number=1,
            worker_actor_id="agent:leader",
            requested_profile_id="spark-low",
            dispatch_idempotency_key=str(uuid.uuid4()),
            dispatch_hash="4" * 64,
            status="completed",
            timeout_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        session.add(root_run)
        session.flush()
        root_event = RunEvent(
            run_id=root_run.id,
            sequence=1,
            event_type="run.completed",
            terminal=True,
        )
        session.add(root_event)
        session.flush()
        assert _directive(session, root_event, settings) is None
        with pytest.raises(CoordinatorError) as invalid_actor:
            _agent(session, "user:owner")
        with pytest.raises(CoordinatorError) as missing_actor:
            _agent(session, "agent:missing")

        broken = Task(
            parent_task_id=uuid.uuid4(),
            requester_actor_id="agent:leader",
            idempotency_key=str(uuid.uuid4()),
            request_hash="8" * 64,
            objective="Cadena rota",
            acceptance_criteria=["Fallar cerrado"],
        )
        no_actor_parent = Task(
            requester_actor_id="user:owner",
            idempotency_key=str(uuid.uuid4()),
            request_hash="7" * 64,
            objective="Padre sin agente",
            acceptance_criteria=["Fallar cerrado"],
        )
        session.add_all([broken, no_actor_parent])
        session.flush()
        no_actor_child = Task(
            parent_task_id=no_actor_parent.id,
            requester_actor_id="user:owner",
            idempotency_key=str(uuid.uuid4()),
            request_hash="6" * 64,
            objective="Hijo sin actor",
            acceptance_criteria=["Fallar cerrado"],
        )
        cycle = Task(
            requester_actor_id="agent:leader",
            idempotency_key=str(uuid.uuid4()),
            request_hash="5" * 64,
            objective="Ciclo",
            acceptance_criteria=["Fallar cerrado"],
        )
        session.add_all([no_actor_child, cycle])
        session.flush()
        cycle.parent_task_id = cycle.id
        with pytest.raises(CoordinatorError):
            _parent_target(session, broken)
        with pytest.raises(CoordinatorError):
            _parent_target(session, no_actor_child)
        with pytest.raises(CoordinatorError):
            _workflow_depth(session, broken)
        with pytest.raises(CoordinatorError):
            _workflow_depth(session, cycle)
        with pytest.raises(CoordinatorError) as missing_directive:
            _persisted_directive(
                WorkflowContinuation(
                    operation_id=root.operation_id,
                    parent_task_id=root.id,
                    trigger_event_id=uuid.uuid4(),
                    target_actor_id="agent:leader",
                    action="resume_completed",
                    idempotency_key=str(uuid.uuid4()),
                    context_snapshot={},
                )
            )
        with pytest.raises(CoordinatorError) as invalid_directive:
            _persisted_directive(
                WorkflowContinuation(
                    operation_id=root.operation_id,
                    parent_task_id=root.id,
                    trigger_event_id=uuid.uuid4(),
                    target_actor_id="agent:leader",
                    action="resume_completed",
                    idempotency_key=str(uuid.uuid4()),
                    context_snapshot={"directive": {"outcome": "completed"}},
                )
            )
        session.rollback()
    assert invalid_actor.value.code == "workflow_target_invalid"
    assert missing_actor.value.code == "workflow_target_unavailable"
    assert missing_directive.value.code == "workflow_directive_missing"
    assert invalid_directive.value.code == "workflow_directive_invalid"

    before = counts(factory)
    result = process.run_once()
    after = counts(factory)

    assert result == []
    assert after == before
    with factory() as session:
        assert session.scalar(select(func.count(AuditEvent.id))) == 0


def counts(factory: sessionmaker[Session]) -> tuple[int, int, int]:
    with factory() as session:
        return (
            session.scalar(select(func.count(Task.id))) or 0,
            session.scalar(select(func.count(Run.id))) or 0,
            session.scalar(select(func.count(WorkflowContinuation.id))) or 0,
        )

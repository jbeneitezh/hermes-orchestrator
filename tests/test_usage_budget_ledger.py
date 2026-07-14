from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import (
    Agent,
    AuditEvent,
    Base,
    Budget,
    ExecutionProfile,
    Run,
    Task,
    UsageLedger,
)
from hermes_orchestrator.task_services import transition_run
from tests.auth_helpers import auth_headers, seed_active_auth_agents, token_settings

LEADER = auth_headers("agent:leader")
OPERATOR = auth_headers("agent:operator")


@contextmanager
def usage_context(tmp_path: Path, **overrides: Any):
    database_path = tmp_path / f"usage-{uuid.uuid4()}.db"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        usage_max_concurrent_runs=10,
        usage_max_fan_out=3,
        usage_soft_token_limit=5_000_000,
        usage_hard_token_limit=10_000_000,
        **token_settings("agent:leader", "agent:operator"),
        **overrides,
    )
    app = create_app(settings)
    Base.metadata.create_all(app.state.engine)
    factory: sessionmaker[Session] = app.state.session_factory
    seed_active_auth_agents(factory, settings)
    with TestClient(app) as client:
        yield client, factory, settings


def create_task(
    client: TestClient,
    key: str,
    *,
    budget: dict[str, int] | None = None,
    parent_task_id: str | None = None,
) -> dict[str, Any]:
    response = client.post(
        "/v1/tasks",
        headers=LEADER | {"Idempotency-Key": key},
        json={
            "objective": f"Objetivo {key}",
            "acceptance_criteria": ["Resultado verificable"],
            "assignee_actor_id": "agent:developer",
            "budget": budget or {},
            "parent_task_id": parent_task_id,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def dispatch(client: TestClient, task_id: str, key: str):
    return client.post(
        f"/v1/tasks/{task_id}/dispatch",
        headers=LEADER | {"Idempotency-Key": key},
        json={
            "worker_actor_id": "agent:developer",
            "requested_profile_id": "spark-low",
            "timeout_seconds": 900,
        },
    )


def finish_run(
    factory: sessionmaker[Session],
    settings: Settings,
    run_id: str,
    *,
    status: str,
    usage: dict[str, Any] | None = None,
    error_code: str | None = None,
    error_details: dict[str, Any] | None = None,
) -> Run:
    with factory() as session:
        transition_run(
            session,
            run_id=uuid.UUID(run_id),
            new_status="running",
            actor_id="system:test",
            settings=settings,
        )
        run = session.get(Run, uuid.UUID(run_id))
        assert run is not None
        run.usage_snapshot = usage or {}
        run.error_details = error_details or {}
        session.commit()
        return transition_run(
            session,
            run_id=run.id,
            new_status=status,
            actor_id="system:test",
            error_code=error_code,
            settings=settings,
        )


def test_exact_rollup_detail_and_unknown_are_preserved(tmp_path: Path) -> None:
    with usage_context(tmp_path) as (client, factory, settings):
        first_task = create_task(client, "usage-exact-task-01")
        first = dispatch(client, first_task["id"], "usage-exact-run-01").json()["run"]
        finish_run(
            factory,
            settings,
            first["id"],
            status="completed",
            usage={
                "input_tokens": 100,
                "output_tokens": 20,
                "reasoning_tokens": 5,
                "cache_read_tokens": 10,
                "cache_write_tokens": 2,
                "api_calls": 3,
                "actual_cost": "0.125",
                "currency": "USD",
                "cost_source": "provider",
            },
        )
        second_task = create_task(client, "usage-exact-task-02")
        with factory() as session:
            task = session.get(Task, uuid.UUID(second_task["id"]))
            assert task is not None
            task.operation_id = uuid.UUID(first_task["operation_id"])
            session.commit()
        second = dispatch(client, second_task["id"], "usage-exact-run-02").json()["run"]
        finish_run(
            factory,
            settings,
            second["id"],
            status="completed",
            usage={
                "input_tokens": 50,
                "output_tokens": 10,
                "reasoning_tokens": 3,
                "cache_read_tokens": 4,
                "cache_write_tokens": 1,
                "api_calls": 2,
                "actual_cost": "0.075",
                "currency": "USD",
            },
        )
        summary = client.get(
            "/v1/usage/summary",
            headers=LEADER,
            params={"operation_id": first_task["operation_id"]},
        )
        detail = client.get(f"/v1/usage/runs/{first['id']}", headers=LEADER)

        assert summary.status_code == 200
        group = summary.json()["groups"][0]
        assert group["runs"] == 2
        assert group["tokens"] == {
            "input_tokens": 150,
            "output_tokens": 30,
            "reasoning_tokens": 8,
            "cache_read_tokens": 14,
            "cache_write_tokens": 3,
        }
        assert Decimal(group["actual_cost"]) == Decimal("0.200000")
        assert group["api_calls"] == 5
        assert detail.status_code == 200
        assert detail.json()["cost_status"] == "known"

        unknown_task = create_task(client, "usage-unknown-task")
        unknown_run = dispatch(client, unknown_task["id"], "usage-unknown-run").json()["run"]
        finish_run(
            factory,
            settings,
            unknown_run["id"],
            status="completed",
            usage={"input_tokens": 7, "output_tokens": 1, "reasoning_tokens": "unknown"},
        )
        unknown = client.get(
            "/v1/usage/summary",
            headers=LEADER,
            params={"operation_id": unknown_task["operation_id"]},
        ).json()["groups"][0]
        assert unknown["tokens"]["reasoning_tokens"] is None
        assert unknown["unknown_tokens"]["reasoning_tokens"] == 1
        assert unknown["actual_cost"] is None
        assert unknown["cost_status"] == "unknown"


def test_retry_is_bounded_without_duplicating_task_or_ledger(tmp_path: Path) -> None:
    with usage_context(tmp_path) as (client, factory, settings):
        task = create_task(client, "usage-retry-task", budget={"max_retries": 1})
        first = dispatch(client, task["id"], "usage-retry-run-01").json()["run"]
        finish_run(
            factory,
            settings,
            first["id"],
            status="failed",
            error_code="transient_provider_error",
        )
        second_response = dispatch(client, task["id"], "usage-retry-run-02")
        second = second_response.json()["run"]
        finish_run(
            factory,
            settings,
            second["id"],
            status="failed",
            error_code="transient_provider_error",
        )
        denied = dispatch(client, task["id"], "usage-retry-run-03")

        with factory() as session:
            task_count = session.scalar(select(func.count()).select_from(Task))
            ledger_count = session.scalar(select(func.count()).select_from(UsageLedger))
        assert second_response.status_code == 202
        assert denied.status_code == 409
        assert denied.json()["detail"]["code"] == "retry_limit_exceeded"
        assert task_count == 1
        assert ledger_count == 2


def test_soft_notice_allows_and_hard_budget_denies_before_dispatch(tmp_path: Path) -> None:
    with usage_context(tmp_path) as (client, factory, _):
        soft = create_task(
            client,
            "usage-soft-task",
            budget={"soft_token_limit": 10, "hard_token_limit": 100, "estimated_tokens": 10},
        )
        soft_response = dispatch(client, soft["id"], "usage-soft-run")
        hard = create_task(
            client,
            "usage-hard-task",
            budget={"soft_token_limit": 10, "hard_token_limit": 100, "estimated_tokens": 100},
        )
        hard_response = dispatch(client, hard["id"], "usage-hard-run")
        with factory() as session:
            soft_audit = session.scalar(
                select(AuditEvent).where(
                    AuditEvent.event_type == "budget.soft_exceeded",
                    AuditEvent.aggregate_id == soft["id"],
                )
            )
            hard_run_count = session.scalar(
                select(func.count()).select_from(Run).where(Run.task_id == uuid.UUID(hard["id"]))
            )
        assert soft_response.status_code == 202
        assert soft_audit is not None
        assert hard_response.status_code == 409
        assert hard_response.json()["detail"]["code"] == "budget_hard_exceeded"
        assert hard_run_count == 0


def test_program_profile_policy_denies_disabled_unallowlisted_and_above_high(
    tmp_path: Path,
) -> None:
    with usage_context(tmp_path) as (client, factory, _):
        with factory() as session:
            session.add(
                Agent(
                    slug="developer",
                    role="developer",
                    description="Fixture policy v3",
                    desired_state="active",
                    owner_actor_id="user:owner",
                    policy_set={"allowed_profiles": ["sol-high"]},
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
                        enabled=False,
                    ),
                    ExecutionProfile(
                        id="terra-medium",
                        provider="openai-api",
                        model="gpt-5.6-terra",
                        reasoning_effort="medium",
                        max_iterations=20,
                        timeout_seconds=900,
                        relative_cost=3,
                    ),
                    ExecutionProfile(
                        id="forbidden-max",
                        provider="openai-api",
                        model="gpt-test-max",
                        reasoning_effort="max",
                        max_iterations=1,
                        timeout_seconds=60,
                        relative_cost=9,
                    ),
                ]
            )
            session.commit()

        disabled_task = create_task(client, "policy-disabled-spark")
        disabled = dispatch(client, disabled_task["id"], "policy-disabled-spark-run")
        assert disabled.status_code == 422
        assert disabled.json()["detail"]["code"] == "execution_profile_unavailable"

        unallowlisted_task = create_task(client, "policy-unallowlisted-terra")
        unallowlisted = client.post(
            f"/v1/tasks/{unallowlisted_task['id']}/dispatch",
            headers=LEADER | {"Idempotency-Key": "policy-unallowlisted-terra-run"},
            json={
                "worker_actor_id": "agent:developer",
                "requested_profile_id": "terra-medium",
                "timeout_seconds": 900,
            },
        )
        assert unallowlisted.status_code == 403
        assert unallowlisted.json()["detail"]["code"] == "execution_profile_denied"

        with factory() as session:
            developer = session.scalar(select(Agent).where(Agent.slug == "developer"))
            assert developer is not None
            developer.policy_set = {"allowed_profiles": ["forbidden-max"]}
            session.commit()
        excessive_task = create_task(client, "policy-effort-max")
        excessive = client.post(
            f"/v1/tasks/{excessive_task['id']}/dispatch",
            headers=LEADER | {"Idempotency-Key": "policy-effort-max-run"},
            json={
                "worker_actor_id": "agent:developer",
                "requested_profile_id": "forbidden-max",
                "timeout_seconds": 900,
            },
        )
        assert excessive.status_code == 403
        assert excessive.json()["detail"]["code"] == "reasoning_effort_denied"

        with factory() as session:
            assert session.scalar(select(func.count()).select_from(Run)) == 0


def test_quota_denial_returns_retry_after_and_resets_by_time(tmp_path: Path) -> None:
    with usage_context(tmp_path) as (client, factory, settings):
        quota_task = create_task(client, "usage-quota-source")
        quota_run = dispatch(client, quota_task["id"], "usage-quota-source-run").json()["run"]
        finish_run(
            factory,
            settings,
            quota_run["id"],
            status="failed",
            error_code="quota_exhausted",
            error_details={"retry_after": 120},
        )
        blocked_task = create_task(client, "usage-quota-blocked")
        blocked = dispatch(client, blocked_task["id"], "usage-quota-blocked-run")
        assert blocked.status_code == 429
        assert blocked.json()["detail"]["code"] == "quota_exhausted"
        assert int(blocked.headers["Retry-After"]) > 0

        with factory() as session:
            entry = session.scalar(
                select(UsageLedger).where(UsageLedger.run_id == uuid.UUID(quota_run["id"]))
            )
            assert entry is not None
            entry.quota_reset_at = datetime(2020, 1, 1, tzinfo=UTC)
            session.commit()
        allowed = dispatch(client, blocked_task["id"], "usage-quota-after-reset")
        assert allowed.status_code == 202


def test_circuit_opens_and_only_operator_can_reset_it(tmp_path: Path) -> None:
    with usage_context(tmp_path, usage_circuit_failure_threshold=3) as (
        client,
        factory,
        settings,
    ):
        for index in range(3):
            task = create_task(client, f"usage-circuit-task-{index}")
            run = dispatch(client, task["id"], f"usage-circuit-run-{index}").json()["run"]
            finish_run(
                factory,
                settings,
                run["id"],
                status="failed",
                error_code="provider_failed",
            )
        blocked_task = create_task(client, "usage-circuit-blocked")
        blocked = dispatch(client, blocked_task["id"], "usage-circuit-blocked-run")
        status_response = client.get("/v1/usage/control-status", headers=LEADER)
        circuit = next(
            item for item in status_response.json()["circuits"] if item["state"] == "open"
        )
        denied_reset = client.post(
            f"/v1/usage/circuits/{circuit['id']}/reset",
            headers=LEADER,
            json={"reason": "El líder no gobierna este control"},
        )
        reset = client.post(
            f"/v1/usage/circuits/{circuit['id']}/reset",
            headers=OPERATOR,
            json={"reason": "Proveedor recuperado y comprobado"},
        )
        allowed = dispatch(client, blocked_task["id"], "usage-circuit-after-reset")

        assert blocked.status_code == 409
        assert blocked.json()["detail"]["code"] == "circuit_open"
        assert status_response.status_code == 200
        assert denied_reset.status_code == 403
        assert reset.status_code == 200
        assert reset.json()["state"] == "closed"
        assert allowed.status_code == 202


def test_concurrency_and_fan_out_are_enforced(tmp_path: Path) -> None:
    with usage_context(tmp_path) as (client, factory, _):
        first = create_task(client, "usage-concurrency-first")
        assert dispatch(client, first["id"], "usage-concurrency-first-run").status_code == 202
        second = create_task(
            client,
            "usage-concurrency-second",
            budget={"max_concurrent_runs": 1},
        )
        concurrency = dispatch(client, second["id"], "usage-concurrency-second-run")
        assert concurrency.json()["detail"]["code"] == "concurrency_limit_exceeded"

        with factory() as session:
            active = session.scalar(select(Run).where(Run.task_id == uuid.UUID(first["id"])))
            assert active is not None
            transition_run(
                session,
                run_id=active.id,
                new_status="cancelled",
                actor_id="system:test",
            )

        parent = create_task(client, "usage-fanout-parent")
        children = [
            create_task(
                client,
                f"usage-fanout-child-{index}",
                parent_task_id=parent["id"],
                budget={"max_fan_out": 2},
            )
            for index in range(3)
        ]
        assert dispatch(client, children[0]["id"], "usage-fanout-run-0").status_code == 202
        assert dispatch(client, children[1]["id"], "usage-fanout-run-1").status_code == 202
        denied = dispatch(client, children[2]["id"], "usage-fanout-run-2")
        assert denied.json()["detail"]["code"] == "fan_out_limit_exceeded"


def test_database_override_missing_detail_and_ingest_conflict(tmp_path: Path) -> None:
    from hermes_orchestrator.usage_services import ControlViolation, ingest_run_usage

    with usage_context(tmp_path) as (client, factory, settings):
        with factory() as session:
            session.add(
                Budget(
                    scope_type="project",
                    scope_key="tradix",
                    window_seconds=3600,
                    soft_token_limit=100,
                    hard_token_limit=200,
                    max_concurrent_runs=2,
                    max_fan_out=2,
                    max_retries=0,
                    circuit_failure_threshold=2,
                    circuit_cooldown_seconds=60,
                )
            )
            session.commit()
        status_response = client.get("/v1/usage/control-status", headers=OPERATOR)
        missing = client.get(f"/v1/usage/runs/{uuid.uuid4()}", headers=LEADER)
        denied = client.get("/v1/usage/summary", headers={"X-Actor-Id": "unknown"})
        missing_reset = client.post(
            f"/v1/usage/circuits/{uuid.uuid4()}/reset",
            headers=OPERATOR,
            json={"reason": "No existe"},
        )
        task = create_task(client, "usage-replay-task")
        run_data = dispatch(client, task["id"], "usage-replay-run").json()["run"]
        finish_run(factory, settings, run_data["id"], status="completed", usage={"input_tokens": 1})
        with factory() as session:
            run = session.get(Run, uuid.UUID(run_data["id"]))
            assert run is not None
            _, replayed = ingest_run_usage(session, run, settings=settings)
            run.usage_snapshot = {"input_tokens": 2}
            try:
                ingest_run_usage(session, run, settings=settings)
            except ControlViolation as error:
                conflict_code = error.code
            else:
                raise AssertionError("El ledger debía rechazar un payload distinto")
        assert status_response.status_code == 200
        assert any(item["source"] == "database" for item in status_response.json()["budgets"])
        assert missing.status_code == 404
        assert denied.status_code == 401
        assert missing_reset.status_code == 404
        assert replayed is True
        assert conflict_code == "usage_ingest_conflict"

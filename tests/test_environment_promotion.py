from __future__ import annotations

import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import AuditEvent, Base, EnvironmentAction
from tests.auth_helpers import auth_headers, seed_active_auth_agents, token_settings

LEADER = auth_headers("agent:leader")
DEVELOPER = auth_headers("agent:developer")
RESEARCHER = auth_headers("agent:researcher")
REPOSITORY = "jbeneitezh/tradix"
SHA_A = "a" * 40
SHA_B = "b" * 40


@contextmanager
def environment_context(tmp_path: Path):
    database_path = tmp_path / f"environment-{uuid.uuid4()}.db"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        environment_allowed_repositories=[REPOSITORY],
        environment_local_port_start=23000,
        environment_local_port_end=23001,
        environment_local_default_ttl_seconds=3600,
        environment_local_max_ttl_seconds=7200,
        **token_settings("agent:leader", "agent:developer", "agent:researcher"),
    )
    app = create_app(settings)
    Base.metadata.create_all(app.state.engine)
    factory: sessionmaker[Session] = app.state.session_factory
    seed_active_auth_agents(factory, settings)
    with TestClient(app) as client:
        yield client, factory


def keyed(actor: dict[str, str], key: str) -> dict[str, str]:
    return actor | {"Idempotency-Key": key}


def create_task(client: TestClient, key: str) -> str:
    response = client.post(
        "/v1/tasks",
        headers=keyed(LEADER, key),
        json={
            "objective": "Preparar entorno local",
            "acceptance_criteria": ["Entorno verificable"],
            "assignee_actor_id": "agent:developer",
            "budget": {},
        },
    )
    assert response.status_code == 201, response.text
    return str(response.json()["id"])


def deploy(
    client: TestClient,
    key: str,
    *,
    environment: str = "dev",
    sha: str = SHA_A,
    branch: str = "feature/f17",
    task_id: str | None = None,
    ttl_seconds: int | None = None,
    actor: dict[str, str] = LEADER,
):
    body: dict[str, Any] = {
        "environment": environment,
        "repository": REPOSITORY,
        "branch": branch,
        "resolved_sha": sha,
    }
    if task_id is not None:
        body["task_id"] = task_id
    if ttl_seconds is not None:
        body["ttl_seconds"] = ttl_seconds
    return client.post("/v1/environments/deployments", headers=keyed(actor, key), json=body)


def promote(
    client: TestClient,
    key: str,
    source_id: str,
    *,
    target: str = "pre",
    sha: str = SHA_A,
    approval_actor: str | None = "agent:validator",
    tag: str | None = None,
):
    body: dict[str, Any] = {
        "source_deployment_id": source_id,
        "target_environment": target,
        "repository": REPOSITORY,
        "resolved_sha": sha,
    }
    if approval_actor is not None:
        body["approval"] = {
            "actor_id": approval_actor,
            "decision": "approved",
            "reason": "Validación independiente satisfactoria",
        }
    if tag is not None:
        body["tag"] = tag
    return client.post("/v1/environments/promotions", headers=keyed(LEADER, key), json=body)


def prepare_pre(
    client: TestClient, prefix: str, *, sha: str = SHA_A
) -> tuple[dict[str, Any], dict[str, Any]]:
    dev_response = deploy(client, f"{prefix}-dev-0001", sha=sha)
    assert dev_response.status_code == 201, dev_response.text
    pre_response = promote(client, f"{prefix}-pre-0001", dev_response.json()["id"], sha=sha)
    assert pre_response.status_code == 201, pre_response.text
    return dev_response.json(), pre_response.json()


def test_local_create_expire_reuses_governed_port_and_is_idempotent(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, factory):
        task_id = create_task(client, "f17-local-task-01")
        created = deploy(
            client,
            "f17-local-deploy-01",
            environment="local",
            task_id=task_id,
            ttl_seconds=120,
            actor=DEVELOPER,
        )
        assert created.status_code == 201, created.text
        assert created.json()["allocated_port"] == 23000
        assert created.json()["instance_key"] == task_id
        replay = deploy(
            client,
            "f17-local-deploy-01",
            environment="local",
            task_id=task_id,
            ttl_seconds=120,
            actor=DEVELOPER,
        )
        assert replay.status_code == 201
        assert replay.json()["replayed"] is True
        conflict = deploy(
            client,
            "f17-local-deploy-02",
            environment="local",
            task_id=task_id,
            actor=DEVELOPER,
        )
        assert conflict.status_code == 409

        expired = client.post(
            f"/v1/environments/deployments/{created.json()['id']}/expire",
            headers=keyed(DEVELOPER, "f17-local-expire-01"),
        )
        assert expired.status_code == 200
        assert expired.json()["status"] == "expired"
        replacement = deploy(
            client,
            "f17-local-deploy-03",
            environment="local",
            task_id=task_id,
            actor=DEVELOPER,
        )
        assert replacement.status_code == 201
        assert replacement.json()["allocated_port"] == 23000
        with factory() as session:
            assert session.scalar(select(func.count(AuditEvent.id))) >= 4


def test_dev_deploy_tracks_mutable_branch_and_supersedes_previous(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, _):
        first = deploy(client, "f17-dev-deploy-01")
        second = deploy(
            client,
            "f17-dev-deploy-02",
            sha=SHA_B,
            branch="fix/f17",
        )
        assert first.status_code == second.status_code == 201
        inventory = client.get("/v1/environments", headers=LEADER)
        assert inventory.status_code == 200
        dev = [item for item in inventory.json()["deployments"] if item["environment"] == "dev"]
        assert [item["status"] for item in dev] == ["active", "superseded"]
        assert dev[0]["ref_kind"] == "branch"
        assert dev[0]["ref_value"] == "fix/f17"
        assert client.get("/v1/environments", headers=RESEARCHER).status_code == 200
        assert client.get("/v1/environments").status_code == 401


def test_pre_is_immutable_and_fix_returns_to_dev_branch(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, _):
        _, pre = prepare_pre(client, "f17-immutable")
        assert pre["ref_kind"] == "sha"
        assert pre["ref_value"] == SHA_A
        assert pre["resolved_sha"] == SHA_A
        direct_pre = deploy(client, "f17-direct-pre", environment="pre")
        assert direct_pre.status_code == 422

        fixed_dev = deploy(
            client,
            "f17-fix-dev-01",
            sha=SHA_B,
            branch="fix/after-pre-validation",
        )
        assert fixed_dev.status_code == 201
        inventory = client.get("/v1/environments", headers=LEADER).json()["deployments"]
        current_pre = next(
            item
            for item in inventory
            if item["environment"] == "pre" and item["status"] == "active"
        )
        assert current_pre["resolved_sha"] == SHA_A
        assert fixed_dev.json()["resolved_sha"] == SHA_B


def test_approved_promotion_reaches_prod_sim_by_tag(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, _):
        _, pre = prepare_pre(client, "f17-prod-sim")
        response = promote(
            client,
            "f17-prod-sim-promote-01",
            pre["id"],
            target="prod-sim",
            approval_actor="agent:operator",
            tag="prod-sim/f17-a",
        )
        assert response.status_code == 201, response.text
        assert response.json()["ref_kind"] == "tag"
        assert response.json()["ref_value"] == "prod-sim/f17-a"
        assert response.json()["approval_actor_id"] == "agent:operator"


def test_promotion_is_denied_without_independent_valid_approval(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, factory):
        dev = deploy(client, "f17-denied-dev-01").json()
        missing = promote(
            client,
            "f17-denied-pre-01",
            dev["id"],
            approval_actor=None,
        )
        assert missing.status_code == 403
        self_approval = promote(
            client,
            "f17-denied-pre-02",
            dev["id"],
            approval_actor="agent:leader",
        )
        assert self_approval.status_code == 403
        mismatch = promote(
            client,
            "f17-denied-pre-03",
            dev["id"],
            sha=SHA_B,
        )
        assert mismatch.status_code == 409
        with factory() as session:
            rejected = session.scalar(
                select(func.count(EnvironmentAction.id)).where(
                    EnvironmentAction.status == "rejected"
                )
            )
            assert rejected == 3


def test_rollback_restores_previous_immutable_pre_candidate(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, _):
        _, first_pre = prepare_pre(client, "f17-rollback-a", sha=SHA_A)
        second_dev = deploy(client, "f17-rollback-b-dev", sha=SHA_B).json()
        second_pre_response = promote(
            client,
            "f17-rollback-b-pre",
            second_dev["id"],
            sha=SHA_B,
        )
        assert second_pre_response.status_code == 201
        rollback = client.post(
            "/v1/environments/pre/rollback",
            headers=keyed(LEADER, "f17-rollback-pre-01"),
            json={
                "target_deployment_id": first_pre["id"],
                "approval": {
                    "actor_id": "agent:validator",
                    "decision": "approved",
                    "reason": "Regresión confirmada",
                },
            },
        )
        assert rollback.status_code == 201, rollback.text
        assert rollback.json()["resolved_sha"] == SHA_A
        assert rollback.json()["rollback_of_deployment_id"] == second_pre_response.json()["id"]


def test_live_is_always_denied_and_audited(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, factory):
        _, pre = prepare_pre(client, "f17-live")
        response = promote(
            client,
            "f17-live-denied-01",
            pre["id"],
            target="live",
            approval_actor="agent:operator",
            tag="live/f17",
        )
        assert response.status_code == 403
        assert response.json()["detail"]["code"] == "live_environment_denied"
        with factory() as session:
            action = session.scalar(
                select(EnvironmentAction).where(
                    EnvironmentAction.idempotency_key == "f17-live-denied-01"
                )
            )
            assert action is not None
            assert action.status == "rejected"


def test_policy_boundaries_and_invalid_environment_inputs(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, _):
        denied_dev = deploy(client, "f17-dev-denied-01", actor=DEVELOPER)
        assert denied_dev.status_code == 403
        repository_denied = client.post(
            "/v1/environments/deployments",
            headers=keyed(LEADER, "f17-repo-denied-01"),
            json={
                "environment": "dev",
                "repository": "someone/else",
                "branch": "main",
                "resolved_sha": SHA_A,
            },
        )
        assert repository_denied.status_code == 403
        extra_remote_mutation = client.post(
            "/v1/environments/deployments",
            headers=keyed(LEADER, "f17-ruleset-denied-01"),
            json={
                "environment": "dev",
                "repository": REPOSITORY,
                "branch": "main",
                "resolved_sha": SHA_A,
                "ruleset": {"enforcement": "active"},
            },
        )
        assert extra_remote_mutation.status_code == 422
        assert (
            deploy(
                client,
                "f17-dev-local-fields",
                task_id=str(uuid.uuid4()),
            ).status_code
            == 422
        )


def test_rejected_edges_replays_and_capacity_are_governed(tmp_path: Path) -> None:
    with environment_context(tmp_path) as (client, factory):
        assert (
            client.get("/v1/environments", headers={"X-Actor-Id": "agent:unknown"}).status_code
            == 401
        )
        unknown_expire = client.post(
            f"/v1/environments/deployments/{uuid.uuid4()}/expire",
            headers=keyed(LEADER, "f17-expire-unknown"),
        )
        assert unknown_expire.status_code == 404

        first_dev = deploy(client, "f17-edge-dev-01").json()
        conflict = deploy(
            client,
            "f17-edge-dev-01",
            sha=SHA_B,
            branch="other",
        )
        assert conflict.status_code == 409
        bad_source = promote(
            client,
            "f17-edge-source-01",
            str(uuid.uuid4()),
        )
        assert bad_source.status_code == 409
        pre_with_tag = promote(
            client,
            "f17-edge-pre-tag-01",
            first_dev["id"],
            tag="not-for-pre",
        )
        assert pre_with_tag.status_code == 422
        replay_rejected = promote(
            client,
            "f17-edge-pre-tag-01",
            first_dev["id"],
            tag="not-for-pre",
        )
        assert replay_rejected.status_code == 409

        pre = promote(client, "f17-edge-pre-ok", first_dev["id"]).json()
        invalid_tag = promote(
            client,
            "f17-edge-prod-tag",
            pre["id"],
            target="prod-sim",
            approval_actor="agent:operator",
            tag="invalid tag",
        )
        assert invalid_tag.status_code == 422
        wrong_approver = promote(
            client,
            "f17-edge-prod-approval",
            pre["id"],
            target="prod-sim",
            approval_actor="agent:validator",
            tag="prod-sim/edge",
        )
        assert wrong_approver.status_code == 403

        denied_environment = client.post(
            "/v1/environments/dev/rollback",
            headers=keyed(LEADER, "f17-edge-rollback-dev"),
            json={
                "target_deployment_id": pre["id"],
                "approval": {
                    "actor_id": "agent:validator",
                    "reason": "No aplica",
                },
            },
        )
        assert denied_environment.status_code == 403
        missing_target = client.post(
            "/v1/environments/pre/rollback",
            headers=keyed(LEADER, "f17-edge-rollback-missing"),
            json={
                "target_deployment_id": str(uuid.uuid4()),
                "approval": {
                    "actor_id": "agent:validator",
                    "reason": "Candidato anterior",
                },
            },
        )
        assert missing_target.status_code == 404
        self_rollback = client.post(
            "/v1/environments/pre/rollback",
            headers=keyed(LEADER, "f17-edge-rollback-self"),
            json={
                "target_deployment_id": pre["id"],
                "approval": {
                    "actor_id": "agent:leader",
                    "reason": "No independiente",
                },
            },
        )
        assert self_rollback.status_code == 403

        task_one = create_task(client, "f17-capacity-task-1")
        task_two = create_task(client, "f17-capacity-task-2")
        task_three = create_task(client, "f17-capacity-task-3")
        assert (
            deploy(
                client,
                "f17-capacity-local-1",
                environment="local",
                task_id=task_one,
            ).status_code
            == 201
        )
        assert (
            deploy(
                client,
                "f17-capacity-local-2",
                environment="local",
                task_id=task_two,
            ).status_code
            == 201
        )
        exhausted = deploy(
            client,
            "f17-capacity-local-3",
            environment="local",
            task_id=task_three,
        )
        assert exhausted.status_code == 409
        missing_task = deploy(
            client,
            "f17-local-task-missing",
            environment="local",
            task_id=str(uuid.uuid4()),
        )
        assert missing_task.status_code == 422
        with factory() as session:
            assert session.scalar(select(func.count(EnvironmentAction.id))) >= 12
        task_id = create_task(client, "f17-ttl-task-01")
        assert (
            deploy(
                client,
                "f17-ttl-denied-01",
                environment="local",
                task_id=task_id,
                ttl_seconds=7201,
            ).status_code
            == 422
        )

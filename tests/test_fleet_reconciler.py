from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.fleet_runner import HttpFleetRunnerClient
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import AuditEvent, Base, FleetReconcileRecord

OPERATOR = {"X-Actor-Id": "agent:operator"}
VALIDATOR_ID = "agent:validator"
COMPOSE_PATH = "/fleet/compose/compose.yaml"
DATA_ROOT = "/fleet/data"


class FakeFleetRunner:
    def __init__(self) -> None:
        self.status_calls = 0
        self.plan_calls = 0
        self.apply_calls = 0
        self.fail_apply = False

    def _result(self, action: str) -> dict[str, Any]:
        return {
            "action": action,
            "compose_digest": "sha256:compose",
            "services": [{"name": "worker-operator", "state": "running", "health": "healthy"}],
            "configured_services": [
                "fleet-reconciler",
                "orchestrator-api",
                "worker-operator",
            ],
            "commands": ["config"] if action != "apply" else ["config", "pull", "up"],
        }

    def status(self) -> dict[str, Any]:
        self.status_calls += 1
        return self._result("status")

    def plan(self) -> dict[str, Any]:
        self.plan_calls += 1
        return self._result("plan")

    def apply(self, services: list[str]) -> dict[str, Any]:
        self.apply_calls += 1
        assert services == ["worker-operator"]
        if self.fail_apply:
            raise RuntimeError("runner failed")
        return self._result("apply")


@pytest.fixture
def fleet_context(
    tmp_path: Path,
) -> Iterator[tuple[TestClient, sessionmaker[Session], FakeFleetRunner]]:
    database_path = tmp_path / "fleet.db"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        actor_roles={
            "user:owner": "owner",
            "agent:leader": "leader",
            "agent:operator": "operator",
            VALIDATOR_ID: "validator",
        },
        fleet_project_name="hermes-test",
        fleet_compose_path=COMPOSE_PATH,
        fleet_allowed_worker_image="hermes-worker:test",
        fleet_allowed_mount_roots=[DATA_ROOT, "/fleet/tradix", "/fleet/compose"],
    )
    runner = FakeFleetRunner()
    app = create_app(settings, fleet_runner=runner)
    Base.metadata.create_all(app.state.engine)
    with TestClient(app) as client:
        yield client, app.state.session_factory, runner


def fleet_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "project_name": "hermes-test",
        "compose_path": COMPOSE_PATH,
        "mode": "dry_run",
        "targets": [
            {
                "name": "worker-operator",
                "image": "hermes-worker:test",
                "mounts": [
                    {
                        "source": f"{DATA_ROOT}/operator",
                        "target": "/workspace",
                        "read_only": False,
                    }
                ],
            }
        ],
    }
    return payload | overrides


def post_reconcile(client: TestClient, key: str, payload: dict[str, Any]):
    return client.post(
        "/v1/fleet/reconcile-requests",
        headers=OPERATOR | {"Idempotency-Key": key},
        json=payload,
    )


def test_status_reports_observed_fleet_and_denies_unknown_actor(fleet_context) -> None:
    client, _, runner = fleet_context

    response = client.get("/v1/fleet/status", headers=OPERATOR)
    denied = client.get("/v1/fleet/status", headers={"X-Actor-Id": "unknown"})

    assert response.status_code == 200
    assert response.json()["compose_digest"] == "sha256:compose"
    assert response.json()["last_reconcile"] is None
    assert denied.status_code == 403
    assert runner.status_calls == 1


def test_dry_run_is_idempotent_and_returns_diff_without_apply(fleet_context) -> None:
    client, _, runner = fleet_context
    payload = fleet_payload()

    first = post_reconcile(client, "fleet-dry-run-01", payload)
    replay = post_reconcile(client, "fleet-dry-run-01", payload)
    collision = post_reconcile(
        client,
        "fleet-dry-run-01",
        fleet_payload(targets=payload["targets"] + [payload["targets"][0] | {"name": "worker-x"}]),
    )

    assert first.status_code == 202
    assert first.json()["status"] == "dry_run"
    assert first.json()["diff"] == {
        "added": ["worker-operator"],
        "changed": [],
        "removed": [],
        "has_changes": True,
    }
    assert replay.json()["replayed"] is True
    assert collision.status_code == 409
    assert runner.plan_calls == 1
    assert runner.apply_calls == 0


def test_apply_requires_independent_approval_and_second_apply_is_noop(fleet_context) -> None:
    client, _, runner = fleet_context
    approved = fleet_payload(
        mode="apply",
        approval={
            "actor_id": VALIDATOR_ID,
            "decision": "approved",
            "reason": "Diff revisado",
        },
    )

    first = post_reconcile(client, "fleet-apply-01", approved)
    second = post_reconcile(
        client,
        "fleet-apply-02",
        fleet_payload(mode="apply"),
    )
    status = client.get("/v1/fleet/status", headers=OPERATOR)

    assert first.status_code == 202
    assert first.json()["status"] == "applied"
    assert first.json()["approval_actor_id"] == VALIDATOR_ID
    assert second.status_code == 202
    assert second.json()["status"] == "no_change"
    assert second.json()["diff"]["has_changes"] is False
    assert second.json()["rollback_available"] is True
    assert status.json()["last_reconcile"]["status"] == "no_change"
    assert runner.apply_calls == 1
    assert runner.plan_calls == 1


@pytest.mark.parametrize(
    ("override", "code"),
    [
        ({"image": "evil/image:latest"}, "image_not_allowed"),
        ({"mounts": [{"source": "/outside", "target": "/workspace"}]}, "mount_root_denied"),
        ({"mounts": [{"source": "relative", "target": "/workspace"}]}, "mount_root_denied"),
        (
            {"mounts": [{"source": f"{DATA_ROOT}/operator", "target": "relative"}]},
            "mount_target_denied",
        ),
        ({"privileged": True}, "privileged_denied"),
        ({"network_mode": "host"}, "network_mode_denied"),
        ({"pid_mode": "host"}, "pid_mode_denied"),
        (
            {"mounts": [{"source": f"{DATA_ROOT}/docker.sock", "target": "/var/run/docker.sock"}]},
            "docker_socket_denied",
        ),
        (
            {"mounts": [{"source": "/fleet/tradix", "target": "/repo", "read_only": False}]},
            "product_mount_must_be_read_only",
        ),
    ],
)
def test_dangerous_target_configuration_is_rejected_and_audited(
    fleet_context, override: dict[str, Any], code: str
) -> None:
    client, factory, runner = fleet_context
    target = fleet_payload()["targets"][0] | override

    response = post_reconcile(
        client,
        f"reject-{code}",
        fleet_payload(targets=[target]),
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == code
    assert runner.plan_calls == runner.apply_calls == 0
    with factory() as session:
        record = session.scalar(select(FleetReconcileRecord))
        events = list(session.scalars(select(AuditEvent).order_by(AuditEvent.created_at)))
    assert record is not None and record.status == "rejected"
    assert [event.event_type for event in events] == [
        "fleet.reconcile.requested",
        "fleet.reconcile.rejected",
    ]


def test_dry_run_rejects_worker_not_present_in_rendered_compose(fleet_context) -> None:
    client, _, runner = fleet_context
    target = fleet_payload()["targets"][0] | {"name": "worker-not-configured"}

    response = post_reconcile(
        client,
        "worker-not-configured",
        fleet_payload(targets=[target]),
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "service_not_configured"
    assert runner.plan_calls == 1


@pytest.mark.parametrize(
    ("approval", "expected"),
    [
        (None, "approval_required"),
        (
            {"actor_id": "agent:operator", "decision": "approved", "reason": "Propia"},
            "approval_required",
        ),
        (
            {"actor_id": "unknown", "decision": "approved", "reason": "Sin rol"},
            "approval_required",
        ),
    ],
)
def test_apply_with_changes_fails_closed_without_valid_approval(
    fleet_context, approval: dict[str, Any] | None, expected: str
) -> None:
    client, factory, runner = fleet_context

    response = post_reconcile(
        client,
        f"approval-{approval!s}",
        fleet_payload(mode="apply", approval=approval),
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == expected
    assert runner.apply_calls == 0
    with factory() as session:
        event_types = list(session.scalars(select(AuditEvent.event_type)))
    assert sorted(event_types) == sorted(
        [
            "fleet.reconcile.requested",
            "fleet.reconcile.approval_required",
        ]
    )


def test_apply_audit_trail_records_request_and_independent_approval(fleet_context) -> None:
    client, factory, _ = fleet_context
    payload = fleet_payload(
        mode="apply",
        approval={
            "actor_id": VALIDATOR_ID,
            "decision": "approved",
            "reason": "Policy suite correcta",
        },
    )

    response = post_reconcile(client, "fleet-audit-01", payload)

    assert response.status_code == 202
    with factory() as session:
        record = session.scalar(select(FleetReconcileRecord))
        events = list(session.scalars(select(AuditEvent).order_by(AuditEvent.created_at)))
    assert record is not None
    assert record.approval_actor_id == VALIDATOR_ID
    assert record.approval_reason == "Policy suite correcta"
    assert [event.event_type for event in events] == [
        "fleet.reconcile.requested",
        "fleet.reconcile.applied",
    ]


def test_runner_failure_is_durable_and_fails_closed(fleet_context) -> None:
    client, factory, runner = fleet_context
    runner.fail_apply = True

    response = post_reconcile(
        client,
        "fleet-runner-failure",
        fleet_payload(
            mode="apply",
            approval={
                "actor_id": VALIDATOR_ID,
                "decision": "approved",
                "reason": "Diff revisado",
            },
        ),
    )

    assert response.status_code == 503
    with factory() as session:
        record = session.scalar(select(FleetReconcileRecord))
    assert record is not None and record.status == "failed"
    assert record.runner_result == {"error_code": "fleet_runner_failed"}


def test_http_runner_client_uses_only_fixed_internal_actions(monkeypatch) -> None:
    calls: list[tuple[str, str, dict[str, Any] | None]] = []

    class StubClient:
        def __init__(self, timeout: int) -> None:
            assert timeout == 120

        def __enter__(self):
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def request(self, method: str, url: str, *, headers, json):
            assert headers == {"X-Reconciler-Token": "internal-token"}
            calls.append((method, url, json))
            return httpx.Response(
                200,
                json={"compose_digest": "x", "services": []},
                request=httpx.Request(method, url),
            )

    monkeypatch.setattr("hermes_orchestrator.fleet_runner.httpx.Client", StubClient)
    settings = Settings(fleet_runner_url="http://runner:8090", fleet_runner_token="internal-token")
    client = HttpFleetRunnerClient(settings)

    assert client.status()["compose_digest"] == "x"
    client.plan()
    client.apply(["worker-operator"])

    assert calls == [
        ("GET", "http://runner:8090/v1/internal/status", None),
        ("POST", "http://runner:8090/v1/internal/reconcile", {"action": "plan"}),
        (
            "POST",
            "http://runner:8090/v1/internal/reconcile",
            {"action": "apply", "services": ["worker-operator"]},
        ),
    ]
    with pytest.raises(RuntimeError, match="token"):
        HttpFleetRunnerClient(Settings(fleet_runner_token="")).status()

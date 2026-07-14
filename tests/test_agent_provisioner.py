from __future__ import annotations

import copy
import hashlib
import importlib
import json
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hermes_orchestrator.config import Settings
from hermes_orchestrator.main import create_app
from hermes_orchestrator.models import Agent, AgentRequestRecord, AuditEvent, Base
from hermes_orchestrator.provisioning import (
    HttpAgentProvisionerClient,
    ManagedAgentRenderer,
    ProvisionerResult,
    ProvisioningError,
    ProvisioningPayload,
)
from tests.auth_helpers import auth_headers, seed_active_auth_agents, token_settings

PAYLOAD = {
    "slug": "data-steward-f15",
    "role": "data_steward",
    "description": "Custodia temporal de datasets",
    "policy_set": {"name": "data-read-only"},
    "capabilities": ["dataset_read"],
    "secret_refs": ["secret://codex/broker-client"],
}
DYNAMIC_TOKEN = "dynamic-data-steward-token"
DYNAMIC_TOKEN_SHA256 = hashlib.sha256(DYNAMIC_TOKEN.encode()).hexdigest()


class FakeFleet:
    def __init__(self) -> None:
        self.apply_calls: list[list[str]] = []
        self.rollback_calls: list[list[str]] = []

    def apply(self, services: list[str]) -> dict[str, Any]:
        self.apply_calls.append(services)
        return {"services": [{"service": services[0], "state": "running", "health": "healthy"}]}

    def rollback(self, services: list[str]) -> dict[str, Any]:
        self.rollback_calls.append(services)
        return {"services": [], "action": "rollback"}


class FailingFleet(FakeFleet):
    def apply(self, services: list[str]) -> dict[str, Any]:
        raise RuntimeError("fleet unavailable")


class StubProvisioner:
    def __init__(self) -> None:
        self.applied: list[ProvisioningPayload] = []
        self.rolled_back: list[ProvisioningPayload] = []
        self.failure: ProvisioningError | None = None

    def apply(self, payload: ProvisioningPayload) -> ProvisionerResult:
        if self.failure is not None:
            raise self.failure
        self.applied.append(payload)
        return ProvisionerResult(
            status="applied",
            service_name=f"worker-{payload.slug}",
            config_digest="sha256:f15-applied",
            health="healthy",
            credential_sha256=DYNAMIC_TOKEN_SHA256,
        )

    def rollback(self, payload: ProvisioningPayload) -> ProvisionerResult:
        self.rolled_back.append(payload)
        return ProvisionerResult(
            status="rolled_back",
            service_name=f"worker-{payload.slug}",
            config_digest="sha256:f15-rolled-back",
            health="stopped",
        )


def provisioning_payload(**updates: Any) -> ProvisioningPayload:
    return ProvisioningPayload.model_validate(
        {"request_id": str(uuid.uuid4()), **PAYLOAD, **updates}
    )


@pytest.fixture
def renderer_context(tmp_path: Path) -> tuple[ManagedAgentRenderer, FakeFleet, Path, Path]:
    managed = tmp_path / "managed"
    data = tmp_path / "agent-data"
    renderer = ManagedAgentRenderer(
        managed_root=managed,
        data_root=data,
        host_data_root="/host_mnt/agent-data",
        dataset_root="/host_mnt/tradix/dataset",
        worker_image="hermes-worker:test",
        project_name="hermes-test",
    )
    return renderer, FakeFleet(), managed, data


@pytest.fixture
def api_context(
    tmp_path: Path,
) -> Iterator[tuple[TestClient, sessionmaker[Session], StubProvisioner]]:
    database_path = tmp_path / "provisioning.db"
    settings = Settings(
        environment="test",
        database_url=f"sqlite+pysqlite:///{database_path.as_posix()}",
        actor_roles={
            "user:owner": "owner",
            "agent:leader": "leader",
            "agent:operator": "operator",
        },
        **token_settings("user:owner", "agent:leader", "agent:operator"),
    )
    provisioner = StubProvisioner()
    app = create_app(settings, agent_provisioner=provisioner)
    Base.metadata.create_all(app.state.engine)
    seed_active_auth_agents(app.state.session_factory, settings)
    with TestClient(app) as client:
        yield client, app.state.session_factory, provisioner


def approved_request(client: TestClient, suffix: str) -> str:
    created = client.post(
        "/v1/agents/requests",
        headers=auth_headers("agent:leader", **{"Idempotency-Key": f"f15-create-{suffix}"}),
        json=PAYLOAD | {"slug": f"data-steward-{suffix}"},
    )
    request_id = created.json()["id"]
    decided = client.post(
        f"/v1/agents/requests/{request_id}/decide",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": f"f15-decide-{suffix}"}),
        json={"decision": "approve", "reason": "Plantilla mínima revisada"},
    )
    assert decided.status_code == 200
    return request_id


def test_render_valido_es_cerrado_y_secreto_fuera_de_git(
    renderer_context, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    renderer, fleet, managed, data = renderer_context
    payload = provisioning_payload()

    result = renderer.apply(payload, fleet)

    document = renderer._read_document()
    service = document["services"]["worker-data-steward-f15"]
    assert result.status == "applied"
    assert result.health == "healthy"
    assert set(service).isdisjoint({"command", "entrypoint", "privileged", "network_mode", "pid"})
    dataset_mount = next(
        mount for mount in service["volumes"] if mount["target"] == "/datasets/tradix"
    )
    assert dataset_mount == {
        "type": "bind",
        "source": "/host_mnt/tradix/dataset",
        "target": "/datasets/tradix",
        "read_only": True,
    }
    assert not (managed / "agents" / payload.slug / "runtime.env").exists()
    assert (data / payload.slug / "runtime" / "runtime.env").exists()
    assert "secret://" in (managed / "agents" / payload.slug / "runtime.env.ref").read_text()
    runtime_manifest = json.loads(
        (data / payload.slug / "managed" / "manifest.yaml").read_text(encoding="utf-8")
    )
    assert runtime_manifest["id"] == payload.slug
    assert runtime_manifest["role"] == "data_steward"
    assert runtime_manifest["execution_profile_default"] == "sol-high"
    assert runtime_manifest["allowed_profiles"] == ["sol-high"]

    server_managed = tmp_path / "server-managed"
    env = {
        "AGENT_PROVISIONER_TOKEN": "f15-server-token",
        "AGENT_PROVISIONER_MANAGED_ROOT": str(server_managed),
        "AGENT_PROVISIONER_DATA_ROOT": str(tmp_path / "server-data"),
        "AGENT_PROVISIONER_HOST_DATA_ROOT": "/host_mnt/server-data",
        "AGENT_PROVISIONER_DATASET_ROOT": "/host_mnt/tradix/dataset",
        "AGENT_PROVISIONER_WORKER_IMAGE": "hermes-worker:test",
        "AGENT_PROVISIONER_PROJECT_NAME": "hermes-test",
        "AGENT_PROVISIONER_FLEET_URL": "http://fleet:8090",
        "AGENT_PROVISIONER_FLEET_TOKEN": "fleet-token",
        "CODEX_BROKER_CLIENT_KEY": "broker-test-secret",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    sys.modules.pop("hermes_orchestrator.provisioner_server", None)
    server = importlib.import_module("hermes_orchestrator.provisioner_server")
    server.fleet = FakeFleet()
    with TestClient(server.app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert (
            client.post(
                "/v1/internal/agents/apply", json=payload.model_dump(mode="json")
            ).status_code
            == 422
        )
        assert (
            client.post(
                "/v1/internal/agents/apply",
                headers={"X-Provisioner-Token": "incorrecto"},
                json=payload.model_dump(mode="json"),
            ).status_code
            == 403
        )
        applied = client.post(
            "/v1/internal/agents/apply",
            headers={"X-Provisioner-Token": "f15-server-token"},
            json=payload.model_dump(mode="json"),
        )
        rolled_back = client.post(
            "/v1/internal/agents/rollback",
            headers={"X-Provisioner-Token": "f15-server-token"},
            json=payload.model_dump(mode="json"),
        )
        server.fleet = FailingFleet()
        failed_payload = payload.model_copy(update={"slug": "data-steward-server-failure"})
        failed = client.post(
            "/v1/internal/agents/apply",
            headers={"X-Provisioner-Token": "f15-server-token"},
            json=failed_payload.model_dump(mode="json"),
        )
        monkeypatch.setattr(
            server.renderer,
            "rollback",
            lambda *_: (_ for _ in ()).throw(
                ProvisioningError("rollback_failed", "Rollback denegado", 503)
            ),
        )
        rollback_failed = client.post(
            "/v1/internal/agents/rollback",
            headers={"X-Provisioner-Token": "f15-server-token"},
            json=payload.model_dump(mode="json"),
        )
    assert applied.status_code == 200
    assert rolled_back.json()["status"] == "rolled_back"
    assert failed.status_code == 503
    assert rollback_failed.status_code == 503


def test_data_steward_recibe_identidad_clon_y_credencial_git_persistentes(
    tmp_path: Path,
) -> None:
    managed = tmp_path / "managed"
    data = tmp_path / "agent-data"
    renderer = ManagedAgentRenderer(
        managed_root=managed,
        data_root=data,
        host_data_root="/host_mnt/agent-data",
        dataset_root="/host_mnt/tradix/dataset",
        worker_image="hermes-worker:test",
        project_name="hermes-test",
        knowledge_repository_url="https://github.com/example/knowledge.git",
        github_login="shared-login",
        github_token="github-test-token",
    )
    payload = provisioning_payload(
        slug="data-steward-f6",
        policy_set={"name": "data-steward-v3", "mount_profile": "knowledge-branch-workspace"},
        secret_refs=["secret://codex/broker-client", "secret://github/shared-agent"],
    )

    renderer.apply(payload, FakeFleet())

    service = renderer._read_document()["services"]["worker-data-steward-f6"]
    assert service["environment"]["HERMES_AGENT_ID"] == "data-steward-f6"
    assert service["environment"]["HERMES_AGENT_ROLE"] == "data_steward"
    assert service["environment"]["HERMES_KNOWLEDGE_REPOSITORY_URL"].endswith("/knowledge.git")
    hosts = data / payload.slug / "home" / ".config" / "gh" / "hosts.yml"
    assert hosts.exists()
    assert "github-test-token" in hosts.read_text(encoding="utf-8")
    assert "secret://github/shared-agent" in (
        managed / "agents" / payload.slug / "runtime.env.ref"
    ).read_text(encoding="utf-8")
    assert "github-test-token" not in renderer.compose_path.read_text(encoding="utf-8")


def test_credencial_git_solicitada_sin_material_falla_cerrado(tmp_path: Path) -> None:
    renderer = ManagedAgentRenderer(
        managed_root=tmp_path / "managed",
        data_root=tmp_path / "data",
        host_data_root="/host_mnt/agent-data",
        dataset_root="/host_mnt/tradix/dataset",
        worker_image="hermes-worker:test",
        project_name="hermes-test",
    )
    payload = provisioning_payload(
        slug="data-steward-f6-missing",
        secret_refs=["secret://codex/broker-client", "secret://github/shared-agent"],
    )

    with pytest.raises(ProvisioningError) as missing:
        renderer.apply(payload, FakeFleet())

    assert missing.value.code == "github_credential_missing"


def test_no_op_no_reaplica_fleet(renderer_context, monkeypatch: pytest.MonkeyPatch) -> None:
    renderer, fleet, _, _ = renderer_context
    payload = provisioning_payload()

    first = renderer.apply(payload, fleet)
    second = renderer.apply(payload, fleet)

    assert first.status == "applied"
    assert second.status == "no_change"
    assert fleet.apply_calls == [["worker-data-steward-f15"]]

    class StubResponse:
        status_code = 200

        def json(self):
            return {
                "status": "applied",
                "service_name": "worker-data-steward-f15",
                "config_digest": "sha256:http-client",
                "health": "healthy",
                "credential_sha256": DYNAMIC_TOKEN_SHA256,
                "runner_result": {},
            }

    class StubClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def post(self, *args, **kwargs):
            return StubResponse()

    monkeypatch.setattr("hermes_orchestrator.provisioning.httpx.Client", StubClient)
    client = HttpAgentProvisionerClient(
        Settings(agent_provisioner_url="http://provisioner:8091", agent_provisioner_token="token")
    )
    assert client.apply(payload).status == "applied"
    assert client.rollback(payload).config_digest == "sha256:http-client"
    StubResponse.status_code = 422
    StubResponse.json = lambda self: {
        "detail": {"code": "template_denied", "detail": "Plantilla denegada"}
    }
    with pytest.raises(ProvisioningError) as rejected:
        client.apply(payload)
    assert rejected.value.code == "template_denied"
    StubResponse.json = lambda self: []
    with pytest.raises(ProvisioningError) as malformed_rejection:
        client.apply(payload)
    assert malformed_rejection.value.code == "provisioner_rejected"

    class FailingClient(StubClient):
        def post(self, *args, **kwargs):
            raise httpx.ConnectError("offline")

    monkeypatch.setattr("hermes_orchestrator.provisioning.httpx.Client", FailingClient)
    with pytest.raises(ProvisioningError) as unavailable:
        client.apply(payload)
    assert unavailable.value.code == "provisioner_unavailable"
    with pytest.raises(ProvisioningError, match="Falta el token"):
        HttpAgentProvisionerClient(Settings(agent_provisioner_token="")).apply(payload)


def test_slug_invalido_se_rechaza_antes_de_escribir(renderer_context) -> None:
    renderer, _, managed, _ = renderer_context
    with pytest.raises(ValidationError):
        provisioning_payload(slug="../escape")
    with pytest.raises(ValidationError, match="Campo de plantilla denegado"):
        provisioning_payload(policy_set={"nested": [{"command": "rm -rf /"}]})
    with pytest.raises(ValidationError, match="execution_profile_default debe ser sol-high"):
        provisioning_payload(
            policy_set={
                "execution_profile_default": "spark-low",
                "allowed_profiles": ["spark-low"],
            }
        )
    with pytest.raises(ValidationError, match="allowed_profiles debe ser exactamente"):
        provisioning_payload(policy_set={"allowed_profiles": ["sol-high", "terra-medium"]})
    renderer.ensure_document()
    renderer.compose_path.write_text("{invalid", encoding="utf-8")
    with pytest.raises(ProvisioningError) as invalid_json:
        renderer._read_document()
    assert invalid_json.value.code == "managed_compose_invalid"
    renderer.compose_path.write_text("[]", encoding="utf-8")
    with pytest.raises(ProvisioningError) as invalid_type:
        renderer._read_document()
    assert invalid_type.value.code == "managed_compose_invalid"
    assert managed.exists()


def test_rol_invalido_no_tiene_plantilla(renderer_context) -> None:
    _, _, managed, _ = renderer_context
    with pytest.raises(ValidationError):
        provisioning_payload(role="docker_admin")
    assert not managed.exists()


def test_mount_externo_es_denegado(renderer_context) -> None:
    renderer, _, _, _ = renderer_context
    payload = provisioning_payload()
    document = renderer._empty_document()
    document["services"]["worker-data-steward-f15"] = renderer._service(payload)
    escaped = copy.deepcopy(document)
    escaped["services"]["worker-data-steward-f15"]["volumes"][0]["source"] = "/etc"

    with pytest.raises(ProvisioningError, match="Mount externo") as denied:
        renderer.validate_document(escaped)
    assert denied.value.code == "mount_root_denied"
    socket = copy.deepcopy(document)
    socket["services"]["worker-data-steward-f15"]["volumes"][0]["source"] = (
        "/host_mnt/agent-data/docker.sock"
    )
    with pytest.raises(ProvisioningError) as socket_denied:
        renderer.validate_document(socket)
    assert socket_denied.value.code == "docker_socket_denied"
    invalid_image = copy.deepcopy(document)
    invalid_image["services"]["worker-data-steward-f15"]["image"] = "evil:latest"
    with pytest.raises(ProvisioningError) as image_denied:
        renderer.validate_document(invalid_image)
    assert image_denied.value.code == "image_not_allowed"
    invalid_root = copy.deepcopy(document) | {"x-escape": {}}
    with pytest.raises(ProvisioningError) as root_denied:
        renderer.validate_document(invalid_root)
    assert root_denied.value.code == "managed_document_denied"
    invalid_services = {"name": "hermes-test", "services": []}
    with pytest.raises(ProvisioningError) as services_denied:
        renderer.validate_document(invalid_services)
    assert services_denied.value.code == "services_invalid"
    invalid_project = copy.deepcopy(document)
    invalid_project["name"] = "otro-proyecto"
    with pytest.raises(ProvisioningError) as project_denied:
        renderer.validate_document(invalid_project)
    assert project_denied.value.code == "project_not_allowed"
    invalid_name = renderer._empty_document()
    invalid_name["services"]["escape"] = renderer._service(payload)
    with pytest.raises(ProvisioningError) as name_denied:
        renderer.validate_document(invalid_name)
    assert name_denied.value.code == "slug_invalid"
    invalid_service = renderer._empty_document()
    invalid_service["services"]["worker-data-steward-f15"] = []
    with pytest.raises(ProvisioningError) as service_denied:
        renderer.validate_document(invalid_service)
    assert service_denied.value.code == "service_invalid"
    arbitrary_target = copy.deepcopy(document)
    arbitrary_target["services"]["worker-data-steward-f15"]["volumes"][0]["target"] = "/etc"
    with pytest.raises(ProvisioningError) as target_denied:
        renderer.validate_document(arbitrary_target)
    assert target_denied.value.code == "mount_target_denied"


def test_privileged_es_denegado(renderer_context) -> None:
    renderer, _, _, _ = renderer_context
    payload = provisioning_payload()
    document = renderer._empty_document()
    document["services"]["worker-data-steward-f15"] = renderer._service(payload)
    document["services"]["worker-data-steward-f15"]["privileged"] = True

    with pytest.raises(ProvisioningError) as denied:
        renderer.validate_document(document)
    assert denied.value.code == "privileged_denied"
    hardened = renderer._empty_document()
    hardened["services"]["worker-data-steward-f15"] = renderer._service(payload)
    hardened["services"]["worker-data-steward-f15"]["read_only"] = False
    with pytest.raises(ProvisioningError) as hardening_denied:
        renderer.validate_document(hardened)
    assert hardening_denied.value.code == "hardening_required"


def test_apply_publico_materializa_catalogo_y_es_idempotente(api_context) -> None:
    client, factory, provisioner = api_context
    request_id = approved_request(client, "apply")
    headers = auth_headers("agent:operator", **{"Idempotency-Key": "f15-provision-apply"})

    first = client.post(
        f"/v1/agents/requests/{request_id}/provision",
        headers=headers,
        json={"reason": "Aplicar plantilla aprobada"},
    )
    replay = client.post(
        f"/v1/agents/requests/{request_id}/provision",
        headers=headers,
        json={"reason": "Aplicar plantilla aprobada"},
    )

    assert first.status_code == 202, first.text
    assert first.json()["status"] == "applied"
    assert replay.json()["replayed"] is True
    assert len(provisioner.applied) == 1
    denied = client.post(
        f"/v1/agents/requests/{request_id}/provision",
        headers=auth_headers("agent:leader", **{"Idempotency-Key": "f15-provision-denied"}),
        json={"reason": "Leader no aplica"},
    )
    assert denied.status_code == 403
    pending = client.post(
        "/v1/agents/requests",
        headers=auth_headers("agent:leader", **{"Idempotency-Key": "f15-pending-create"}),
        json=PAYLOAD | {"slug": "data-steward-pending"},
    )
    invalid = client.post(
        f"/v1/agents/requests/{pending.json()['id']}/provision",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": "f15-pending-provision"}),
        json={"reason": "No está aprobada"},
    )
    assert invalid.status_code == 409
    failed_id = approved_request(client, "failure")
    provisioner.failure = ProvisioningError("template_denied", "Plantilla denegada")
    failed = client.post(
        f"/v1/agents/requests/{failed_id}/provision",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": "f15-provision-failure"}),
        json={"reason": "Debe fallar cerrado"},
    )
    assert failed.status_code == 422
    provisioner.failure = None
    second_id = approved_request(client, "apply-second")
    with factory() as session:
        second = session.get(AgentRequestRecord, uuid.UUID(second_id))
        assert second is not None
        second.payload = second.payload | {"slug": "data-steward-apply"}
        session.commit()
    existing = client.post(
        f"/v1/agents/requests/{second_id}/provision",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": "f15-existing-agent"}),
        json={"reason": "Actualizar agente existente"},
    )
    assert existing.status_code == 202

    invalid_id = approved_request(client, "invalid-template")
    with factory() as session:
        invalid_record = session.get(AgentRequestRecord, uuid.UUID(invalid_id))
        assert invalid_record is not None
        invalid_record.payload = invalid_record.payload | {"role": "docker_admin"}
        session.commit()
    template_denied = client.post(
        f"/v1/agents/requests/{invalid_id}/provision",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": "f15-invalid-apply"}),
        json={"reason": "Debe denegarse"},
    )
    assert template_denied.status_code == 422
    with factory() as session:
        request = session.get(AgentRequestRecord, uuid.UUID(request_id))
        agent = session.scalar(select(Agent).where(Agent.slug == "data-steward-apply"))
        assert request is not None and request.status == "applied"
        assert agent is not None and agent.instances[0].health == "healthy"
        assert agent.policy_set["execution_profile_default"] == "sol-high"
        assert agent.policy_set["allowed_profiles"] == ["sol-high"]


def test_rollback_detiene_y_preserva_datos_logicos(api_context, renderer_context) -> None:
    client, factory, provisioner = api_context
    request_id = approved_request(client, "rollback")
    applied = client.post(
        f"/v1/agents/requests/{request_id}/provision",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": "f15-provision-rollback"}),
        json={"reason": "Aplicar antes de rollback"},
    )
    dynamic_headers = {
        "Authorization": f"Bearer {DYNAMIC_TOKEN}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }
    dynamic_tools = client.post(
        "/mcp/",
        headers=dynamic_headers,
        json={"jsonrpc": "2.0", "id": 16, "method": "tools/list", "params": {}},
    )
    dynamic_agents_denied = client.get(
        "/v1/agents",
        headers={"Authorization": f"Bearer {DYNAMIC_TOKEN}"},
    )
    headers = auth_headers("agent:operator", **{"Idempotency-Key": "f15-rollback-command"})
    rolled_back = client.post(
        f"/v1/agents/requests/{request_id}/rollback",
        headers=headers,
        json={"reason": "Rollback controlado"},
    )
    replay = client.post(
        f"/v1/agents/requests/{request_id}/rollback",
        headers=headers,
        json={"reason": "Rollback controlado"},
    )

    assert applied.status_code == 202, applied.text
    assert dynamic_tools.status_code == 200, dynamic_tools.text
    assert {tool["name"] for tool in dynamic_tools.json()["result"]["tools"]} == {"task_get"}
    assert dynamic_agents_denied.status_code == 403
    assert rolled_back.status_code == 202
    assert rolled_back.json()["status"] == "rolled_back"
    assert replay.json()["replayed"] is True
    assert len(provisioner.rolled_back) == 1
    conflict = client.post(
        f"/v1/agents/requests/{request_id}/rollback",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": "f15-rollback-different"}),
        json={"reason": "Clave conflictiva"},
    )
    assert conflict.status_code == 409
    revoked = client.post(
        "/mcp/",
        headers=dynamic_headers,
        json={"jsonrpc": "2.0", "id": 17, "method": "tools/list", "params": {}},
    )
    assert revoked.status_code == 401
    assert revoked.json()["detail"]["code"] == "token_revoked"
    orphan_id = approved_request(client, "orphan")
    orphan = client.post(
        f"/v1/agents/requests/{orphan_id}/rollback",
        headers=auth_headers("agent:operator", **{"Idempotency-Key": "f15-orphan-rollback"}),
        json={"reason": "No existe agente materializado"},
    )
    assert orphan.status_code == 404
    with factory() as session:
        agent = session.scalar(select(Agent).where(Agent.slug == "data-steward-rollback"))
        assert agent is not None and agent.desired_state == "disabled"
        assert agent.instances[0].reconciliation_state == "rolled_back"
        events = list(
            session.scalars(select(AuditEvent).where(AuditEvent.aggregate_id == request_id))
        )
        request = session.get(AgentRequestRecord, uuid.UUID(request_id))
        assert request is not None
        assert request.status == "retired"
        assert "runtime_auth_token_sha256" not in agent.policy_set
        assert agent.policy_set["revoked_runtime_auth_token_sha256"] == DYNAMIC_TOKEN_SHA256
        assert {event.event_type for event in events} >= {
            "agent.provisioning_rolled_back",
            "agent.request_retired",
        }

    renderer, fleet, _, data = renderer_context
    payload = provisioning_payload(slug="data-steward-renderer-rollback")
    renderer.apply(payload, fleet)
    renderer_rollback = renderer.rollback(payload, fleet)
    renderer_replay = renderer.rollback(payload, fleet)
    assert renderer_rollback.status == "rolled_back"
    assert renderer_replay.status == "rolled_back"
    assert fleet.rollback_calls == [["worker-data-steward-renderer-rollback"]]
    assert (data / payload.slug).exists()
    renderer_failure = ManagedAgentRenderer(
        managed_root=data / "failure-managed",
        data_root=data / "failure-data",
        host_data_root="/host_mnt/failure-data",
        dataset_root="/host_mnt/tradix/dataset",
        worker_image="hermes-worker:test",
        project_name="hermes-test",
    )
    with pytest.raises(ProvisioningError) as apply_failed:
        renderer_failure.apply(
            provisioning_payload(slug="data-steward-fleet-failure"), FailingFleet()
        )
    assert apply_failed.value.code == "fleet_apply_failed"

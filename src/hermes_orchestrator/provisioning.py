from __future__ import annotations

import hashlib
import json
import os
import posixpath
import re
import secrets
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any, Literal, Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

from hermes_orchestrator.config import Settings

AgentTemplateRole = Literal[
    "leader",
    "researcher",
    "developer",
    "validator",
    "operator",
    "data_steward",
    "risk_manager",
    "trader",
]

FORBIDDEN_TEMPLATE_KEYS = {
    "command",
    "entrypoint",
    "privileged",
    "network_mode",
    "pid",
    "pid_mode",
    "volumes",
    "mounts",
    "docker_socket",
}
FORBIDDEN_SERVICE_KEYS = {
    "command",
    "entrypoint",
    "privileged",
    "network_mode",
    "pid",
    "pid_mode",
}
ALLOWED_MOUNT_TARGETS = {
    "/opt/data",
    "/home/hermes",
    "/workspace",
    "/bootstrap",
    "/datasets/tradix",
    "/workspace/repos/tradix",
}
PROGRAM_EXECUTION_PROFILE = "sol-high"
PROGRAM_ALLOWED_PROFILES = [PROGRAM_EXECUTION_PROFILE]
WORKER_API_SECRET_PREFIX = "secret://hermes/api-server/"
ROLE_TEMPLATE_ROOT = (Path(__file__).parent / "agent_templates").resolve()
KNOWLEDGE_DATASET_ROLES = {"data_steward", "risk_manager", "trader"}
TRADIX_READONLY_ROLES = {"risk_manager", "trader"}
KNOWLEDGE_WRITER_ROLES = {"data_steward", "risk_manager", "trader"}
REQUIRED_ROLE_TEMPLATE_FILES = {"manifest.json", "SOUL.md", "foundation.md", "context.md"}


class ProvisioningError(Exception):
    def __init__(self, code: str, detail: str, status_code: int = 422) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code


class ProvisioningPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: uuid.UUID
    slug: str = Field(pattern=r"^[a-z][a-z0-9-]{2,39}$")
    role: AgentTemplateRole
    description: str = Field(min_length=1, max_length=2000)
    policy_set: dict[str, Any] = Field(default_factory=dict)
    capabilities: list[str] = Field(default_factory=list)
    secret_refs: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def deny_template_escape(self) -> ProvisioningPayload:
        def visit(value: Any) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    if str(key).lower() in FORBIDDEN_TEMPLATE_KEYS:
                        raise ValueError(f"Campo de plantilla denegado: {key}")
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        visit(self.policy_set)
        requested_default = self.policy_set.get(
            "execution_profile_default", PROGRAM_EXECUTION_PROFILE
        )
        requested_allowed = self.policy_set.get("allowed_profiles", PROGRAM_ALLOWED_PROFILES)
        if requested_default != PROGRAM_EXECUTION_PROFILE:
            raise ValueError(f"execution_profile_default debe ser {PROGRAM_EXECUTION_PROFILE}")
        if requested_allowed != PROGRAM_ALLOWED_PROFILES:
            raise ValueError(f"allowed_profiles debe ser exactamente {PROGRAM_ALLOWED_PROFILES}")
        return self


class ProvisionerResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["applied", "no_change", "rolled_back"]
    service_name: str
    config_digest: str
    health: str
    credential_sha256: str | None = None
    runner_result: dict[str, Any] = Field(default_factory=dict)


class WorkerCredentialRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    secret_ref: str = Field(pattern=r"^secret://hermes/api-server/[a-z][a-z0-9-]{2,39}$")


class WorkerCredentialResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    secret_ref: str
    credential: str


class AgentProvisioner(Protocol):
    def apply(self, payload: ProvisioningPayload) -> ProvisionerResult: ...

    def rollback(self, payload: ProvisioningPayload) -> ProvisionerResult: ...


class FleetManagedRunner(Protocol):
    def status(self) -> dict[str, Any]: ...

    def apply(self, services: list[str]) -> dict[str, Any]: ...

    def rollback(self, services: list[str]) -> dict[str, Any]: ...


class HttpAgentProvisionerClient:
    def __init__(self, settings: Settings) -> None:
        self.base_url = settings.agent_provisioner_url.rstrip("/")
        self.token = settings.agent_provisioner_token

    def _request(self, path: str, payload: ProvisioningPayload) -> ProvisionerResult:
        if not self.token:
            raise ProvisioningError(
                "provisioner_token_missing", "Falta el token interno del provisioner", 503
            )
        try:
            with httpx.Client(timeout=300) as client:
                response = client.post(
                    f"{self.base_url}{path}",
                    headers={"X-Provisioner-Token": self.token},
                    json=payload.model_dump(mode="json"),
                )
        except httpx.HTTPError as error:
            raise ProvisioningError(
                "provisioner_unavailable", "Agent provisioner no disponible", 503
            ) from error
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", {})
                code = str(detail.get("code", "provisioner_rejected"))
                message = str(detail.get("detail", "Provisioning rechazado"))
            except (AttributeError, ValueError):
                code, message = "provisioner_rejected", "Provisioning rechazado"
            raise ProvisioningError(code, message, response.status_code)
        return ProvisionerResult.model_validate(response.json())

    def apply(self, payload: ProvisioningPayload) -> ProvisionerResult:
        return self._request("/v1/internal/agents/apply", payload)

    def rollback(self, payload: ProvisioningPayload) -> ProvisionerResult:
        return self._request("/v1/internal/agents/rollback", payload)

    def resolve_worker_credential(self, secret_ref: str) -> str:
        if not self.token:
            raise ProvisioningError(
                "provisioner_token_missing", "Falta el token interno del provisioner", 503
            )
        try:
            with httpx.Client(timeout=10) as client:
                response = client.post(
                    f"{self.base_url}/v1/internal/agents/credential",
                    headers={"X-Provisioner-Token": self.token},
                    json={"secret_ref": secret_ref},
                )
        except httpx.HTTPError as error:
            raise ProvisioningError(
                "provisioner_unavailable", "Agent provisioner no disponible", 503
            ) from error
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", {})
                code = str(detail.get("code", "worker_credential_rejected"))
                message = str(detail.get("detail", "Credencial de worker rechazada"))
            except (AttributeError, ValueError):
                code, message = (
                    "worker_credential_rejected",
                    "Credencial de worker rechazada",
                )
            raise ProvisioningError(code, message, response.status_code)
        try:
            return WorkerCredentialResponse.model_validate(response.json()).credential
        except (ValueError, TypeError) as error:
            raise ProvisioningError(
                "worker_credential_invalid",
                "Respuesta de credencial de worker inválida",
                503,
            ) from error


class ManagedAgentRenderer:
    def __init__(
        self,
        *,
        managed_root: Path,
        data_root: Path,
        host_data_root: str,
        dataset_root: str,
        tradix_root: str = "",
        worker_image: str,
        project_name: str,
        knowledge_repository_url: str = "",
        github_login: str = "",
        github_token: str = "",
    ) -> None:
        self.managed_root = managed_root.resolve()
        self.data_root = data_root.resolve()
        self.host_data_root = posixpath.normpath(host_data_root)
        self.dataset_root = posixpath.normpath(dataset_root)
        self.tradix_root = posixpath.normpath(tradix_root) if tradix_root else ""
        self.worker_image = worker_image
        self.project_name = project_name
        self.knowledge_repository_url = knowledge_repository_url.strip()
        self.github_login = github_login.strip()
        self.github_token = github_token.strip()
        self.compose_path = self.managed_root / "agents.compose.yaml"

    def _role_template(self, role: AgentTemplateRole) -> tuple[dict[str, Any], Path] | None:
        template_root = ROLE_TEMPLATE_ROOT / role
        if not template_root.exists():
            return None
        resolved = template_root.resolve()
        if not resolved.is_relative_to(ROLE_TEMPLATE_ROOT):
            raise ProvisioningError("template_root_denied", "Raíz de template fuera del catálogo")
        names = {
            path.name
            for path in resolved.iterdir()
            if path.is_file() and path.name != "__init__.py"
        }
        if names != REQUIRED_ROLE_TEMPLATE_FILES:
            raise ProvisioningError("template_files_invalid", "Template de rol incompleto")
        try:
            manifest = json.loads((resolved / "manifest.json").read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as error:
            raise ProvisioningError(
                "template_manifest_invalid", "Manifest de rol inválido"
            ) from error
        if not isinstance(manifest, dict) or manifest.get("role") != role:
            raise ProvisioningError("template_manifest_invalid", "Manifest de rol incompatible")
        return manifest, resolved

    def _validate_role_template(self, payload: ProvisioningPayload) -> dict[str, Any] | None:
        loaded = self._role_template(payload.role)
        if loaded is None:
            return None
        template, _ = loaded
        checks = (
            ("capabilities", payload.capabilities, template.get("allowed_capabilities")),
            ("secret_refs", payload.secret_refs, template.get("allowed_secret_refs")),
            (
                "toolsets",
                payload.policy_set.get("toolsets", []),
                template.get("allowed_toolsets"),
            ),
            (
                "mcp_tools",
                payload.policy_set.get("mcp_tools", []),
                template.get("allowed_mcp_tools"),
            ),
            (
                "communication",
                payload.policy_set.get("communication", []),
                template.get("allowed_communication"),
            ),
        )
        for key, requested, allowed in checks:
            if not isinstance(requested, list) or not isinstance(allowed, list):
                raise ProvisioningError(
                    "template_manifest_invalid", "Allowlist de template inválida"
                )
            if not set(requested).issubset(set(allowed)):
                raise ProvisioningError(f"{key}_not_allowlisted", f"{key} fuera del template")
        if payload.policy_set.get("name") not in template.get("policy_names", []):
            raise ProvisioningError("policy_name_not_allowlisted", "Policy fuera del template")
        if payload.policy_set.get("mount_profile") not in template.get("mount_profiles", []):
            raise ProvisioningError("mount_not_allowlisted", "Mount profile fuera del template")
        return template

    @staticmethod
    def _digest(document: dict[str, Any]) -> str:
        canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
        return f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"

    def _empty_document(self) -> dict[str, Any]:
        return {
            "name": self.project_name,
            "services": {},
        }

    def _read_document(self) -> dict[str, Any]:
        if not self.compose_path.exists():
            return self._empty_document()
        try:
            value = json.loads(self.compose_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as error:
            raise ProvisioningError(
                "managed_compose_invalid", "Compose managed inválido"
            ) from error
        if not isinstance(value, dict):
            raise ProvisioningError("managed_compose_invalid", "Compose managed inválido")
        return value

    def ensure_document(self) -> None:
        self.managed_root.mkdir(parents=True, exist_ok=True)
        if not self.compose_path.exists():
            self.compose_path.write_text(
                json.dumps(self._empty_document(), indent=2) + "\n", encoding="utf-8"
            )

    def resolve_worker_credential(self, secret_ref: str) -> str:
        try:
            request = WorkerCredentialRequest(secret_ref=secret_ref)
        except ValueError as error:
            raise ProvisioningError(
                "worker_secret_ref_invalid", "Referencia de API Hermes inválida"
            ) from error
        slug = request.secret_ref.removeprefix(WORKER_API_SECRET_PREFIX)
        try:
            runtime_path = (self.data_root / slug / "runtime" / "runtime.env").resolve(strict=True)
        except OSError as error:
            raise ProvisioningError(
                "worker_credential_missing", "Credencial de worker no disponible", 404
            ) from error
        if not runtime_path.is_relative_to(self.data_root):
            raise ProvisioningError(
                "worker_secret_ref_invalid", "Referencia de API Hermes inválida"
            )
        try:
            for line in runtime_path.read_text(encoding="utf-8").splitlines():
                key, separator, value = line.partition("=")
                if separator and key == "API_SERVER_KEY" and value:
                    return value
        except OSError as error:
            raise ProvisioningError(
                "worker_credential_missing", "Credencial de worker no disponible", 404
            ) from error
        raise ProvisioningError(
            "worker_credential_missing", "Credencial de worker no disponible", 404
        )

    def _mount(self, slug: str, leaf: str, target: str, read_only: bool = False) -> dict[str, Any]:
        return {
            "type": "bind",
            "source": posixpath.join(self.host_data_root, slug, leaf),
            "target": target,
            "read_only": read_only,
        }

    def _service(self, payload: ProvisioningPayload) -> dict[str, Any]:
        self._validate_role_template(payload)
        slug = payload.slug
        service: dict[str, Any] = {
            "image": self.worker_image,
            "restart": "unless-stopped",
            "read_only": True,
            "init": True,
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"],
            "tmpfs": ["/tmp:rw,noexec,nosuid,size=64m"],
            "env_file": [f"../hermes-agents-data/{slug}/runtime/runtime.env"],
            "environment": {
                "TZ": "Europe/Madrid",
                "HOME": "/home/hermes",
                "HERMES_HOME": "/opt/data",
                "HERMES_WRITE_SAFE_ROOT": (
                    "/workspace" if payload.role in KNOWLEDGE_WRITER_ROLES else "/opt/data"
                ),
                "HERMES_ORIG_CWD": "/workspace",
                "API_SERVER_ENABLED": "true",
                "API_SERVER_HOST": "0.0.0.0",
                "API_SERVER_PORT": "8642",
                "OPENAI_BASE_URL": "http://codex-broker:8650/v1",
                "HERMES_AGENT_ID": payload.role,
                "HERMES_AGENT_ROLE": payload.role,
                "HERMES_AGENT_SLUG": slug,
            },
            "volumes": [
                self._mount(slug, "hermes", "/opt/data"),
                self._mount(slug, "home", "/home/hermes"),
                self._mount(slug, "workspace", "/workspace"),
                self._mount(slug, "managed", "/bootstrap", True),
            ],
            "expose": ["8642"],
            "networks": ["control", "broker-plane"],
            "depends_on": {
                "orchestrator-api": {"condition": "service_healthy"},
                "codex-broker": {"condition": "service_healthy"},
            },
            "healthcheck": {
                "test": [
                    "CMD",
                    "python",
                    "-c",
                    "import urllib.request; "
                    "urllib.request.urlopen('http://127.0.0.1:8642/health', "
                    "timeout=2).read()",
                ],
                "interval": "3s",
                "timeout": "3s",
                "retries": 30,
                "start_period": "10s",
            },
            "labels": {
                "io.hermes.provisioned": "managed-v2",
                "io.hermes.agent.role": payload.role,
                "io.hermes.agent.request": str(payload.request_id),
            },
        }
        if payload.role in KNOWLEDGE_DATASET_ROLES:
            if self.knowledge_repository_url:
                service["environment"]["HERMES_KNOWLEDGE_REPOSITORY_URL"] = (
                    self.knowledge_repository_url
                )
                service["environment"]["HERMES_KNOWLEDGE_WORKTREE"] = (
                    "/workspace/repos/hermes-tradix-knowledge"
                )
            service["volumes"].append(
                {
                    "type": "bind",
                    "source": self.dataset_root,
                    "target": "/datasets/tradix",
                    "read_only": True,
                }
            )
        if payload.role in TRADIX_READONLY_ROLES:
            if not self.tradix_root:
                raise ProvisioningError("tradix_root_missing", "Falta el root read-only de Tradix")
            service["volumes"].append(
                {
                    "type": "bind",
                    "source": self.tradix_root,
                    "target": "/workspace/repos/tradix",
                    "read_only": True,
                }
            )
        return service

    def validate_document(self, document: dict[str, Any]) -> None:
        if set(document) != {"name", "services"}:
            raise ProvisioningError("managed_document_denied", "Claves raíz no permitidas")
        if document.get("name") != self.project_name:
            raise ProvisioningError("project_not_allowed", "Proyecto managed no permitido")
        services = document.get("services")
        if not isinstance(services, dict):
            raise ProvisioningError("services_invalid", "Services managed inválidos")
        for name, service in services.items():
            if not re.fullmatch(r"worker-[a-z][a-z0-9-]{2,39}", str(name)):
                raise ProvisioningError("slug_invalid", "Slug de servicio inválido")
            if not isinstance(service, dict):
                raise ProvisioningError("service_invalid", "Servicio managed inválido")
            if service.get("image") != self.worker_image:
                raise ProvisioningError("image_not_allowed", "Imagen no allowlisted")
            for key in FORBIDDEN_SERVICE_KEYS:
                if key in service:
                    raise ProvisioningError(f"{key}_denied", f"{key} está denegado")
            if service.get("cap_drop") != ["ALL"] or service.get("read_only") is not True:
                raise ProvisioningError("hardening_required", "Hardening de worker incompleto")
            for mount in service.get("volumes", []):
                source = posixpath.normpath(str(mount.get("source", "")))
                target = str(mount.get("target", ""))
                dataset_mount = (
                    source == self.dataset_root
                    and target == "/datasets/tradix"
                    and mount.get("read_only") is True
                )
                tradix_mount = (
                    bool(self.tradix_root)
                    and source == self.tradix_root
                    and target == "/workspace/repos/tradix"
                    and mount.get("read_only") is True
                )
                try:
                    inside = (
                        dataset_mount
                        or tradix_mount
                        or (
                            posixpath.commonpath((source, self.host_data_root))
                            == self.host_data_root
                        )
                    )
                except ValueError:
                    inside = dataset_mount
                if not inside:
                    raise ProvisioningError("mount_root_denied", "Mount externo denegado")
                if "docker.sock" in source.lower() or "docker.sock" in target.lower():
                    raise ProvisioningError("docker_socket_denied", "Socket Docker denegado")
                if target not in ALLOWED_MOUNT_TARGETS:
                    raise ProvisioningError("mount_target_denied", "Destino de mount denegado")

    def _write_agent_files(self, payload: ProvisioningPayload) -> str:
        agent_root = self.managed_root / "agents" / payload.slug
        runtime_root = self.data_root / payload.slug / "runtime"
        for leaf in ("hermes", "home", "workspace", "workspace/repos", "managed"):
            worker_path = self.data_root / payload.slug / leaf
            worker_path.mkdir(parents=True, exist_ok=True)
            with suppress(OSError, AttributeError):
                os.chown(worker_path, -1, 10000)
                worker_path.chmod(0o770)
        agent_root.mkdir(parents=True, exist_ok=True)
        runtime_root.mkdir(parents=True, exist_ok=True)
        if "secret://github/shared-agent" in payload.secret_refs:
            if not self.github_login or not self.github_token:
                raise ProvisioningError(
                    "github_credential_missing",
                    "Falta la credencial GitHub estándar del agente",
                    503,
                )
            github_config = self.data_root / payload.slug / "home" / ".config" / "gh"
            github_config.mkdir(parents=True, exist_ok=True)
            hosts_path = github_config / "hosts.yml"
            hosts_path.write_text(
                "github.com:\n"
                f"  user: {json.dumps(self.github_login)}\n"
                f"  oauth_token: {json.dumps(self.github_token)}\n"
                "  git_protocol: https\n",
                encoding="utf-8",
            )
            for secure_path in (github_config.parent, github_config, hosts_path):
                with suppress(OSError, AttributeError):
                    # El provisioner no es root: puede compartir el path con el
                    # grupo del worker, pero no transferir su propietario. Si
                    # se intenta cambiar ambos valores, chown falla completo y
                    # Hermes pierde acceso a su configuración persistente.
                    os.chown(secure_path, -1, 10000)
            github_config.parent.chmod(0o770)
            github_config.chmod(0o770)
            hosts_path.chmod(0o660)
        template = self._validate_role_template(payload)
        manifest = payload.model_dump(mode="json")
        runtime_manifest = {
            "id": payload.role,
            "role": payload.role,
            "description": payload.description,
            "workspace": "/workspace",
            "execution_profile_default": PROGRAM_EXECUTION_PROFILE,
            "allowed_profiles": PROGRAM_ALLOWED_PROFILES,
            "toolsets": payload.policy_set.get("toolsets", ["terminal_read", "files_read", "mcp"]),
            "mcp_tools": payload.policy_set.get(
                "mcp_tools", ["task_get", "task_comment", "task_block", "task_complete"]
            ),
            "capabilities": payload.capabilities,
            "denied_capabilities": (
                template.get("denied_capabilities", [])
                if template is not None
                else ["docker_admin", "secret_read", "environment_promote", "live_trade"]
            ),
            "secret_refs": payload.secret_refs,
            "communication": payload.policy_set.get("communication", []),
            "mount_profile": payload.policy_set.get("mount_profile", "tradix-dataset-readonly"),
            "budget": payload.policy_set.get("budget", {}),
        }
        if payload.role in KNOWLEDGE_WRITER_ROLES:
            runtime_manifest["approvals_mode"] = "off"
        (agent_root / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        references = [
            f"MCP_HERMES_ORCHESTRATOR_API_KEY=secret://orchestrator/internal-auth/{payload.slug}",
            f"API_SERVER_KEY=secret://hermes/api-server/{payload.slug}",
            "OPENAI_API_KEY=secret://codex/broker-client",
        ]
        if "secret://github/shared-agent" in payload.secret_refs:
            references.append("GITHUB_AUTH=secret://github/shared-agent")
        (agent_root / "runtime.env.ref").write_text("\n".join(references) + "\n", encoding="utf-8")
        (self.data_root / payload.slug / "managed" / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (self.data_root / payload.slug / "managed" / "manifest.yaml").write_text(
            json.dumps(runtime_manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        loaded = self._role_template(payload.role)
        if loaded is not None:
            _, template_root = loaded
            for name in ("SOUL.md", "foundation.md", "context.md"):
                (self.data_root / payload.slug / "managed" / name).write_text(
                    (template_root / name).read_text(encoding="utf-8"),
                    encoding="utf-8",
                )
        runtime_path = runtime_root / "runtime.env"
        runtime_token: str | None = None
        if runtime_path.exists():
            for line in runtime_path.read_text(encoding="utf-8").splitlines():
                key, separator, value = line.partition("=")
                if separator and key == "MCP_HERMES_ORCHESTRATOR_API_KEY":
                    runtime_token = value
                    break
        if runtime_token is None:
            runtime_token = secrets.token_urlsafe(32)
            runtime_path.write_text(
                "\n".join(
                    [
                        f"API_SERVER_KEY={secrets.token_urlsafe(32)}",
                        f"MCP_HERMES_ORCHESTRATOR_API_KEY={runtime_token}",
                        "OPENAI_API_KEY="
                        + os.environ.get("CODEX_BROKER_CLIENT_KEY", "managed-at-runtime"),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
        with suppress(OSError, AttributeError):
            os.chown(runtime_path, -1, 0)
            runtime_path.chmod(0o640)
        return hashlib.sha256(runtime_token.encode("utf-8")).hexdigest()

    def apply(self, payload: ProvisioningPayload, fleet: FleetManagedRunner) -> ProvisionerResult:
        self.managed_root.mkdir(parents=True, exist_ok=True)
        document = self._read_document()
        previous = json.dumps(document, sort_keys=True, separators=(",", ":"))
        service_name = f"worker-{payload.slug}"
        document.setdefault("services", {})[service_name] = self._service(payload)
        self.validate_document(document)
        current = json.dumps(document, sort_keys=True, separators=(",", ":"))
        credential_sha256 = self._write_agent_files(payload)
        if current == previous:
            runner_result = fleet.status()
            health = (
                "healthy"
                if any(
                    item.get("service") == service_name and item.get("health") == "healthy"
                    for item in runner_result.get("services", [])
                )
                else "unknown"
            )
            return ProvisionerResult(
                status="no_change",
                service_name=service_name,
                config_digest=self._digest(document),
                health=health,
                credential_sha256=credential_sha256,
                runner_result=runner_result,
            )
        self.compose_path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        try:
            runner_result = fleet.apply([service_name])
        except Exception as error:
            self.compose_path.write_text(
                json.dumps(json.loads(previous), indent=2) + "\n", encoding="utf-8"
            )
            raise ProvisioningError(
                "fleet_apply_failed", "Fleet reconciler no pudo aplicar el worker", 503
            ) from error
        health = (
            "healthy"
            if any(
                item.get("service") == service_name and item.get("health") == "healthy"
                for item in runner_result.get("services", [])
            )
            else "starting"
        )
        return ProvisionerResult(
            status="applied",
            service_name=service_name,
            config_digest=self._digest(document),
            health=health,
            credential_sha256=credential_sha256,
            runner_result=runner_result,
        )

    def rollback(
        self, payload: ProvisioningPayload, fleet: FleetManagedRunner
    ) -> ProvisionerResult:
        document = self._read_document()
        service_name = f"worker-{payload.slug}"
        services = document.get("services", {})
        if service_name not in services:
            return ProvisionerResult(
                status="rolled_back",
                service_name=service_name,
                config_digest=self._digest(document),
                health="stopped",
            )
        runner_result = fleet.rollback([service_name])
        del services[service_name]
        self.validate_document(document)
        self.compose_path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        return ProvisionerResult(
            status="rolled_back",
            service_name=service_name,
            config_digest=self._digest(document),
            health="stopped",
            runner_result=runner_result,
        )

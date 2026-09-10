"""Backend Swarm restringido para workers gestionados, con identidad singleton."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import httpx

TERMINAL_TASK_STATES = {"complete", "shutdown", "failed", "rejected", "remove", "orphaned"}


def registry_auth(entry: dict[str, Any], host: str) -> str:
    """Docker config almacena auth; Engine requiere campos AuthConfig explícitos."""
    value = {key: entry[key] for key in ("username", "password", "identitytoken") if key in entry}
    if "auth" in entry:
        try:
            decoded = base64.b64decode(entry["auth"], validate=True).decode()
            username, separator, password = decoded.partition(":")
            if not separator:
                raise ValueError
        except (ValueError, UnicodeError) as error:
            raise ValueError("credencial registry con formato inválido") from error
        value.update(username=username, password=password)
    value["serveraddress"] = host
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode()


def duration(value: str | int) -> int:
    if isinstance(value, int):
        return value
    match = re.fullmatch(r"(\d+)(ns|us|ms|s|m|h)", value)
    if not match:
        raise ValueError("duración Swarm no soportada")
    return (
        int(match[1])
        * {"ns": 1, "us": 1000, "ms": 10**6, "s": 10**9, "m": 60 * 10**9, "h": 3600 * 10**9}[
            match[2]
        ]
    )


def service_spec(
    project: str, name: str, source: dict[str, Any], node_id: str, networks: dict[str, str]
) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,60}", project) or not re.fullmatch(
        r"worker-[a-z][a-z0-9-]{2,60}", name
    ):
        raise ValueError("identidad Swarm no permitida")
    if not node_id:
        raise ValueError("FLEET_SWARM_NODE_ID obligatorio para persistencia local")
    supported = {
        "image",
        "restart",
        "read_only",
        "init",
        "cap_drop",
        "cap_add",
        "security_opt",
        "tmpfs",
        "environment",
        "volumes",
        "expose",
        "networks",
        "depends_on",
        "healthcheck",
        "labels",
        "command",
        "entrypoint",
        "user",
        "working_dir",
        "stop_grace_period",
        "group_add",
        "deploy",
        "gpus",
    }
    if set(source) - supported:
        raise ValueError(
            "claves Compose sin equivalencia Swarm: " + ",".join(sorted(set(source) - supported))
        )
    if not re.fullmatch(
        r"(?:host\.docker\.internal|192\.168\.1\.197):5443/tradix/[a-z0-9/-]+@sha256:[a-f0-9]{64}",
        source["image"],
    ):
        raise ValueError("imagen requiere registry Tradix y digest")
    if set(source.get("security_opt", [])) - {"no-new-privileges:true", "no-new-privileges"}:
        raise ValueError("security_opt sin equivalencia")
    if source.get("gpus"):
        # Docker Desktop actualmente no anuncia GPU a Swarm; no degradar a CPU.
        raise ValueError(
            "GPU Swarm requiere runtime NVIDIA y recursos genéricos verificados; "
            "gpus Compose no es trasladable"
        )
    if source.get("deploy"):
        raise ValueError("deploy arbitrario denegado; singleton y placement fijados por el backend")
    container: dict[str, Any] = {
        "Image": source["image"],
        "Init": source.get("init", True),
        "ReadOnly": source.get("read_only", True),
        "CapabilityDrop": source.get("cap_drop", ["ALL"]),
        "Privileges": {"NoNewPrivileges": True},
        "Env": [f"{k}={v}" for k, v in source.get("environment", {}).items() if v is not None],
        "Labels": source.get("labels", {}),
        "Mounts": [],
        "StopGracePeriod": duration(source.get("stop_grace_period", "8s")),
    }
    if source.get("cap_add"):
        raise ValueError("cap_add denegado")
    for key, target in (
        ("command", "Args"),
        ("entrypoint", "Command"),
        ("user", "User"),
        ("working_dir", "Dir"),
        ("group_add", "Groups"),
    ):
        if key in source:
            if key in ("command", "entrypoint") and not isinstance(source[key], list):
                raise ValueError("comandos requieren lista explícita")
            container[target] = source[key]
    for mount in source.get("volumes", []):
        if mount.get("type") != "bind" or not mount.get("source", "").startswith("/"):
            raise ValueError("worker requiere bind absoluto previamente permitido")
        if "docker.sock" in mount["source"] or "docker.sock" in mount["target"]:
            raise ValueError("socket Docker denegado al worker")
        container["Mounts"].append(
            {
                "Type": "bind",
                "Source": mount["source"],
                "Target": mount["target"],
                "ReadOnly": mount.get("read_only", False),
            }
        )
    for value in source.get("tmpfs", []):
        target, _, opts = value.partition(":")
        flags = opts.split(",") if opts else []
        size = next((x[5:] for x in flags if x.startswith("size=")), "64m")
        if set(flags) - {"rw", "noexec", "nosuid", "nodev", "size=" + size}:
            raise ValueError("opciones tmpfs no soportadas")
        if not re.fullmatch(r"\d+[mk]?", size):
            raise ValueError("tamaño tmpfs inválido")
        factor = {"m": 1024**2, "k": 1024}.get(size[-1], 1)
        amount = int(size[:-1] if size[-1] in "mk" else size) * factor
        container["Mounts"].append(
            {
                "Type": "tmpfs",
                "Target": target,
                "TmpfsOptions": {
                    "SizeBytes": amount,
                    "Options": [[x] for x in flags if x in ("noexec", "nosuid", "nodev")],
                },
            }
        )
    if health := source.get("healthcheck"):
        container["Healthcheck"] = {"Test": health["test"], "Retries": health.get("retries", 3)}
        for key, target in (
            ("interval", "Interval"),
            ("timeout", "Timeout"),
            ("start_period", "StartPeriod"),
        ):
            if key in health:
                container["Healthcheck"][target] = duration(health[key])
    attachments = []
    for network in source.get("networks", []):
        if network not in networks:
            raise ValueError("red de rol sin mapping overlay: " + network)
        attachments.append({"Target": networks[network], "Aliases": [name]})
    effective = {"container": container, "networks": attachments, "node": node_id}
    revision = hashlib.sha256(json.dumps(effective, sort_keys=True).encode()).hexdigest()
    container["Labels"] = dict(container["Labels"], **{"io.hermes.fleet.spec": revision})
    return {
        "Name": f"{project}_{name}",
        "Labels": {"io.hermes.fleet.project": project, "io.hermes.fleet.worker": name},
        "TaskTemplate": {
            "ContainerSpec": container,
            "Networks": attachments,
            "Placement": {"Constraints": [f"node.id=={node_id}"]},
            "RestartPolicy": {"Condition": "any", "Delay": 3 * 10**9},
        },
        "Mode": {"Replicated": {"Replicas": 1}},
        "UpdateConfig": {
            "Parallelism": 1,
            "Order": "stop-first",
            "FailureAction": "rollback",
            "Monitor": 30 * 10**9,
        },
        "RollbackConfig": {"Parallelism": 1, "Order": "stop-first", "Monitor": 30 * 10**9},
    }


class SwarmBackend:
    def __init__(self, project: str) -> None:
        self.project = project
        self.node_id = os.environ.get("FLEET_SWARM_NODE_ID", "")
        self.networks = json.loads(os.environ.get("FLEET_SWARM_NETWORKS", "{}"))
        self.client = httpx.Client(
            transport=httpx.HTTPTransport(uds="/var/run/docker.sock"),
            base_url="http://docker/v1.47",
            timeout=30,
        )

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.client.request(method, path, **kwargs)
        if response.status_code >= 400:
            raise ValueError(f"operación Swarm fallida: HTTP {response.status_code}")
        return response.json() if response.content else None

    def owned(self, name: str) -> dict[str, Any] | None:
        items = self.request(
            "GET", "/services", params={"filters": json.dumps({"name": [f"{self.project}_{name}"]})}
        )
        exact = [item for item in items if item["Spec"]["Name"] == f"{self.project}_{name}"]
        if not exact:
            return None
        item = exact[0]
        if (
            item["Spec"].get("Labels", {}).get("io.hermes.fleet.project") != self.project
            or item["Spec"].get("Labels", {}).get("io.hermes.fleet.worker") != name
        ):
            raise ValueError("servicio preexistente sin propiedad del backend")
        return item

    def status(self) -> list[dict[str, Any]]:
        services = self.request(
            "GET",
            "/services",
            params={"filters": json.dumps({"label": [f"io.hermes.fleet.project={self.project}"]})},
        )
        result = []
        for service in services:
            tasks = self.request(
                "GET",
                "/tasks",
                params={
                    "filters": json.dumps(
                        {"service": [service["ID"]], "desired-state": ["running"]}
                    )
                },
            )
            healthy = False
            running = [task for task in tasks if task["Status"]["State"] == "running"]
            if len(running) == 1:
                cid = running[0]["Status"].get("ContainerStatus", {}).get("ContainerID")
                if cid:
                    try:
                        info = self.request("GET", f"/containers/{cid}/json")
                        healthy = info["State"].get("Health", {}).get("Status") == "healthy"
                    except ValueError:
                        pass  # El manager no inspecciona contenedores de un nodo remoto.
            result.append(
                {
                    "name": service["Spec"]["Name"],
                    "service": service["Spec"]["Labels"]["io.hermes.fleet.worker"],
                    "image": service["Spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
                    "state": "running" if running else "pending",
                    "health": "healthy" if healthy else "unknown",
                    "update_state": service.get("UpdateStatus", {}).get("State", "completed"),
                    "task_image": running[0].get("Spec", {}).get("ContainerSpec", {}).get("Image")
                    if len(running) == 1
                    else None,
                    "spec_revision": running[0]
                    .get("Spec", {})
                    .get("ContainerSpec", {})
                    .get("Labels", {})
                    .get("io.hermes.fleet.spec")
                    if len(running) == 1
                    else None,
                }
            )
        return result

    def apply(self, rendered: dict[str, Any], names: list[str]) -> None:
        # Compilar y comprobar propiedad de todo el lote antes de la primera mutación.
        plans = [
            (
                name,
                service_spec(
                    self.project, name, rendered["services"][name], self.node_id, self.networks
                ),
                self.owned(name),
            )
            for name in names
        ]
        for name in names:
            for dependency in rendered["services"][name].get("depends_on", {}):
                self.require_healthy_dependency(dependency)
        auth = {}
        if path := os.environ.get("FLEET_REGISTRY_AUTH_FILE"):
            auth = json.loads(Path(path).read_text())["auths"]
        completed = []
        try:
            for name, spec, existing in plans:
                self._apply_one(name, spec, existing, auth)
                completed.append((name, existing))
        except (ValueError, httpx.HTTPError):
            # Incluye el servicio fallido: puede haberse actualizado antes del error.
            failures = []
            for saved_name, saved in reversed([*completed, (name, existing)]):
                try:
                    self.restore(saved_name, saved)
                except (ValueError, httpx.HTTPError):
                    failures.append(saved_name)
            detail = (
                "restauración incompleta: " + ",".join(failures)
                if failures
                else "restauración comprobada"
            )
            raise ValueError("aplicación Swarm fallida; " + detail) from None

    def require_healthy_dependency(self, name: str) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,60}", name):
            raise ValueError("dependencia inválida")
        full_name = f"{self.project}_{name}"
        services = self.request(
            "GET", "/services", params={"filters": json.dumps({"name": [full_name]})}
        )
        exact = [item for item in services if item["Spec"]["Name"] == full_name]
        if len(exact) != 1:
            raise ValueError("dependencia Swarm ausente: " + name)
        tasks = self.request(
            "GET",
            "/tasks",
            params={
                "filters": json.dumps({"service": [exact[0]["ID"]], "desired-state": ["running"]})
            },
        )
        running = [item for item in tasks if item["Status"]["State"] == "running"]
        if len(running) != 1:
            raise ValueError("dependencia Swarm sin tarea única: " + name)
        cid = running[0]["Status"].get("ContainerStatus", {}).get("ContainerID")
        if (
            not cid
            or self.request("GET", f"/containers/{cid}/json")["State"]
            .get("Health", {})
            .get("Status")
            != "healthy"
        ):
            raise ValueError("dependencia Swarm sin health verificado: " + name)

    def _apply_one(
        self, name: str, spec: dict[str, Any], existing: dict[str, Any] | None, auth: dict[str, Any]
    ) -> None:
        host = spec["TaskTemplate"]["ContainerSpec"]["Image"].split("/", 1)[0]
        headers = {"X-Registry-Auth": registry_auth(auth[host], host)} if host in auth else {}
        if existing:
            self.request(
                "POST",
                f"/services/{existing['ID']}/update",
                params={"version": existing["Version"]["Index"]},
                json=spec,
                headers=headers,
            )
        else:
            self.request("POST", "/services/create", json=spec, headers=headers)
        expected = spec["TaskTemplate"]["ContainerSpec"]["Labels"]["io.hermes.fleet.spec"]
        stable_since = None
        for _ in range(90):
            if any(
                item["service"] == name
                and item["health"] == "healthy"
                and item.get("spec_revision") == expected
                and item.get("update_state") == "completed"
                for item in self.status()
            ):
                stable_since = stable_since or time.monotonic()
                if time.monotonic() - stable_since >= 30:
                    break
            else:
                stable_since = None
            time.sleep(2)
        else:
            raise ValueError("worker Swarm sin health de la revisión solicitada")

    def restore(self, name: str, saved: dict[str, Any] | None) -> None:
        if saved is None:
            self.rollback([name])
        elif current := self.owned(name):
            self.request(
                "POST",
                f"/services/{current['ID']}/update",
                params={"version": current["Version"]["Index"]},
                json=saved["Spec"],
            )
        else:
            raise ValueError("servicio desaparecido durante restauración")
        expected = (
            saved["Spec"]["TaskTemplate"]["ContainerSpec"]
            .get("Labels", {})
            .get("io.hermes.fleet.spec")
            if saved
            else None
        )
        was_stopped = saved is None or saved["Spec"]["Mode"]["Replicated"]["Replicas"] == 0
        stable_since = None
        for _ in range(90):
            if was_stopped:
                current = self.owned(name)
                tasks = (
                    self.request(
                        "GET",
                        "/tasks",
                        params={"filters": json.dumps({"service": [current["ID"]]})},
                    )
                    if current
                    else []
                )
                if not any(task["Status"]["State"] not in TERMINAL_TASK_STATES for task in tasks):
                    return
            elif any(
                item["service"] == name
                and item["health"] == "healthy"
                and item.get("spec_revision") == expected
                and item.get("update_state") == "completed"
                for item in self.status()
            ):
                stable_since = stable_since or time.monotonic()
                if time.monotonic() - stable_since >= 30:
                    return
            else:
                stable_since = None
            time.sleep(2)
        raise ValueError("restauración sin convergencia comprobada")

    def rollback(self, names: list[str]) -> None:
        for name in names:
            if existing := self.owned(name):
                # Provisioner usa rollback para desactivar identidad; conserva servicio y datos.
                spec = copy.deepcopy(existing["Spec"])
                spec["Mode"] = {"Replicated": {"Replicas": 0}}
                self.request(
                    "POST",
                    f"/services/{existing['ID']}/update",
                    params={"version": existing["Version"]["Index"]},
                    json=spec,
                )
                for _ in range(90):
                    tasks = self.request(
                        "GET",
                        "/tasks",
                        params={"filters": json.dumps({"service": [existing["ID"]]})},
                    )
                    if not any(
                        task["Status"]["State"] not in TERMINAL_TASK_STATES for task in tasks
                    ):
                        break
                    time.sleep(2)
                else:
                    raise ValueError("parada Swarm sin convergencia comprobada")

from __future__ import annotations

import hashlib
import hmac
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field


class ReconcileCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["plan", "apply", "rollback"]
    services: list[str] = Field(default_factory=list)


TOKEN = os.environ["FLEET_RECONCILER_TOKEN"]
PROJECT = os.environ["FLEET_PROJECT_NAME"]
COMPOSE_PATH = Path(os.environ["FLEET_COMPOSE_PATH"])
MANAGED_COMPOSE_RAW = os.environ.get("FLEET_MANAGED_COMPOSE_PATH", "")
ENV_PATH = Path(os.environ["FLEET_ENV_PATH"])
ALLOWED_IMAGES = set(json.loads(os.environ["FLEET_ALLOWED_IMAGES"]))
ALLOWED_MOUNT_ROOTS = [
    os.path.normpath(path) for path in json.loads(os.environ["FLEET_ALLOWED_MOUNT_ROOTS"])
]
BASE_COMMAND = [
    "docker",
    "compose",
    "--project-name",
    PROJECT,
    "--env-file",
    str(ENV_PATH),
    "--file",
    str(COMPOSE_PATH),
]
if MANAGED_COMPOSE_RAW:
    BASE_COMMAND.extend(["--file", MANAGED_COMPOSE_RAW])

app = FastAPI(title="Hermes Fleet Reconciler", docs_url=None, redoc_url=None)


def authorize(x_reconciler_token: str = Header(alias="X-Reconciler-Token")) -> None:
    if not hmac.compare_digest(x_reconciler_token, TOKEN):
        raise HTTPException(status_code=403, detail="forbidden")


def run_command(arguments: list[str], *, timeout: int = 120) -> str:
    try:
        result = subprocess.run(
            [*BASE_COMMAND, *arguments],
            cwd=COMPOSE_PATH.parent,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=True,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise HTTPException(status_code=503, detail="fixed compose command failed") from error
    return result.stdout


def inside_allowed_root(path: str) -> bool:
    normalized = os.path.normpath(path)
    return any(os.path.commonpath((normalized, root)) == root for root in ALLOWED_MOUNT_ROOTS)


def rendered_compose() -> tuple[dict[str, Any], str]:
    raw = run_command(["config", "--format", "json"])
    try:
        rendered = json.loads(raw)
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=503, detail="invalid rendered compose") from error
    if rendered.get("name") != PROJECT:
        raise HTTPException(status_code=422, detail="project not allowed")
    services = rendered.get("services")
    if not isinstance(services, dict):
        raise HTTPException(status_code=422, detail="services missing")
    for name, service in services.items():
        image = service.get("image")
        if image not in ALLOWED_IMAGES:
            raise HTTPException(status_code=422, detail=f"image not allowed: {name}")
        if service.get("privileged"):
            raise HTTPException(status_code=422, detail=f"privileged denied: {name}")
        if service.get("network_mode") == "host":
            raise HTTPException(status_code=422, detail=f"host network denied: {name}")
        if service.get("pid") == "host":
            raise HTTPException(status_code=422, detail=f"host pid denied: {name}")
        for mount in service.get("volumes", []):
            if not isinstance(mount, dict):
                raise HTTPException(status_code=422, detail=f"unparsed mount denied: {name}")
            source = str(mount.get("source", ""))
            target = str(mount.get("target", ""))
            is_socket = "docker.sock" in source.lower() or "docker.sock" in target.lower()
            if is_socket and name != "fleet-reconciler":
                raise HTTPException(status_code=422, detail=f"docker socket denied: {name}")
            if mount.get("type") == "bind" and not is_socket and not inside_allowed_root(source):
                raise HTTPException(status_code=422, detail=f"mount root denied: {name}")
    canonical = json.dumps(rendered, sort_keys=True, separators=(",", ":"))
    return rendered, f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"


def parse_ps(raw: str) -> list[dict[str, Any]]:
    if not raw.strip():
        return []
    try:
        decoded = json.loads(raw)
        values = decoded if isinstance(decoded, list) else [decoded]
    except json.JSONDecodeError:
        values = [json.loads(line) for line in raw.splitlines() if line.strip()]
    return [
        {
            "name": item.get("Name"),
            "service": item.get("Service"),
            "image": item.get("Image"),
            "state": item.get("State"),
            "health": item.get("Health", ""),
        }
        for item in values
    ]


def snapshot(action: str, commands: list[str]) -> dict[str, Any]:
    rendered, digest = rendered_compose()
    services = parse_ps(run_command(["ps", "--format", "json"]))
    return {
        "action": action,
        "project_name": PROJECT,
        "compose_digest": digest,
        "services": services,
        "configured_services": sorted(rendered["services"]),
        "commands": commands,
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/internal/status", dependencies=[Depends(authorize)])
def status() -> dict[str, Any]:
    return snapshot("status", ["config", "ps"])


@app.post("/v1/internal/reconcile", dependencies=[Depends(authorize)])
def reconcile(command: ReconcileCommand) -> dict[str, Any]:
    rendered, _ = rendered_compose()
    if command.action == "plan":
        if command.services:
            raise HTTPException(status_code=422, detail="plan does not accept services")
        return snapshot("plan", ["config", "ps"])
    configured = set(rendered["services"])
    if (
        not command.services
        or len(command.services) != len(set(command.services))
        or any(not service.startswith("worker-") for service in command.services)
        or any(service not in configured for service in command.services)
    ):
        raise HTTPException(status_code=422, detail="worker service not allowed")
    if command.action == "rollback":
        run_command(["stop", *command.services], timeout=300)
        run_command(["rm", "--force", "--stop", *command.services], timeout=300)
        return snapshot(
            "rollback",
            ["config", "stop <workers>", "rm --force --stop <workers>"],
        )
    run_command(
        [
            "pull",
            "--policy",
            "missing",
            "--ignore-pull-failures",
            "--quiet",
            *command.services,
        ],
        timeout=300,
    )
    run_command(
        ["up", "-d", "--no-build", "--no-deps", *command.services],
        timeout=300,
    )
    return snapshot(
        "apply",
        [
            "config",
            "pull --policy missing <workers>",
            "up -d --no-build --no-deps <workers>",
        ],
    )

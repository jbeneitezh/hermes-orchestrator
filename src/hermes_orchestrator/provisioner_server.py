from __future__ import annotations

import hmac
import os
from pathlib import Path
from typing import Annotated, NoReturn

from fastapi import Depends, FastAPI, Header, HTTPException

from hermes_orchestrator.config import Settings
from hermes_orchestrator.fleet_runner import HttpFleetRunnerClient
from hermes_orchestrator.provisioning import (
    ManagedAgentRenderer,
    ProvisionerResult,
    ProvisioningError,
    ProvisioningPayload,
)

TOKEN = os.environ["AGENT_PROVISIONER_TOKEN"]
renderer = ManagedAgentRenderer(
    managed_root=Path(os.environ["AGENT_PROVISIONER_MANAGED_ROOT"]),
    data_root=Path(os.environ["AGENT_PROVISIONER_DATA_ROOT"]),
    host_data_root=os.environ["AGENT_PROVISIONER_HOST_DATA_ROOT"],
    dataset_root=os.environ["AGENT_PROVISIONER_DATASET_ROOT"],
    worker_image=os.environ["AGENT_PROVISIONER_WORKER_IMAGE"],
    project_name=os.environ["AGENT_PROVISIONER_PROJECT_NAME"],
)
renderer.ensure_document()
fleet = HttpFleetRunnerClient(
    Settings(
        fleet_runner_url=os.environ["AGENT_PROVISIONER_FLEET_URL"],
        fleet_runner_token=os.environ["AGENT_PROVISIONER_FLEET_TOKEN"],
    )
)

app = FastAPI(title="Hermes Agent Provisioner", docs_url=None, redoc_url=None)


def authorize(
    x_provisioner_token: Annotated[str, Header(alias="X-Provisioner-Token")],
) -> None:
    if not hmac.compare_digest(x_provisioner_token, TOKEN):
        raise HTTPException(status_code=403, detail="forbidden")


def raise_http(error: ProvisioningError) -> NoReturn:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "detail": error.detail},
    ) from error


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/v1/internal/agents/apply",
    response_model=ProvisionerResult,
    dependencies=[Depends(authorize)],
)
def apply(payload: ProvisioningPayload) -> ProvisionerResult:
    try:
        return renderer.apply(payload, fleet)
    except ProvisioningError as error:
        raise_http(error)


@app.post(
    "/v1/internal/agents/rollback",
    response_model=ProvisionerResult,
    dependencies=[Depends(authorize)],
)
def rollback(payload: ProvisioningPayload) -> ProvisionerResult:
    try:
        return renderer.rollback(payload, fleet)
    except ProvisioningError as error:
        raise_http(error)

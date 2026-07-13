from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Annotated, NoReturn

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import get_session
from hermes_orchestrator.policy import Permission, actor_is_allowed
from hermes_orchestrator.provisioning import AgentProvisioner, ProvisioningError
from hermes_orchestrator.provisioning_services import (
    AgentProvisioningResult,
    provision_agent_request,
    rollback_agent_request,
)
from hermes_orchestrator.schemas import (
    AgentProvisionCommand,
    AgentProvisionResponse,
    ErrorEnvelope,
)
from hermes_orchestrator.services import AgentRequestLifecycleError, IdempotencyConflictError

SessionDependency = Annotated[Session, Depends(get_session)]
IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=160)]


def _raise_http(error: Exception) -> NoReturn:
    if isinstance(error, ProvisioningError | AgentRequestLifecycleError):
        code, detail, status_code = error.code, error.detail, error.status_code
    else:
        code, detail, status_code = (
            "idempotency_conflict",
            "La clave ya se usó con otra operación",
            409,
        )
    raise HTTPException(
        status_code=status_code,
        detail={"code": code, "detail": detail},
    ) from error


def _response(result: AgentProvisioningResult) -> AgentProvisionResponse:
    return AgentProvisionResponse(
        request_id=result.request_id,
        status=result.provisioner.status,
        service_name=result.provisioner.service_name,
        config_digest=result.provisioner.config_digest,
        health=result.provisioner.health,
        replayed=result.replayed,
    )


def build_provisioning_router(settings: Settings, provisioner: AgentProvisioner) -> APIRouter:
    router = APIRouter(prefix="/v1/agents/requests", tags=["provisioning"])

    def require(permission: Permission) -> Callable[[str], str]:
        def dependency(x_actor_id: Annotated[str, Header(alias="X-Actor-Id")]) -> str:
            if not actor_is_allowed(x_actor_id, permission, settings.actor_roles):
                raise HTTPException(
                    status_code=403,
                    detail={"code": "permission_denied", "detail": "Permiso denegado"},
                )
            return x_actor_id

        return dependency

    @router.post(
        "/{request_id}/provision",
        response_model=AgentProvisionResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses={
            403: {"model": ErrorEnvelope},
            409: {"model": ErrorEnvelope},
            422: {"model": ErrorEnvelope},
            503: {"model": ErrorEnvelope},
        },
    )
    def provision(
        request_id: uuid.UUID,
        _: AgentProvisionCommand,
        response: Response,
        session: SessionDependency,
        idempotency_key: IdempotencyKey,
        actor_id: str = Depends(require(Permission.AGENTS_PROVISION)),
    ) -> AgentProvisionResponse:
        try:
            result = provision_agent_request(
                session,
                provisioner=provisioner,
                request_id=request_id,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
            )
        except (ProvisioningError, AgentRequestLifecycleError, IdempotencyConflictError) as error:
            _raise_http(error)
        if result.replayed:
            response.headers["Idempotent-Replayed"] = "true"
        return _response(result)

    @router.post(
        "/{request_id}/rollback",
        response_model=AgentProvisionResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses={
            403: {"model": ErrorEnvelope},
            404: {"model": ErrorEnvelope},
            409: {"model": ErrorEnvelope},
            503: {"model": ErrorEnvelope},
        },
    )
    def rollback(
        request_id: uuid.UUID,
        command: AgentProvisionCommand,
        response: Response,
        session: SessionDependency,
        idempotency_key: IdempotencyKey,
        actor_id: str = Depends(require(Permission.AGENTS_PROVISION)),
    ) -> AgentProvisionResponse:
        try:
            result = rollback_agent_request(
                session,
                provisioner=provisioner,
                request_id=request_id,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                reason=command.reason,
            )
        except (ProvisioningError, AgentRequestLifecycleError, IdempotencyConflictError) as error:
            _raise_http(error)
        if result.replayed:
            response.headers["Idempotent-Replayed"] = "true"
        return _response(result)

    return router

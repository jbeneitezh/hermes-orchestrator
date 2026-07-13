import uuid
from collections.abc import Callable
from typing import Annotated, Any, NoReturn

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import get_session
from hermes_orchestrator.environment_services import (
    ENVIRONMENT_DEFINITIONS,
    EnvironmentError,
    deploy_environment,
    expire_local,
    list_deployments,
    promote_environment,
    rollback_environment,
    to_environment_response,
)
from hermes_orchestrator.policy import Permission, actor_is_allowed
from hermes_orchestrator.schemas import (
    EnvironmentDeployCreate,
    EnvironmentDeploymentResponse,
    EnvironmentInventoryResponse,
    EnvironmentPromotionCreate,
    EnvironmentRollbackCreate,
    ErrorEnvelope,
)

SessionDependency = Annotated[Session, Depends(get_session)]
IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=160)]


def _raise_http(error: EnvironmentError) -> NoReturn:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "detail": error.detail},
    ) from error


def build_environment_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/v1/environments", tags=["entornos"])

    def require(permission: Permission) -> Callable[[str], str]:
        def dependency(x_actor_id: Annotated[str, Header(alias="X-Actor-Id")]) -> str:
            if not actor_is_allowed(x_actor_id, permission, settings.actor_roles):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={"code": "permission_denied", "detail": "Permiso denegado"},
                )
            return x_actor_id

        return dependency

    errors: dict[int | str, dict[str, Any]] = {
        403: {"model": ErrorEnvelope},
        404: {"model": ErrorEnvelope},
        409: {"model": ErrorEnvelope},
        422: {"model": ErrorEnvelope},
    }

    @router.get("", response_model=EnvironmentInventoryResponse, responses=errors)
    def inventory(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.ENVIRONMENTS_READ))],
    ) -> EnvironmentInventoryResponse:
        return EnvironmentInventoryResponse(
            definitions=ENVIRONMENT_DEFINITIONS,
            deployments=[to_environment_response(item) for item in list_deployments(session)],
        )

    @router.post(
        "/deployments",
        response_model=EnvironmentDeploymentResponse,
        status_code=status.HTTP_201_CREATED,
        responses=errors,
    )
    def deploy(
        body: EnvironmentDeployCreate,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.ENVIRONMENTS_DEPLOY))],
        idempotency_key: IdempotencyKey,
    ) -> EnvironmentDeploymentResponse:
        try:
            return to_environment_response(
                deploy_environment(
                    session,
                    body=body,
                    actor_id=actor_id,
                    idempotency_key=idempotency_key,
                    settings=settings,
                )
            )
        except EnvironmentError as error:
            _raise_http(error)

    @router.post(
        "/deployments/{deployment_id}/expire",
        response_model=EnvironmentDeploymentResponse,
        responses=errors,
    )
    def expire(
        deployment_id: uuid.UUID,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.ENVIRONMENTS_DEPLOY))],
        idempotency_key: IdempotencyKey,
    ) -> EnvironmentDeploymentResponse:
        try:
            return to_environment_response(
                expire_local(
                    session,
                    deployment_id=deployment_id,
                    actor_id=actor_id,
                    idempotency_key=idempotency_key,
                )
            )
        except EnvironmentError as error:
            _raise_http(error)

    @router.post(
        "/promotions",
        response_model=EnvironmentDeploymentResponse,
        status_code=status.HTTP_201_CREATED,
        responses=errors,
    )
    def promote(
        body: EnvironmentPromotionCreate,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.ENVIRONMENTS_PROMOTE))],
        idempotency_key: IdempotencyKey,
    ) -> EnvironmentDeploymentResponse:
        try:
            return to_environment_response(
                promote_environment(
                    session,
                    body=body,
                    actor_id=actor_id,
                    idempotency_key=idempotency_key,
                    settings=settings,
                )
            )
        except EnvironmentError as error:
            _raise_http(error)

    @router.post(
        "/{environment}/rollback",
        response_model=EnvironmentDeploymentResponse,
        status_code=status.HTTP_201_CREATED,
        responses=errors,
    )
    def rollback(
        environment: str,
        body: EnvironmentRollbackCreate,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.ENVIRONMENTS_ROLLBACK))],
        idempotency_key: IdempotencyKey,
    ) -> EnvironmentDeploymentResponse:
        try:
            return to_environment_response(
                rollback_environment(
                    session,
                    environment=environment,
                    body=body,
                    actor_id=actor_id,
                    idempotency_key=idempotency_key,
                    settings=settings,
                )
            )
        except EnvironmentError as error:
            _raise_http(error)

    return router

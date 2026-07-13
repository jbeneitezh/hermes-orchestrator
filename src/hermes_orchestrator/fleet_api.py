from collections.abc import Callable
from typing import Annotated, NoReturn

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import get_session
from hermes_orchestrator.fleet_runner import FleetRunner
from hermes_orchestrator.fleet_services import (
    FleetReconcileError,
    request_fleet_reconcile,
    to_fleet_response,
)
from hermes_orchestrator.policy import Permission, actor_is_allowed
from hermes_orchestrator.repositories import FleetReconcileRepository
from hermes_orchestrator.schemas import (
    ErrorEnvelope,
    FleetReconcileCreate,
    FleetReconcileResponse,
    FleetStatusResponse,
)

SessionDependency = Annotated[Session, Depends(get_session)]


def _raise_http(error: FleetReconcileError) -> NoReturn:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "detail": error.detail},
    ) from error


def build_fleet_router(settings: Settings, runner: FleetRunner) -> APIRouter:
    router = APIRouter(prefix="/v1/fleet", tags=["flota"])

    def require(permission: Permission) -> Callable[[str], str]:
        def dependency(x_actor_id: Annotated[str, Header(alias="X-Actor-Id")]) -> str:
            if not actor_is_allowed(x_actor_id, permission, settings.actor_roles):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={"code": "permission_denied", "detail": "Permiso denegado"},
                )
            return x_actor_id

        return dependency

    @router.get(
        "/status",
        response_model=FleetStatusResponse,
        responses={403: {"model": ErrorEnvelope}, 503: {"model": ErrorEnvelope}},
    )
    def fleet_status(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.FLEET_READ))],
    ) -> FleetStatusResponse:
        try:
            observed = runner.status()
        except Exception as error:
            _raise_http(
                FleetReconcileError(
                    "fleet_runner_unavailable", "Fleet reconciler no disponible", 503
                )
            )
            raise AssertionError("unreachable") from error
        latest = FleetReconcileRepository(session).latest(
            settings.fleet_project_name, settings.fleet_compose_path
        )
        return FleetStatusResponse(
            project_name=settings.fleet_project_name,
            compose_path=settings.fleet_compose_path,
            compose_digest=str(observed["compose_digest"]),
            services=list(observed["services"]),
            last_reconcile=to_fleet_response(latest) if latest is not None else None,
        )

    @router.post(
        "/reconcile-requests",
        response_model=FleetReconcileResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses={
            403: {"model": ErrorEnvelope},
            409: {"model": ErrorEnvelope},
            422: {"model": ErrorEnvelope},
            503: {"model": ErrorEnvelope},
        },
    )
    def reconcile_request(
        body: FleetReconcileCreate,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.FLEET_RECONCILE_REQUEST))],
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=8, max_length=160)
        ],
    ) -> FleetReconcileResponse:
        try:
            result = request_fleet_reconcile(
                session,
                settings=settings,
                runner=runner,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                body=body,
            )
        except FleetReconcileError as error:
            _raise_http(error)
        return to_fleet_response(result.record, replayed=result.replayed)

    return router

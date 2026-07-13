import uuid
from collections.abc import Callable
from typing import Annotated, Literal, NoReturn

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import get_session
from hermes_orchestrator.policy import Permission, actor_is_allowed
from hermes_orchestrator.schemas import (
    CircuitResetCommand,
    CircuitResetResponse,
    ErrorEnvelope,
    UsageControlStatusResponse,
    UsageDetailResponse,
    UsageSummaryResponse,
)
from hermes_orchestrator.usage_services import (
    ControlViolation,
    control_status,
    get_usage_detail,
    reset_circuit,
    summarize_usage,
)

SessionDependency = Annotated[Session, Depends(get_session)]


def raise_http(error: ControlViolation) -> NoReturn:
    detail: dict[str, object] = {"code": error.code, "detail": error.detail}
    headers = None
    if error.retry_after is not None:
        detail["retry_after"] = error.retry_after
        headers = {"Retry-After": str(error.retry_after)}
    raise HTTPException(
        status_code=error.status_code,
        detail=detail,
        headers=headers,
    ) from error


def build_usage_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/v1/usage", tags=["consumo y límites"])

    def require(permission: Permission) -> Callable[[str], str]:
        def dependency(x_actor_id: Annotated[str, Header(alias="X-Actor-Id")]) -> str:
            if not actor_is_allowed(x_actor_id, permission, settings.actor_roles):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={"code": "permission_denied", "detail": "Permiso denegado"},
                )
            return x_actor_id

        return dependency

    @router.get("/summary", response_model=UsageSummaryResponse)
    def usage_summary_route(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.USAGE_READ))],
        group_by: Annotated[Literal["operation", "agent", "profile", "day"], Query()] = "operation",
        operation_id: uuid.UUID | None = None,
    ) -> UsageSummaryResponse:
        return UsageSummaryResponse.model_validate(
            summarize_usage(session, group_by=group_by, operation_id=operation_id)
        )

    @router.get(
        "/runs/{run_id}",
        response_model=UsageDetailResponse,
        responses={404: {"model": ErrorEnvelope}},
    )
    def usage_detail_route(
        run_id: uuid.UUID,
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.USAGE_READ))],
    ) -> UsageDetailResponse:
        try:
            entry = get_usage_detail(session, run_id)
        except ControlViolation as error:
            raise_http(error)
        return UsageDetailResponse.model_validate(entry)

    @router.get("/control-status", response_model=UsageControlStatusResponse)
    def usage_control_status_route(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.USAGE_READ))],
    ) -> UsageControlStatusResponse:
        return UsageControlStatusResponse.model_validate(control_status(session, settings))

    @router.post(
        "/circuits/{circuit_id}/reset",
        response_model=CircuitResetResponse,
        responses={404: {"model": ErrorEnvelope}},
    )
    def reset_circuit_route(
        circuit_id: uuid.UUID,
        body: CircuitResetCommand,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.USAGE_CONTROL_RESET))],
    ) -> CircuitResetResponse:
        try:
            circuit = reset_circuit(
                session,
                circuit_id,
                actor_id=actor_id,
                reason=body.reason,
            )
        except ControlViolation as error:
            raise_http(error)
        return CircuitResetResponse.model_validate(circuit)

    return router

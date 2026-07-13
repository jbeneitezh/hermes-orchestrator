import uuid
from collections.abc import Callable
from typing import Annotated, Literal, NoReturn

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import get_session
from hermes_orchestrator.fleet_runner import FleetRunner
from hermes_orchestrator.operations_services import (
    OperationsReadError,
    approvals_view,
    fleet_view,
    quota_view,
    task_view,
    timeline_view,
    usage_view,
)
from hermes_orchestrator.operations_ui import OPERATIONS_DASHBOARD_HTML
from hermes_orchestrator.policy import Permission, actor_is_allowed

SessionDependency = Annotated[Session, Depends(get_session)]


def _raise_http(error: OperationsReadError) -> NoReturn:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "detail": error.detail},
    ) from error


def build_operations_router(settings: Settings, runner: FleetRunner) -> APIRouter:
    router = APIRouter(tags=["operaciones"])

    def require(permission: Permission) -> Callable[[str], str]:
        def dependency(x_actor_id: Annotated[str, Header(alias="X-Actor-Id")]) -> str:
            if not actor_is_allowed(x_actor_id, permission, settings.actor_roles):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={"code": "permission_denied", "detail": "Permiso denegado"},
                )
            return x_actor_id

        return dependency

    @router.get("/operations", response_class=HTMLResponse, include_in_schema=True)
    def dashboard() -> str:
        return OPERATIONS_DASHBOARD_HTML

    @router.get("/v1/operations/fleet")
    def operations_fleet(
        _: Annotated[str, Depends(require(Permission.OPERATIONS_READ))],
    ) -> dict[str, object]:
        try:
            return fleet_view(runner)
        except OperationsReadError as error:
            _raise_http(error)

    @router.get("/v1/operations/tasks")
    def operations_tasks(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.OPERATIONS_READ))],
        task_status: Annotated[str | None, Query(alias="status")] = None,
        assignee: str | None = None,
        operation_id: uuid.UUID | None = None,
        active_only: bool = False,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
    ) -> dict[str, object]:
        return task_view(
            session,
            status=task_status,
            assignee=assignee,
            operation_id=operation_id,
            active_only=active_only,
            stale_after_seconds=settings.operations_stale_after_seconds,
            limit=limit,
        )

    @router.get("/v1/operations/timeline")
    def operations_timeline(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.OPERATIONS_READ))],
        operation_id: uuid.UUID | None = None,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
    ) -> dict[str, object]:
        try:
            return timeline_view(
                session,
                operation_id=operation_id,
                cursor=cursor,
                limit=limit,
            )
        except OperationsReadError as error:
            _raise_http(error)

    @router.get("/v1/operations/usage")
    def operations_usage(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.OPERATIONS_READ))],
        group_by: Annotated[Literal["operation", "agent", "profile", "day"], Query()] = "operation",
        operation_id: uuid.UUID | None = None,
    ) -> dict[str, object]:
        return usage_view(session, group_by=group_by, operation_id=operation_id)

    @router.get("/v1/operations/approvals")
    def operations_approvals(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.OPERATIONS_READ))],
        approval_status: Annotated[str | None, Query(alias="status")] = None,
        operation_id: uuid.UUID | None = None,
    ) -> dict[str, object]:
        return approvals_view(
            session,
            status=approval_status,
            operation_id=operation_id,
        )

    @router.get("/v1/operations/quota")
    def operations_quota(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.OPERATIONS_READ))],
    ) -> dict[str, object]:
        return quota_view(session, settings)

    return router

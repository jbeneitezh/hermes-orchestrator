import uuid
from collections.abc import Callable
from typing import Annotated, NoReturn, cast

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import get_session
from hermes_orchestrator.models import Approval, Run, Task, TaskComment
from hermes_orchestrator.policy import Permission, actor_is_allowed
from hermes_orchestrator.schemas import (
    ApprovalDecision,
    ApprovalResponse,
    CancelCommand,
    DispatchCommand,
    DispatchResponse,
    ErrorEnvelope,
    RunResponse,
    TaskCommentCreate,
    TaskCommentResponse,
    TaskCreate,
    TaskResponse,
)
from hermes_orchestrator.task_services import (
    LifecycleError,
    add_comment,
    cancel_task,
    create_task,
    dispatch_task,
    get_run,
    get_task,
    resolve_approval,
)

SessionDependency = Annotated[Session, Depends(get_session)]
IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=160)]


def raise_http(error: LifecycleError) -> NoReturn:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "detail": error.detail},
    ) from error


def to_task_response(task: Task, *, replayed: bool = False) -> TaskResponse:
    response = TaskResponse.model_validate(task)
    return response.model_copy(
        update={
            "dependency_ids": [link.depends_on_task_id for link in task.dependency_links],
            "replayed": replayed,
        }
    )


def build_task_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/v1", tags=["tareas"])

    def require(permission: Permission) -> Callable[[str], str]:
        def dependency(x_actor_id: Annotated[str, Header(alias="X-Actor-Id")]) -> str:
            if not actor_is_allowed(x_actor_id, permission, settings.actor_roles):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={"code": "permission_denied", "detail": "Permiso denegado"},
                )
            return x_actor_id

        return dependency

    @router.post(
        "/tasks",
        response_model=TaskResponse,
        status_code=status.HTTP_201_CREATED,
        responses={409: {"model": ErrorEnvelope}},
    )
    def create_task_route(
        body: TaskCreate,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.TASKS_CREATE))],
        idempotency_key: IdempotencyKey,
    ) -> TaskResponse:
        try:
            result = create_task(
                session,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                payload=body.model_dump(mode="python"),
            )
        except LifecycleError as error:
            raise_http(error)
        return to_task_response(cast(Task, result.value), replayed=result.replayed)

    @router.get(
        "/tasks/{task_id}",
        response_model=TaskResponse,
        responses={404: {"model": ErrorEnvelope}},
    )
    def get_task_route(
        task_id: uuid.UUID,
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.TASKS_READ))],
    ) -> TaskResponse:
        try:
            task = get_task(session, task_id)
        except LifecycleError as error:
            raise_http(error)
        return to_task_response(task)

    @router.post(
        "/tasks/{task_id}/dispatch",
        response_model=DispatchResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses={409: {"model": ErrorEnvelope}},
    )
    def dispatch_task_route(
        task_id: uuid.UUID,
        body: DispatchCommand,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.TASKS_DISPATCH))],
        idempotency_key: IdempotencyKey,
    ) -> DispatchResponse:
        try:
            result = dispatch_task(
                session,
                task_id=task_id,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                payload=body.model_dump(mode="python"),
            )
        except LifecycleError as error:
            raise_http(error)
        return DispatchResponse(
            run=RunResponse.model_validate(cast(Run, result.value)), replayed=result.replayed
        )

    @router.post(
        "/tasks/{task_id}/comments",
        response_model=TaskCommentResponse,
        status_code=status.HTTP_201_CREATED,
        responses={409: {"model": ErrorEnvelope}},
    )
    def comment_task_route(
        task_id: uuid.UUID,
        body: TaskCommentCreate,
        response: Response,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.TASKS_COMMENT))],
        idempotency_key: IdempotencyKey,
    ) -> TaskCommentResponse:
        try:
            result = add_comment(
                session,
                task_id=task_id,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                body=body.body,
            )
        except LifecycleError as error:
            raise_http(error)
        if result.replayed:
            response.headers["Idempotent-Replayed"] = "true"
        return TaskCommentResponse.model_validate(cast(TaskComment, result.value))

    @router.post(
        "/tasks/{task_id}/cancel",
        response_model=TaskResponse,
        responses={409: {"model": ErrorEnvelope}},
    )
    def cancel_task_route(
        task_id: uuid.UUID,
        body: CancelCommand,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.TASKS_CANCEL))],
        idempotency_key: IdempotencyKey,
    ) -> TaskResponse:
        try:
            result = cancel_task(
                session,
                task_id=task_id,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                reason=body.reason,
            )
        except LifecycleError as error:
            raise_http(error)
        return to_task_response(cast(Task, result.value), replayed=result.replayed)

    @router.get(
        "/runs/{run_id}",
        response_model=RunResponse,
        responses={404: {"model": ErrorEnvelope}},
    )
    def get_run_route(
        run_id: uuid.UUID,
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.RUNS_READ))],
    ) -> RunResponse:
        try:
            run = get_run(session, run_id)
        except LifecycleError as error:
            raise_http(error)
        return RunResponse.model_validate(run)

    @router.post(
        "/runs/{run_id}/approval",
        response_model=ApprovalResponse,
        responses={409: {"model": ErrorEnvelope}},
    )
    def resolve_approval_route(
        run_id: uuid.UUID,
        body: ApprovalDecision,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.APPROVALS_DECIDE))],
        idempotency_key: IdempotencyKey,
    ) -> ApprovalResponse:
        try:
            result = resolve_approval(
                session,
                run_id=run_id,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                decision=body.decision,
                reason=body.reason,
            )
        except LifecycleError as error:
            raise_http(error)
        return ApprovalResponse.model_validate(cast(Approval, result.value))

    return router

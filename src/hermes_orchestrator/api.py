import uuid
from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.orm import Session

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import get_session
from hermes_orchestrator.policy import Permission, actor_is_allowed
from hermes_orchestrator.repositories import AgentRepository, ExecutionProfileRepository
from hermes_orchestrator.schemas import (
    AgentRequestCreate,
    AgentRequestResponse,
    AgentResponse,
    ErrorEnvelope,
    ExecutionProfileResponse,
)
from hermes_orchestrator.services import IdempotencyConflictError, request_agent

SessionDependency = Annotated[Session, Depends(get_session)]


def build_catalog_router(settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/v1", tags=["catálogo"])

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
        "/agents",
        response_model=list[AgentResponse],
        responses={status.HTTP_403_FORBIDDEN: {"model": ErrorEnvelope}},
    )
    def list_agents(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.AGENTS_READ))],
    ) -> list[AgentResponse]:
        return [AgentResponse.model_validate(agent) for agent in AgentRepository(session).list()]

    @router.post(
        "/agents/requests",
        response_model=AgentRequestResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses={
            status.HTTP_403_FORBIDDEN: {"model": ErrorEnvelope},
            status.HTTP_409_CONFLICT: {"model": ErrorEnvelope},
        },
    )
    def create_agent_request(
        body: AgentRequestCreate,
        response: Response,
        session: SessionDependency,
        actor_id: Annotated[str, Depends(require(Permission.AGENTS_REQUEST))],
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=8, max_length=160)
        ],
    ) -> AgentRequestResponse:
        try:
            result = request_agent(
                session,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
                payload=body.model_dump(mode="json"),
            )
        except IdempotencyConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "idempotency_conflict",
                    "detail": "La clave ya se usó con otra solicitud",
                },
            ) from exc
        if result.replayed:
            response.headers["Idempotent-Replayed"] = "true"
        return AgentRequestResponse(
            id=result.request.id,
            status=result.request.status,
            replayed=result.replayed,
            created_at=result.request.created_at,
        )

    @router.get(
        "/agents/{agent_id}",
        response_model=AgentResponse,
        responses={
            status.HTTP_403_FORBIDDEN: {"model": ErrorEnvelope},
            status.HTTP_404_NOT_FOUND: {"model": ErrorEnvelope},
        },
    )
    def get_agent(
        agent_id: uuid.UUID,
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.AGENTS_READ))],
    ) -> AgentResponse:
        agent = AgentRepository(session).get(agent_id)
        if agent is None:
            raise HTTPException(
                status_code=404,
                detail={"code": "not_found", "detail": "Agente no encontrado"},
            )
        return AgentResponse.model_validate(agent)

    @router.get(
        "/execution-profiles",
        response_model=list[ExecutionProfileResponse],
        responses={status.HTTP_403_FORBIDDEN: {"model": ErrorEnvelope}},
    )
    def list_execution_profiles(
        session: SessionDependency,
        _: Annotated[str, Depends(require(Permission.PROFILES_READ))],
    ) -> list[ExecutionProfileResponse]:
        return [
            ExecutionProfileResponse.model_validate(profile)
            for profile in ExecutionProfileRepository(session).list_enabled()
        ]

    return router

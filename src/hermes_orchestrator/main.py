from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.engine import Engine

from hermes_orchestrator import __version__
from hermes_orchestrator.api import build_catalog_router
from hermes_orchestrator.config import Settings, get_settings
from hermes_orchestrator.database import (
    create_database_engine,
    create_session_factory,
    database_is_ready,
)
from hermes_orchestrator.schemas import CapabilitiesResponse, HealthResponse
from hermes_orchestrator.task_api import build_task_router


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()
    engine = create_database_engine(resolved_settings.database_url)
    session_factory = create_session_factory(engine)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        engine.dispose()

    app = FastAPI(
        title=resolved_settings.app_name,
        version=__version__,
        lifespan=lifespan,
    )
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.include_router(build_catalog_router(resolved_settings))
    app.include_router(build_task_router(resolved_settings))

    @app.get(
        "/health",
        response_model=HealthResponse,
        responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": HealthResponse}},
        tags=["plataforma"],
    )
    def health(request: Request) -> HealthResponse | JSONResponse:
        request_engine = cast(Engine, request.app.state.engine)
        try:
            ready = database_is_ready(request_engine)
        except Exception:
            ready = False
        if not ready:
            payload = HealthResponse(status="unavailable", database="unavailable")
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=payload.model_dump(),
            )
        return HealthResponse(status="ok", database="ok")

    @app.get(
        "/v1/capabilities",
        response_model=CapabilitiesResponse,
        tags=["plataforma"],
    )
    def capabilities() -> CapabilitiesResponse:
        return CapabilitiesResponse(
            service="hermes-orchestrator",
            version=__version__,
            capabilities=[
                "health",
                "capabilities",
                "postgresql",
                "alembic",
                "agent_catalog",
                "execution_profiles",
                "deny_by_default_policy",
                "append_only_audit",
                "task_run_lifecycle",
                "independent_review",
                "approvals",
            ],
        )

    return app


app = create_app()

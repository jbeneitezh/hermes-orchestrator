from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.engine import Engine

from hermes_orchestrator import __version__
from hermes_orchestrator.config import Settings, get_settings
from hermes_orchestrator.database import create_database_engine, database_is_ready
from hermes_orchestrator.schemas import CapabilitiesResponse, HealthResponse


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()
    engine = create_database_engine(resolved_settings.database_url)

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
            capabilities=["health", "capabilities", "postgresql", "alembic"],
        )

    return app


app = create_app()

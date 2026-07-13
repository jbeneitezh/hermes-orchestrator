from __future__ import annotations

from fastapi.testclient import TestClient

from hermes_orchestrator.config import Settings
from hermes_orchestrator.database import create_database_engine, database_is_ready
from hermes_orchestrator.main import create_app


def build_client(monkeypatch, *, database_ready: bool = True) -> TestClient:
    monkeypatch.setattr(
        "hermes_orchestrator.main.database_is_ready",
        lambda _: database_ready,
    )
    settings = Settings(
        environment="test",
        database_url="postgresql+psycopg://unused:unused@localhost:1/unused",
    )
    return TestClient(create_app(settings))


def test_health_reports_api_and_database_ready(monkeypatch) -> None:
    with build_client(monkeypatch) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


def test_health_reports_database_unavailable(monkeypatch) -> None:
    with build_client(monkeypatch, database_ready=False) as client:
        response = client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "database": "unavailable"}


def test_capabilities_publish_only_bootstrap_features(monkeypatch) -> None:
    with build_client(monkeypatch) as client:
        response = client.get("/v1/capabilities")

    assert response.status_code == 200
    assert response.json() == {
        "service": "hermes-orchestrator",
        "version": "0.1.0",
        "api_version": "v1",
        "capabilities": ["health", "capabilities", "postgresql", "alembic"],
    }


def test_openapi_contains_exactly_the_two_domain_routes(monkeypatch) -> None:
    with build_client(monkeypatch) as client:
        schema = client.get("/openapi.json").json()

    domain_paths = {path for path in schema["paths"] if not path.startswith("/docs")}
    assert domain_paths == {"/health", "/v1/capabilities"}


def test_settings_accept_environment_overrides(monkeypatch) -> None:
    monkeypatch.setenv("HERMES_ORCHESTRATOR_ENVIRONMENT", "preproduction")
    monkeypatch.setenv(
        "HERMES_ORCHESTRATOR_DATABASE_URL",
        "postgresql+psycopg://user:password@database:5432/orchestrator",
    )

    settings = Settings()

    assert settings.environment == "preproduction"
    assert settings.database_url.endswith("@database:5432/orchestrator")

    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    assert database_is_ready(engine)
    engine.dispose()

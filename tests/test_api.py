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
        "capabilities": [
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
            "hermes_runs_adapter",
            "run_events",
            "mcp_streamable_http",
            "mcp_governed_tools",
            "fleet_status",
            "fleet_reconcile_request",
            "fleet_allowlisted_runner",
            "usage_ledger",
            "budget_dispatch_controls",
            "quota_status",
            "circuit_breaker",
            "governed_environments",
            "immutable_promotion",
            "local_ttl_port_allocation",
            "environment_rollback",
            "operations_dashboard",
            "reconnectable_timeline",
            "process_watchdog_no_llm",
            "bearer_internal_auth",
        ],
    }


def test_openapi_contains_the_bootstrap_and_catalog_routes(monkeypatch) -> None:
    with build_client(monkeypatch) as client:
        schema = client.get("/openapi.json").json()

    domain_paths = {path for path in schema["paths"] if not path.startswith("/docs")}
    assert domain_paths == {
        "/health",
        "/v1/capabilities",
        "/v1/agents",
        "/v1/agents/requests",
        "/v1/agents/{agent_id}",
        "/v1/execution-profiles",
        "/v1/tasks",
        "/v1/tasks/{task_id}",
        "/v1/tasks/{task_id}/dispatch",
        "/v1/tasks/{task_id}/comments",
        "/v1/tasks/{task_id}/cancel",
        "/v1/runs/{run_id}",
        "/v1/runs/{run_id}/events",
        "/v1/runs/{run_id}/approval",
        "/v1/fleet/status",
        "/v1/fleet/reconcile-requests",
        "/v1/usage/summary",
        "/v1/usage/runs/{run_id}",
        "/v1/usage/control-status",
        "/v1/usage/circuits/{circuit_id}/reset",
        "/v1/environments",
        "/v1/environments/deployments",
        "/v1/environments/deployments/{deployment_id}/expire",
        "/v1/environments/promotions",
        "/v1/environments/{environment}/rollback",
        "/operations",
        "/v1/operations/fleet",
        "/v1/operations/tasks",
        "/v1/operations/timeline",
        "/v1/operations/usage",
        "/v1/operations/approvals",
        "/v1/operations/quota",
    }


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

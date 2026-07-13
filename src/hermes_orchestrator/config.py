from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuración del proceso obtenida de entorno o `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="HERMES_ORCHESTRATOR_",
        extra="ignore",
    )

    app_name: str = "Hermes Orchestrator"
    environment: Literal["development", "test", "preproduction", "production"] = "development"
    database_url: str = "postgresql+psycopg://hermes:hermes@localhost:55432/hermes_orchestrator"
    actor_roles: dict[str, str] = {
        "user:owner": "owner",
        "agent:leader": "leader",
        "agent:operator": "operator",
        "agent:researcher": "researcher",
        "agent:developer": "developer",
        "agent:validator": "validator",
    }
    internal_auth_tokens: dict[str, SecretStr] = {}
    internal_auth_revoked_token_hashes: set[str] = set()
    fleet_runner_url: str = "http://fleet-reconciler:8090"
    fleet_runner_token: str = ""
    fleet_project_name: str = "hermes-tradix-f11"
    fleet_compose_path: str = (
        "/host_mnt/d/Personal/hermes-tradix/hermes-agents-compose/compose.yaml"
    )
    fleet_allowed_worker_image: str = "hermes-tradix-worker:f10"
    fleet_allowed_mount_roots: list[str] = [
        "/host_mnt/d/Personal/hermes-tradix/hermes-agents-data",
        "/host_mnt/d/Personal/hermes-tradix/tradix",
        "/host_mnt/d/Personal/hermes-tradix/hermes-agents-compose",
    ]
    usage_project_id: str = "tradix"
    usage_window_seconds: int = 86400
    usage_soft_token_limit: int = 5_000_000
    usage_hard_token_limit: int = 10_000_000
    usage_max_concurrent_runs: int = 1
    usage_max_fan_out: int = 3
    usage_max_retries: int = 1
    usage_circuit_failure_threshold: int = 3
    usage_circuit_cooldown_seconds: int = 3600
    environment_allowed_repositories: list[str] = ["jbeneitezh/tradix"]
    environment_local_port_start: int = 22000
    environment_local_port_end: int = 22999
    environment_local_default_ttl_seconds: int = 14400
    environment_local_max_ttl_seconds: int = 86400
    operations_watchdog_state_path: str = "/var/lib/hermes-orchestrator/watchdog/state.json"
    operations_watchdog_api_url: str = "http://orchestrator-api:8080"
    operations_watchdog_actor_id: str = "agent:operator"
    operations_watchdog_api_token: SecretStr = SecretStr("")
    operations_watchdog_check_seconds: int = 300
    operations_summary_interval_seconds: int = 9000
    operations_stale_after_seconds: int = 1800
    run_dispatcher_id: str = "run-dispatcher"
    run_dispatcher_batch_size: int = 1
    run_dispatcher_poll_seconds: float = 2.0
    run_dispatcher_lease_seconds: int = 120
    run_dispatcher_heartbeat_seconds: float = 30.0
    run_dispatcher_retry_seconds: int = 15
    run_dispatcher_worker_secrets: dict[str, str] = {}
    workflow_coordinator_id: str = "workflow-coordinator"
    workflow_coordinator_batch_size: int = 10
    workflow_coordinator_poll_seconds: float = 2.0
    workflow_max_depth: int = 2
    workflow_dispatch_timeout_seconds: int = 900


@lru_cache
def get_settings() -> Settings:
    return Settings()

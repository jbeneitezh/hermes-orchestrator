from __future__ import annotations

from functools import lru_cache
from typing import Literal

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


@lru_cache
def get_settings() -> Settings:
    return Settings()

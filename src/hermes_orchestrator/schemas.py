from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "unavailable"]
    database: Literal["ok", "unavailable"]


class CapabilitiesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: str
    version: str
    api_version: Literal["v1"] = "v1"
    capabilities: list[str]

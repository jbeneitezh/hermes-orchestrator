from __future__ import annotations

from typing import Any, Protocol

import httpx

from hermes_orchestrator.config import Settings


class FleetRunner(Protocol):
    def status(self) -> dict[str, Any]: ...

    def plan(self) -> dict[str, Any]: ...

    def apply(self, services: list[str]) -> dict[str, Any]: ...

    def rollback(self, services: list[str]) -> dict[str, Any]: ...


class FleetOperationUncertain(RuntimeError):
    """El transporte terminó sin conocer el resultado de una mutación."""


class HttpFleetRunnerClient:
    def __init__(self, settings: Settings) -> None:
        self.base_url = settings.fleet_runner_url.rstrip("/")
        self.token = settings.fleet_runner_token
        self.timeout = settings.fleet_runner_timeout_seconds

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.token:
            raise RuntimeError("Falta el token interno del fleet reconciler")
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.request(
                    method,
                    f"{self.base_url}{path}",
                    headers={"X-Reconciler-Token": self.token},
                    json=payload,
                )
        except httpx.TransportError as error:
            if payload and payload.get("action") in {"apply", "rollback"}:
                raise FleetOperationUncertain("Resultado de operación fleet desconocido") from error
            raise
        if (
            response.status_code >= 500
            and payload
            and payload.get("action") in {"apply", "rollback"}
        ):
            raise FleetOperationUncertain("Fleet no confirmó convergencia de la operación")
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("Respuesta inválida del fleet reconciler")
        return data

    def status(self) -> dict[str, Any]:
        return self._request("GET", "/v1/internal/status")

    def plan(self) -> dict[str, Any]:
        return self._request("POST", "/v1/internal/reconcile", {"action": "plan"})

    def apply(self, services: list[str]) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/internal/reconcile",
            {"action": "apply", "services": services},
        )

    def rollback(self, services: list[str]) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/internal/reconcile",
            {"action": "rollback", "services": services},
        )

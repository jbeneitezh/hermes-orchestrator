from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, cast

import httpx

REQUIRED_FEATURES = {
    "run_submission",
    "run_status",
    "run_events_sse",
    "run_stop",
    "run_approval_response",
}
TERMINAL_EVENTS = {"run.completed", "run.failed", "run.cancelled"}
TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
SENSITIVE_KEY_PARTS = ("authorization", "password", "secret", "api_key", "access_token")


class HermesAdapterError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        retry_after: float | None = None,
        human_action_required: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.retry_after = retry_after
        self.human_action_required = human_action_required

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "retry_after": self.retry_after,
            "human_action_required": self.human_action_required,
        }


class WorkerUnhealthyError(HermesAdapterError):
    def __init__(self, message: str = "Worker Hermes no disponible") -> None:
        super().__init__("worker_unhealthy", message, retryable=True)


class CapabilityMissingError(HermesAdapterError):
    def __init__(self, missing: list[str]) -> None:
        super().__init__(
            "capability_unavailable",
            f"Faltan capabilities Hermes: {','.join(missing)}",
            human_action_required=True,
        )
        self.missing = missing


@dataclass(frozen=True)
class HermesEvent:
    event_id: str | None
    event_type: str
    payload: dict[str, Any]
    terminal: bool


@dataclass(frozen=True)
class HermesRunState:
    run_id: str
    status: str
    output: str | None
    requested_model: str | None
    requested_reasoning_effort: str | None
    effective_model: str | None
    effective_provider: str | None
    effective_reasoning_effort: str | None
    runtime_fallback: dict[str, Any]
    usage: dict[str, int]
    error: dict[str, Any]

    @property
    def model(self) -> str | None:
        """Compatibilidad de lectura con el estado anterior del adapter."""

        return self.effective_model


class HermesRunsAdapter:
    def __init__(
        self,
        base_url: str,
        api_token: str,
        *,
        client: httpx.Client | None = None,
        timeout_seconds: float = 30,
        max_reconnects: int = 2,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._api_token = api_token
        self._owns_client = client is None
        self.client = client or httpx.Client(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {api_token}"},
            timeout=timeout_seconds,
        )
        self.max_reconnects = max_reconnects

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def __enter__(self) -> HermesRunsAdapter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _redact_string(self, value: str) -> str:
        redacted = value.replace(self._api_token, "[REDACTED]") if self._api_token else value
        redacted = re.sub(r"(?i)bearer\s+[a-z0-9._~+/-]+", "Bearer [REDACTED]", redacted)
        return re.sub(
            r"(?i)(token|api[_-]?key|secret|password)=([^&\s]+)",
            r"\1=[REDACTED]",
            redacted,
        )

    def redact(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(key): ("[REDACTED]" if self._is_sensitive_key(str(key)) else self.redact(child))
                for key, child in value.items()
            }
        if isinstance(value, list):
            return [self.redact(child) for child in value]
        if isinstance(value, str):
            return self._redact_string(value)
        return value

    @staticmethod
    def _is_sensitive_key(key: str) -> bool:
        normalized = key.lower()
        if normalized.endswith("_tokens") or normalized in {
            "tokens",
            "prompt_tokens",
            "completion_tokens",
        }:
            return False
        return normalized == "token" or any(part in normalized for part in SENSITIVE_KEY_PARTS)

    def _json(self, response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            payload = {"error": {"message": response.text}}
        if not isinstance(payload, dict):
            return {"data": payload}
        return payload

    def _raise_for_response(self, response: httpx.Response) -> None:
        if response.is_success:
            return
        payload = self.redact(self._json(response))
        error = payload.get("error", payload)
        code = error.get("code") if isinstance(error, dict) else None
        message = error.get("message") if isinstance(error, dict) else str(error)
        retry_after_header = response.headers.get("Retry-After")
        retry_after = float(retry_after_header) if retry_after_header else None
        normalized_code = str(code or "transient_provider_error")
        raise HermesAdapterError(
            normalized_code,
            str(message or f"Hermes HTTP {response.status_code}"),
            retryable=response.status_code >= 500 or response.status_code == 429,
            retry_after=retry_after,
            human_action_required=response.status_code in {401, 403},
        )

    def discover(self) -> dict[str, Any]:
        try:
            health = self.client.get(self._url("/health"))
        except httpx.TransportError as exc:
            raise WorkerUnhealthyError(self._redact_string(str(exc))) from exc
        if not health.is_success or self._json(health).get("status") != "ok":
            raise WorkerUnhealthyError()
        capabilities = self.client.get(self._url("/v1/capabilities"))
        self._raise_for_response(capabilities)
        payload = self._json(capabilities)
        features = payload.get("features", {})
        missing = sorted(
            feature for feature in REQUIRED_FEATURES if not bool(features.get(feature))
        )
        if missing:
            raise CapabilityMissingError(missing)
        return cast(dict[str, Any], self.redact(payload))

    def start_run(
        self,
        input_text: str,
        *,
        model_alias: str | None = None,
        reasoning_effort: str | None = None,
        instructions: str | None = None,
        session_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        payload = {"input": input_text}
        optional = {
            "model": model_alias,
            "reasoning_effort": reasoning_effort,
            "instructions": instructions,
            "session_id": session_id,
        }
        payload.update({key: value for key, value in optional.items() if value is not None})
        response = self.client.post(self._url("/v1/runs"), json=payload, headers=headers)
        self._raise_for_response(response)
        run_id = self._json(response).get("run_id")
        if not run_id:
            raise HermesAdapterError("invalid_worker_response", "Hermes no devolvió run_id")
        return str(run_id)

    def normalize_usage(self, payload: dict[str, Any]) -> dict[str, int]:
        source = payload.get("usage") or {}
        if not isinstance(source, dict):
            return {}

        def value(*names: str) -> int:
            for name in names:
                candidate = source.get(name)
                if isinstance(candidate, int):
                    return candidate
            return 0

        return {
            "input_tokens": value("input_tokens", "prompt_tokens"),
            "output_tokens": value("output_tokens", "completion_tokens"),
            "reasoning_tokens": value("reasoning_tokens"),
            "cache_read_tokens": value("cache_read_tokens", "cached_input_tokens"),
            "api_calls": value("api_calls"),
        }

    def normalize_error(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw = payload.get("error")
        if not raw:
            return {}
        if not isinstance(raw, dict):
            raw = {"message": str(raw)}
        redacted = self.redact(raw)
        code = str(redacted.get("code") or "transient_provider_error")
        return {
            "code": code,
            "message": str(redacted.get("message") or "Error Hermes"),
            "retryable": code
            in {"rate_limited", "timeout", "transient_provider_error", "worker_unhealthy"},
            "retry_after": redacted.get("retry_after"),
            "human_action_required": code in {"permission_denied", "approval_required"},
        }

    def get_run(self, run_id: str) -> HermesRunState:
        response = self.client.get(self._url(f"/v1/runs/{run_id}"))
        self._raise_for_response(response)
        payload = self.redact(self._json(response))
        raw_fallback = payload.get("runtime_fallback") or payload.get("fallback") or {}
        return HermesRunState(
            run_id=str(payload.get("run_id") or run_id),
            status=str(payload.get("status") or "unknown"),
            output=str(payload["output"]) if payload.get("output") is not None else None,
            requested_model=(
                str(payload["requested_model"])
                if payload.get("requested_model") is not None
                else None
            ),
            requested_reasoning_effort=(
                str(payload["requested_reasoning_effort"])
                if payload.get("requested_reasoning_effort") is not None
                else None
            ),
            effective_model=(
                str(payload["effective_model"])
                if payload.get("effective_model") is not None
                else str(payload["model"])
                if payload.get("model") is not None
                else None
            ),
            effective_provider=(
                str(payload["effective_provider"])
                if payload.get("effective_provider") is not None
                else None
            ),
            effective_reasoning_effort=(
                str(payload["effective_reasoning_effort"])
                if payload.get("effective_reasoning_effort") is not None
                else None
            ),
            runtime_fallback=(
                cast(dict[str, Any], raw_fallback) if isinstance(raw_fallback, dict) else {}
            ),
            usage=self.normalize_usage(payload),
            error=self.normalize_error(payload),
        )

    def stop_run(self, run_id: str) -> HermesRunState:
        response = self.client.post(self._url(f"/v1/runs/{run_id}/stop"), json={})
        self._raise_for_response(response)
        return self.get_run(run_id)

    def respond_approval(self, run_id: str, choice: str) -> dict[str, Any]:
        response = self.client.post(
            self._url(f"/v1/runs/{run_id}/approval"), json={"choice": choice}
        )
        self._raise_for_response(response)
        return cast(dict[str, Any], self.redact(self._json(response)))

    def stream_events(self, run_id: str) -> list[HermesEvent]:
        events: list[HermesEvent] = []
        fingerprints: set[str] = set()
        last_event_id: str | None = None
        reconnects = 0
        while reconnects <= self.max_reconnects:
            headers = {"Accept": "text/event-stream"}
            if last_event_id:
                headers["Last-Event-ID"] = last_event_id
            try:
                with self.client.stream(
                    "GET", self._url(f"/v1/runs/{run_id}/events"), headers=headers
                ) as response:
                    self._raise_for_response(response)
                    frame: dict[str, str] = {}
                    for line in response.iter_lines():
                        if line == "":
                            event = self._parse_frame(frame)
                            frame = {}
                            if event is None:
                                continue
                            fingerprint = self._fingerprint(event)
                            if fingerprint in fingerprints:
                                continue
                            fingerprints.add(fingerprint)
                            events.append(event)
                            if event.event_id:
                                last_event_id = event.event_id
                            if event.terminal:
                                return events
                            continue
                        if line.startswith(":") or ":" not in line:
                            continue
                        key, value = line.split(":", 1)
                        frame[key] = value.lstrip()
                    event = self._parse_frame(frame)
                    if event is not None:
                        fingerprint = self._fingerprint(event)
                        if fingerprint not in fingerprints:
                            fingerprints.add(fingerprint)
                            events.append(event)
                            if event.event_id:
                                last_event_id = event.event_id
                            if event.terminal:
                                return events
            except httpx.TransportError:
                pass
            reconnects += 1
        raise HermesAdapterError(
            "worker_disconnected",
            "SSE terminó sin evento terminal",
            retryable=True,
        )

    def _parse_frame(self, frame: dict[str, str]) -> HermesEvent | None:
        if "data" not in frame:
            return None
        try:
            decoded = json.loads(frame["data"])
        except json.JSONDecodeError:
            decoded = {"message": frame["data"]}
        if not isinstance(decoded, dict):
            decoded = {"data": decoded}
        payload = self.redact(decoded)
        event_type = str(frame.get("event") or payload.get("event") or "message")
        return HermesEvent(
            event_id=frame.get("id"),
            event_type=event_type,
            payload=payload,
            terminal=event_type in TERMINAL_EVENTS,
        )

    @staticmethod
    def _fingerprint(event: HermesEvent) -> str:
        raw = json.dumps(
            {"id": event.event_id, "event": event.event_type, "payload": event.payload},
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(raw.encode()).hexdigest()

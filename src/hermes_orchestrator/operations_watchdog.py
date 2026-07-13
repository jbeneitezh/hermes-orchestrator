from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from hermes_orchestrator.config import Settings


class OperationsReader(Protocol):
    def get(self, path: str, query: dict[str, str] | None = None) -> dict[str, Any]: ...


class PublicOperationsApiReader:
    def __init__(self, base_url: str, actor_id: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.actor_id = actor_id

    def get(self, path: str, query: dict[str, str] | None = None) -> dict[str, Any]:
        suffix = f"?{urllib.parse.urlencode(query)}" if query else ""
        request = urllib.request.Request(
            f"{self.base_url}{path}{suffix}",
            headers={"X-Actor-Id": self.actor_id},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read())
        if not isinstance(payload, dict):
            raise RuntimeError("Respuesta pública de operaciones inválida")
        return payload


def utc_now() -> datetime:
    return datetime.now(UTC)


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "summary_count": 0,
            "idle_checks": 0,
            "active_checks": 0,
            "model_calls": 0,
            "last_cursor": None,
            "last_summary_at": None,
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "summary_count": 0,
            "idle_checks": 0,
            "active_checks": 0,
            "model_calls": 0,
            "last_cursor": None,
            "last_summary_at": None,
        }
    return payload if isinstance(payload, dict) else {}


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def _summary_due(state: dict[str, Any], now: datetime, interval_seconds: int) -> bool:
    raw = state.get("last_summary_at")
    if not isinstance(raw, str):
        return True
    try:
        previous = datetime.fromisoformat(raw)
    except ValueError:
        return True
    if previous.tzinfo is None:
        previous = previous.replace(tzinfo=UTC)
    return (now - previous).total_seconds() >= interval_seconds


def evaluate_watchdog(
    reader: OperationsReader,
    state_path: str,
    *,
    now: datetime | None = None,
    summary_interval_seconds: int = 9000,
) -> dict[str, Any]:
    effective_now = now or utc_now()
    path = Path(state_path)
    state = _load_state(path)
    tasks = reader.get("/v1/operations/tasks", {"active_only": "true", "limit": "200"})
    query = {"limit": "200"}
    previous_cursor = state.get("last_cursor")
    if isinstance(previous_cursor, str) and previous_cursor:
        query["cursor"] = previous_cursor
    timeline = reader.get("/v1/operations/timeline", query)
    active_count = int(tasks.get("active_count", 0))
    stale_count = int(tasks.get("stale_count", 0))
    new_event_count = int(timeline.get("count", 0))
    has_work = active_count > 0
    has_new_events = new_event_count > 0
    due = _summary_due(state, effective_now, summary_interval_seconds)
    summary_emitted = has_work and has_new_events and due

    state["checked_at"] = effective_now.isoformat()
    state["last_cursor"] = timeline.get("next_cursor") or previous_cursor
    state["active_tasks"] = active_count
    state["stale_tasks"] = stale_count
    state["new_events"] = new_event_count
    state["model_calls"] = 0
    state["status"] = "active" if has_work else "idle"
    state["summary_emitted"] = summary_emitted
    state["idle_checks"] = int(state.get("idle_checks", 0)) + int(not has_work)
    state["active_checks"] = int(state.get("active_checks", 0)) + int(has_work)
    state["summary_count"] = int(state.get("summary_count", 0))
    if summary_emitted:
        state["summary_count"] += 1
        state["last_summary_at"] = effective_now.isoformat()
        state["last_summary"] = {
            "kind": "deterministic_operations_rollup",
            "active_tasks": active_count,
            "stale_tasks": stale_count,
            "new_events": new_event_count,
            "generated_at": effective_now.isoformat(),
            "model_calls": 0,
        }
    _write_state(path, state)
    return state


def _record_failure(path: str, error: Exception) -> None:
    state_path = Path(path)
    state = _load_state(state_path)
    state.update(
        {
            "checked_at": utc_now().isoformat(),
            "status": "degraded",
            "last_error": type(error).__name__,
            "model_calls": 0,
            "summary_emitted": False,
        }
    )
    _write_state(state_path, state)


def main() -> None:
    parser = argparse.ArgumentParser(description="Watchdog público y determinista de operaciones")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    settings = Settings()
    reader = PublicOperationsApiReader(
        settings.operations_watchdog_api_url,
        settings.operations_watchdog_actor_id,
    )
    while True:
        try:
            evaluate_watchdog(
                reader,
                settings.operations_watchdog_state_path,
                summary_interval_seconds=settings.operations_summary_interval_seconds,
            )
        except Exception as error:
            _record_failure(settings.operations_watchdog_state_path, error)
        if args.once:
            break
        time.sleep(settings.operations_watchdog_check_seconds)


if __name__ == "__main__":
    main()

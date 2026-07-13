from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

FEATURES = {
    "run_submission": True,
    "run_status": True,
    "run_events_sse": True,
    "run_stop": True,
    "run_approval_response": True,
}


@dataclass
class FakeHermesState:
    scenario: str = "completed"
    healthy: bool = True
    features: dict[str, bool] = field(default_factory=lambda: dict(FEATURES))
    event_requests: int = 0
    last_event_ids: list[str | None] = field(default_factory=list)
    status: str = "completed"


class FakeHermesServer:
    def __init__(self, state: FakeHermesState | None = None) -> None:
        self.state = state or FakeHermesState()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> FakeHermesServer:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        state = self.state

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_: object) -> None:
                return

            def send_json(self, status: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                if self.path == "/health":
                    self.send_json(
                        200 if state.healthy else 503,
                        {"status": "ok" if state.healthy else "error"},
                    )
                    return
                if self.path == "/v1/capabilities":
                    self.send_json(200, {"features": state.features})
                    return
                if self.path == "/v1/runs/fake-run":
                    error = (
                        {"code": "provider_failed", "message": "Proveedor rechazó la petición"}
                        if state.status == "failed"
                        else None
                    )
                    self.send_json(
                        200,
                        {
                            "run_id": "fake-run",
                            "status": state.status,
                            "output": "F8_OK" if state.status == "completed" else None,
                            "model": "gpt-5.3-codex-spark",
                            "usage": {"prompt_tokens": 11, "completion_tokens": 3},
                            "error": error,
                        },
                    )
                    return
                if self.path == "/v1/runs/fake-run/events":
                    state.event_requests += 1
                    state.last_event_ids.append(self.headers.get("Last-Event-ID"))
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    first = 'id: 1\nevent: message.delta\ndata: {"delta":"F8"}\n\n'
                    self.wfile.write(first.encode())
                    self.wfile.flush()
                    if state.scenario == "reconnect" and state.event_requests == 1:
                        return
                    terminal = "run.failed" if state.status == "failed" else "run.completed"
                    terminal_frame = (
                        f'id: 2\nevent: {terminal}\ndata: {{"status":"{state.status}"}}\n\n'
                    )
                    self.wfile.write(terminal_frame.encode())
                    self.wfile.flush()
                    return
                self.send_json(404, {"error": {"code": "not_found", "message": "No existe"}})

            def do_POST(self) -> None:
                if self.path == "/v1/runs":
                    self.send_json(202, {"run_id": "fake-run"})
                    return
                if self.path == "/v1/runs/fake-run/stop":
                    state.status = "cancelled"
                    self.send_json(200, {"status": "cancelled"})
                    return
                if self.path == "/v1/runs/fake-run/approval":
                    self.send_json(200, {"accepted": True})
                    return
                self.send_json(404, {"error": {"code": "not_found", "message": "No existe"}})

        return Handler

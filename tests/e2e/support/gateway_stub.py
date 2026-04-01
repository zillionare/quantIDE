"""Minimal qmt-gateway stub used by end-to-end tests."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _normalize_prefix(prefix: str) -> str:
    text = str(prefix or "/").strip() or "/"
    if not text.startswith("/"):
        text = f"/{text}"
    if text != "/":
        text = text.rstrip("/")
    return text


def _build_ping_handler(prefix: str):
    expected_path = "/ping" if prefix == "/" else f"{prefix}/ping"

    class GatewayPingHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path != expected_path:
                self.send_response(404)
                self.end_headers()
                return

            body = b'{"ok": true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            return

    return GatewayPingHandler


@dataclass
class GatewayStub:
    """Running gateway stub server instance."""

    host: str
    port: int
    prefix: str
    _server: ThreadingHTTPServer
    _thread: threading.Thread

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=1)


def start_gateway_stub(host: str = "127.0.0.1", prefix: str = "/") -> GatewayStub:
    """Start a lightweight HTTP server that only supports `GET /ping`."""

    normalized_prefix = _normalize_prefix(prefix)
    server = ThreadingHTTPServer(
        (host, 0),
        _build_ping_handler(normalized_prefix),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return GatewayStub(
        host=host,
        port=int(server.server_address[1]),
        prefix=normalized_prefix,
        _server=server,
        _thread=thread,
    )


@contextmanager
def running_gateway_stub(host: str = "127.0.0.1", prefix: str = "/"):
    """Yield a running gateway stub and stop it automatically."""

    stub = start_gateway_stub(host=host, prefix=prefix)
    try:
        yield stub
    finally:
        stub.stop()
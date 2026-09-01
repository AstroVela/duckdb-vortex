#!/usr/bin/env python3
"""A quiet S3 reverse proxy with deterministic COPY publication faults."""

from __future__ import annotations

import http.client
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


class S3FaultController:
    """Match one publication phase while allowing all other S3 requests."""

    STAGES = frozenset({"upload", "metadata", "manifest", "marker"})

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stage: str | None = None
        self._hits = 0

    def arm(self, stage: str) -> None:
        if stage not in self.STAGES:
            raise ValueError(f"unsupported S3 fault stage: {stage}")
        with self._lock:
            if self._stage is not None:
                raise RuntimeError(f"S3 fault stage is already armed: {self._stage}")
            self._stage = stage
            self._hits = 0

    def disarm(self) -> None:
        with self._lock:
            self._stage = None

    @property
    def hits(self) -> int:
        with self._lock:
            return self._hits

    @staticmethod
    def _object_path(request_target: str) -> str:
        return urllib.parse.unquote(urllib.parse.urlsplit(request_target).path)

    def reject_before_forward(self, method: str, request_target: str) -> bool:
        path = self._object_path(request_target)
        with self._lock:
            reject = (
                self._stage == "upload"
                and method in {"POST", "PUT"}
                and path.endswith(".vortex")
            ) or (
                self._stage == "manifest"
                and method in {"POST", "PUT"}
                and path.endswith("/manifest.txt")
            ) or (
                self._stage == "marker"
                and method in {"POST", "PUT"}
                and path.endswith("/committed")
            )
            if reject:
                self._hits += 1
        return reject

    def reject_successful_response(
        self, method: str, request_target: str, status: int
    ) -> bool:
        # httpfs falls back from a failed HEAD to a ranged GET while loading
        # object metadata, so both successful response forms must be faulted.
        if method not in {"GET", "HEAD"} or not 200 <= status < 300:
            return False
        path = self._object_path(request_target)
        if not path.endswith(".vortex"):
            return False
        with self._lock:
            if self._stage != "metadata":
                return False
            self._hits += 1
            return True


class S3FaultProxy:
    def __init__(self, upstream_endpoint: str) -> None:
        parsed = urllib.parse.urlsplit(upstream_endpoint)
        if parsed.scheme != "http" or not parsed.hostname or parsed.path not in {"", "/"}:
            raise ValueError("the fault proxy requires a plain HTTP upstream origin")
        self._upstream_host = parsed.hostname
        self._upstream_port = parsed.port or 80
        self.controller = S3FaultController()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def endpoint(self) -> str:
        if self._server is None:
            raise RuntimeError("S3 fault proxy is not running")
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> "S3FaultProxy":
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, _format: str, *args: object) -> None:
                # Request headers contain SigV4 material; never emit them.
                return None

            def _read_chunked_body(self) -> bytes:
                chunks: list[bytes] = []
                while True:
                    size_line = self.rfile.readline()
                    if not size_line:
                        raise ConnectionError("unexpected EOF in chunked request")
                    size_text = size_line.split(b";", 1)[0].strip()
                    size = int(size_text, 16)
                    if size == 0:
                        while self.rfile.readline() not in {b"\r\n", b"\n", b""}:
                            pass
                        break
                    chunks.append(self.rfile.read(size))
                    if self.rfile.read(2) != b"\r\n":
                        raise ConnectionError("invalid chunk terminator")
                return b"".join(chunks)

            def _read_body(self) -> bytes:
                content_length = self.headers.get("Content-Length")
                if content_length is not None:
                    return self.rfile.read(int(content_length))
                if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                    return self._read_chunked_body()
                return b""

            def _send_fault(self) -> None:
                body = (
                    b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                    b"<Error><Code>AccessDenied</Code>"
                    b"<Message>injected Vortex COPY S3 fault</Message></Error>"
                )
                self.send_response(403, "Forbidden")
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
                self.close_connection = True

            def _proxy(self) -> None:
                body = self._read_body()
                if proxy.controller.reject_before_forward(self.command, self.path):
                    self._send_fault()
                    return
                headers = {
                    name: value
                    for name, value in self.headers.items()
                    if name.lower() not in _HOP_BY_HOP_HEADERS
                    and name.lower() != "content-length"
                }
                headers["Content-Length"] = str(len(body))
                upstream = http.client.HTTPConnection(
                    proxy._upstream_host,
                    proxy._upstream_port,
                    timeout=30,
                )
                try:
                    upstream.request(self.command, self.path, body=body, headers=headers)
                    response = upstream.getresponse()
                    response_body = response.read()
                    response_content_length = response.getheader("Content-Length")
                    if proxy.controller.reject_successful_response(
                        self.command, self.path, response.status
                    ):
                        self._send_fault()
                        return
                    self.send_response(response.status, response.reason)
                    for name, value in response.getheaders():
                        if (
                            name.lower() not in _HOP_BY_HOP_HEADERS
                            and name.lower() != "content-length"
                        ):
                            self.send_header(name, value)
                    if self.command == "HEAD" and response_content_length is not None:
                        self.send_header("Content-Length", response_content_length)
                    else:
                        self.send_header("Content-Length", str(len(response_body)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    if self.command != "HEAD" and response_body:
                        self.wfile.write(response_body)
                    self.close_connection = True
                finally:
                    upstream.close()

            do_DELETE = _proxy
            do_GET = _proxy
            do_HEAD = _proxy
            do_POST = _proxy
            do_PUT = _proxy

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="vortex-s3-fault-proxy",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise RuntimeError("S3 fault proxy did not stop")
        self._server = None
        self._thread = None

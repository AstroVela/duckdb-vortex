# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Bison download failover, checksum and bootstrap-cache regressions."""

from __future__ import annotations

import hashlib
import importlib.util
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "fetch_vane_bison", ROOT / "scripts/fetch_vane_bison.py"
)
fetcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetcher)
PAYLOAD = b"verified Bison archive fixture"


@pytest.fixture
def checksum(monkeypatch):
    monkeypatch.setattr(fetcher, "SHA256", hashlib.sha256(PAYLOAD).hexdigest())


@pytest.mark.parametrize("failure", [7, 28, "timeout", "checksum"])
def test_failed_primary_uses_verified_mirror(tmp_path, monkeypatch, checksum, failure):
    urls = []

    def download(command, *, check, timeout):
        assert check and timeout == 150
        urls.append(command[-1])
        candidate = Path(command[command.index("--output") + 1])
        candidate.write_bytes(b"partial response" if len(urls) == 1 else PAYLOAD)
        if len(urls) == 1:
            if isinstance(failure, int):
                raise subprocess.CalledProcessError(failure, command)
            if failure == "timeout":
                raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(fetcher.subprocess, "run", download)
    archive = fetcher.fetch_bison(tmp_path)
    assert urls == list(fetcher.URLS)
    assert archive.read_bytes() == PAYLOAD
    assert list(tmp_path.iterdir()) == [archive]


def test_verified_archive_is_reused_without_network(tmp_path, monkeypatch, checksum):
    archive = tmp_path / fetcher.ARCHIVE_NAME
    archive.write_bytes(PAYLOAD)
    download = Mock(side_effect=AssertionError("cached archive must not be downloaded"))
    monkeypatch.setattr(fetcher.subprocess, "run", download)
    assert fetcher.fetch_bison(tmp_path) == archive
    download.assert_not_called()


def test_corrupt_cache_is_replaced_only_after_verification(
    tmp_path, monkeypatch, checksum
):
    archive = tmp_path / fetcher.ARCHIVE_NAME
    archive.write_bytes(b"corrupt cache")

    def download(command, **kwargs):
        assert archive.read_bytes() == b"corrupt cache"
        Path(command[command.index("--output") + 1]).write_bytes(PAYLOAD)

    monkeypatch.setattr(fetcher.subprocess, "run", download)
    assert fetcher.fetch_bison(tmp_path).read_bytes() == PAYLOAD
    assert list(tmp_path.iterdir()) == [archive]


@pytest.mark.parametrize("failure", ["network", "checksum"])
def test_all_failed_sources_leave_no_partial_archive(
    tmp_path, monkeypatch, checksum, failure
):
    urls = []

    def download(command, **kwargs):
        urls.append(command[-1])
        Path(command[command.index("--output") + 1]).write_bytes(b"invalid response")
        if failure == "network":
            raise subprocess.CalledProcessError(7, command)

    monkeypatch.setattr(fetcher.subprocess, "run", download)
    with pytest.raises(RuntimeError, match="All pinned Bison download sources failed"):
        fetcher.fetch_bison(tmp_path)
    assert urls == list(fetcher.URLS)
    assert list(tmp_path.iterdir()) == []


def test_real_curl_rejects_http_error_and_uses_mirror(tmp_path, monkeypatch, checksum):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            body = PAYLOAD if self.path == "/mirror" else b"unavailable"
            self.send_response(200 if self.path == "/mirror" else 404)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        monkeypatch.setattr(fetcher, "URLS", (f"{base}/primary", f"{base}/mirror"))
        monkeypatch.setenv("NO_PROXY", "127.0.0.1")
        monkeypatch.setenv("no_proxy", "127.0.0.1")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            assert fetcher.fetch_bison(tmp_path).read_bytes() == PAYLOAD
        finally:
            server.shutdown()
            thread.join(timeout=5)
    assert requests == ["/primary", "/mirror"]


@pytest.mark.parametrize("job_name", ["vane-dynamic-wheel", "vane-provider-prepare"])
def test_workflows_verify_archive_before_pinned_bootstrap(job_name):
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/VaneExtension.yml").read_text()
    )
    bootstrap = next(
        step
        for step in workflow["jobs"][job_name]["steps"]
        if step.get("name") == "Build pinned Bison with the exact Vane bootstrap"
    )
    commands = bootstrap["run"]
    assert commands.index(
        'python -I "$VANE_EXTENSION_ROOT/scripts/fetch_vane_bison.py"'
    ) < commands.index('bash "$VANE_SOURCE_DIR/scripts/bootstrap_manylinux_bison.sh"')

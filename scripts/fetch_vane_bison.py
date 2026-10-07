#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Fill the pinned Vane bootstrap's Bison archive cache from verified mirrors."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
from pathlib import Path

VERSION = "3.8.2"
ARCHIVE_NAME = f"bison-{VERSION}.tar.gz"
SHA256 = "06c9e13bdf7eb24d4ceb6b59205a4f67c2c7e7213119644430fe82fbd14a0abb"
URLS = (
    f"https://ftp.gnu.org/gnu/bison/{ARCHIVE_NAME}",
    f"https://mirrors.kernel.org/gnu/bison/{ARCHIVE_NAME}",
)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def fetch_bison(build_root: Path) -> Path:
    build_root.mkdir(parents=True, exist_ok=True)
    archive = build_root / ARCHIVE_NAME
    if archive.is_file() and digest(archive) == SHA256:
        print(f"Using verified GNU Bison archive: {archive}", flush=True)
        return archive

    with tempfile.TemporaryDirectory(prefix="bison-download-", dir=build_root) as work:
        candidate = Path(work) / ARCHIVE_NAME
        for url in URLS:
            print(f"Downloading GNU Bison {VERSION} from {url}", flush=True)
            try:
                subprocess.run(
                    [
                        "curl",
                        "--fail",
                        "--location",
                        "--silent",
                        "--show-error",
                        "--connect-timeout",
                        "15",
                        "--max-time",
                        "60",
                        "--retry",
                        "2",
                        "--retry-delay",
                        "2",
                        "--retry-max-time",
                        "90",
                        "--output",
                        str(candidate),
                        url,
                    ],
                    check=True,
                    timeout=150,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                print(f"Bison download failed: {error}", file=sys.stderr, flush=True)
            else:
                if digest(candidate) == SHA256:
                    candidate.replace(archive)
                    print(f"Verified GNU Bison {VERSION}: {SHA256}", flush=True)
                    return archive
                print(f"Bison SHA-256 mismatch from {url}", file=sys.stderr, flush=True)
            candidate.unlink(missing_ok=True)

    raise RuntimeError("All pinned Bison download sources failed verification")


if __name__ == "__main__":
    fetch_bison(Path(os.environ["VANE_BUILD_TOOLS_DIR"]))

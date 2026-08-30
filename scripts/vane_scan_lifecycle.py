#!/usr/bin/env python3
"""Build and run the Vane Vortex scan lifecycle test under ASAN/LSAN."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any


NAME_RE = re.compile(r"^[a-z0-9_]+$")
SOURCE_ID_RE = re.compile(r"^[0-9a-f]{40}$")


class ConfigurationError(RuntimeError):
    """Raised when the explicit Vane lifecycle build contract is not met."""


def fail(message: str) -> None:
    raise ConfigurationError(message)


def run(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    capture: bool = False,
) -> str:
    print(f"+ {shlex.join(command)}", file=sys.stderr)
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        stdout=subprocess.PIPE if capture else None,
    )
    result.check_returncode()
    return result.stdout if capture else ""


def read_json(command: list[str], *, cwd: Path, label: str) -> dict[str, Any]:
    output = run(command, cwd=cwd, capture=True)
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        fail(f"{label} did not return valid JSON: {exc}")
    if not isinstance(value, dict):
        fail(f"{label} did not return a JSON object")
    return value


def require_file(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        fail(f"{label} is not a file: {resolved}")
    return resolved


def require_positive_int(value: str, label: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        fail(f"{label} must be a positive integer: {value}")
    if parsed <= 0:
        fail(f"{label} must be a positive integer: {value}")
    return parsed


def require_string(mapping: dict[str, Any], key: str, label: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        fail(f"{label} has no valid {key}")
    return value


def append_flags(current: str | None, required: str) -> str:
    return " ".join(part for part in (current, required) if part)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--vane-source", type=Path, required=True)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--jobs", required=True)
    args = parser.parse_args()

    if sys.platform != "linux" or platform.machine() != "x86_64":
        fail("the Vane ASAN/LSAN lifecycle lane requires 64-bit x86 Linux")

    jobs = require_positive_int(args.jobs, "jobs")
    extension_root = args.extension_root.resolve()
    manifest_path = require_file(args.manifest, "Vane manifest")
    if not manifest_path.is_relative_to(extension_root):
        fail("the Vane manifest must be inside the extension repository")

    ci_tool = require_file(
        extension_root / "vane-extension-ci-tools/scripts/vane_extension.py",
        "pinned Vane CI tool",
    )
    vane_source = args.vane_source.resolve()
    duckdb_source = vane_source / "external/duckdb"
    if not (duckdb_source / "CMakeLists.txt").is_file():
        fail(f"Vane source has no external DuckDB tree: {duckdb_source}")

    toolchain_value = os.environ.get("VCPKG_TOOLCHAIN_PATH")
    if not toolchain_value:
        fail("VCPKG_TOOLCHAIN_PATH must select the exact pinned vcpkg toolchain")
    toolchain = require_file(Path(toolchain_value), "vcpkg toolchain")

    tool_command = [
        sys.executable,
        str(ci_tool),
        "--manifest",
        str(manifest_path),
        "--extension-root",
        str(extension_root),
    ]
    manifest = read_json(
        tool_command + ["manifest"], cwd=extension_root, label="Vane manifest"
    )
    identity = read_json(
        tool_command + ["identity", "--vane-source", str(vane_source)],
        cwd=extension_root,
        label="Vane identity",
    )
    run(tool_command + ["verify-vcpkg"], cwd=extension_root)

    extension_config = require_file(
        Path(require_string(manifest, "extension_config", "Vane manifest")),
        "Vane extension config",
    )
    if not extension_config.is_relative_to(extension_root):
        fail("the Vane extension config must be inside the extension repository")
    build_extensions = require_string(manifest, "build_extensions", "Vane manifest")
    if any(not NAME_RE.fullmatch(name) for name in build_extensions.split(";")):
        fail(f"Vane manifest has invalid build_extensions: {build_extensions!r}")

    source_id = require_string(identity, "source_id", "Vane identity")
    if not SOURCE_ID_RE.fullmatch(source_id):
        fail(f"Vane identity has invalid source_id: {source_id!r}")
    fork_version = require_string(identity, "fork_version", "Vane identity")
    upstream_version = require_string(identity, "upstream_version", "Vane identity")

    target_triplet = os.environ.get("VCPKG_TARGET_TRIPLET", "x64-linux")
    if target_triplet != "x64-linux":
        fail(
            "the Vane ASAN/LSAN lifecycle lane requires VCPKG_TARGET_TRIPLET=x64-linux"
        )

    build_dir = args.build_dir.resolve()
    build_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir = build_dir / "test"
    sanitizer_flags = "-fsanitize=address -fno-omit-frame-pointer"
    build_env = os.environ.copy()
    # CMake instruments DuckDB itself. CXXFLAGS additionally reaches the
    # companion C++ objects built by Cargo's cc crate; setting global CFLAGS
    # would also instrument host build dependencies without an ASAN runtime.
    build_env["CXXFLAGS"] = append_flags(build_env.get("CXXFLAGS"), sanitizer_flags)
    build_env["RUSTFLAGS"] = append_flags(
        build_env.get("RUSTFLAGS"), "-Cforce-frame-pointers=yes"
    )
    build_env["CARGO_BUILD_JOBS"] = str(jobs)
    build_env["CMAKE_BUILD_PARALLEL_LEVEL"] = str(jobs)
    build_env["VCPKG_MAX_CONCURRENCY"] = str(jobs)

    cmake_command = [
        "cmake",
        "--fresh",
        "-S",
        str(duckdb_source),
        "-B",
        str(build_dir),
        "-G",
        os.environ.get("VANE_CMAKE_GENERATOR", "Ninja"),
        "-DCMAKE_BUILD_TYPE=Debug",
        "-DCMAKE_CXX_STANDARD=20",
        "-DCMAKE_CXX_STANDARD_REQUIRED=ON",
        "-DCMAKE_CXX_EXTENSIONS=OFF",
        f"-DCMAKE_RUNTIME_OUTPUT_DIRECTORY={runtime_dir}",
        "-DBUILD_UNITTESTS=ON",
        "-DBUILD_BENCHMARKS=OFF",
        "-DBUILD_DISTRIBUTED_EXCHANGE=OFF",
        "-DEXTENSION_STATIC_BUILD=ON",
        "-DENABLE_EXTENSION_AUTOLOADING=OFF",
        "-DENABLE_EXTENSION_AUTOINSTALL=OFF",
        "-DENABLE_SANITIZER=ON",
        "-DENABLE_UBSAN=OFF",
        "-DDISABLE_VPTR_SANITIZER=ON",
        "-DVORTEX_VANE_DISTRIBUTED=ON",
        "-DBUILD_VORTEX_DISTRIBUTED_TESTS=ON",
        f"-DDUCKDB_SOURCE_PATH={duckdb_source}",
        f"-DDUCKDB_EXTENSION_CONFIGS={extension_config}",
        f"-DBUILD_EXTENSIONS={build_extensions}",
        f"-DUNITTEST_ROOT_DIRECTORY={extension_root}",
        "-DENABLE_UNITTEST_CPP_TESTS=FALSE",
        "-DVCPKG_BUILD=ON",
        f"-DCMAKE_TOOLCHAIN_FILE={toolchain}",
        f"-DVCPKG_MANIFEST_DIR={extension_root}",
        f"-DVCPKG_TARGET_TRIPLET={target_triplet}",
        f"-DOVERRIDE_GIT_DESCRIBE={upstream_version}-0-g{source_id[:10]}",
        f"-DDUCKDB_EXPLICIT_VERSION={fork_version}",
        f"-DGIT_COMMIT_HASH={source_id[:10]}",
    ]
    launcher = os.environ.get("VANE_CMAKE_COMPILER_LAUNCHER")
    if launcher is not None:
        if launcher != "ccache":
            fail("VANE_CMAKE_COMPILER_LAUNCHER must be ccache")
        cmake_command.extend(
            [
                "-DCMAKE_C_COMPILER_LAUNCHER=ccache",
                "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",
            ]
        )

    run(cmake_command, cwd=extension_root, env=build_env)
    run(
        [
            "cmake",
            "--build",
            str(build_dir),
            "--target",
            "vortex_distributed_protocol_test",
            "--parallel",
            str(jobs),
        ],
        cwd=extension_root,
        env=build_env,
    )

    test_binary = require_file(
        runtime_dir / "vortex_distributed_protocol_test",
        "Vane Vortex lifecycle test",
    )
    dynamic_dependencies = run(
        ["readelf", "--dynamic", str(test_binary)],
        cwd=extension_root,
        capture=True,
    )
    dynamic_symbols = run(
        ["readelf", "--wide", "--symbols", str(test_binary)],
        cwd=extension_root,
        capture=True,
    )
    if "libasan.so" not in dynamic_dependencies or "__asan_init" not in dynamic_symbols:
        fail("the Vane lifecycle executable is not linked to the ASAN runtime")

    test_env = build_env.copy()
    test_env["ASAN_OPTIONS"] = (
        "detect_leaks=1:halt_on_error=1:abort_on_error=1:strict_string_checks=1"
    )
    test_env["LSAN_OPTIONS"] = "exitcode=23:report_objects=1:print_suppressions=0"
    test_env["RUST_BACKTRACE"] = "1"
    run([str(test_binary)], cwd=extension_root, env=test_env)
    print("Vane Vortex ASAN/LSAN lifecycle qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ConfigurationError, subprocess.CalledProcessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Package original signed native data without build tools, keys or upload OIDC."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import stat
import sys
import tempfile
from pathlib import Path

from packaging.utils import parse_wheel_filename
from packaging.version import Version

_SPEC = importlib.util.spec_from_file_location(
    "_vortex_wheel_builder", Path(__file__).with_name("build_vane_dynamic_wheel.py")
)
builder = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(builder)

LICENSE_NAMES = (
    "DuckDB-Vortex-MIT.txt",
    "Vane-Apache-2.0.txt",
    "Vane-NOTICE.txt",
    "DuckDB-static-engine-licenses.txt",
    "Rust-third-party-licenses.txt",
    "Rust-standard-library-licenses.html",
)
MAX_ARTIFACT_BYTES = 384 * 1024 * 1024
MAX_LICENSE_BYTES = 16 * 1024 * 1024


def require_regular(path: Path, maximum: int) -> int:
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or not 0 < info.st_size <= maximum
        or any(parent.is_symlink() for parent in path.parents)
    ):
        raise builder.QualificationError(
            "bundle data must be bounded regular non-symlink files"
        )
    return info.st_size


def require_signed_payload(unsigned: Path, signed: Path) -> Path:
    original_size = require_regular(unsigned, MAX_ARTIFACT_BYTES)
    signed_size = require_regular(signed, MAX_ARTIFACT_BYTES)
    if original_size <= 512 or original_size != signed_size:
        raise builder.QualificationError(
            "signing changed the native artifact size or footer is missing"
        )
    remaining = original_size - 256
    with unsigned.open("rb") as before, signed.open("rb") as after:
        while remaining:
            size = min(1024 * 1024, remaining)
            original, candidate = before.read(size), after.read(size)
            if len(original) != size or original != candidate:
                raise builder.QualificationError(
                    "signing changed the native artifact payload"
                )
            remaining -= size
        original, candidate = before.read(257), after.read(257)
        if original != b"\0" * 256 or len(candidate) != 256 or candidate == original:
            raise builder.QualificationError(
                "signing must replace only the empty native signature slot"
            )
    return signed


def require_licenses(directory: Path) -> tuple[Path, ...]:
    if set(path.name for path in directory.iterdir()) != set(LICENSE_NAMES):
        raise builder.QualificationError(
            "Vortex license bundle differs from the reviewed set"
        )
    files = tuple(directory / name for name in LICENSE_NAMES)
    for path in files:
        require_regular(path, MAX_LICENSE_BYTES)
    return files


def require_runtime_wheels(wheels: list[Path], version: str, profile: str) -> None:
    parsed = Version(version)
    if (
        str(parsed) != version
        or len(parsed.release) != 3
        or parsed.local is not None
        or parsed.epoch
        or parsed.is_devrelease != (profile == "testpypi")
    ):
        raise builder.QualificationError(
            "runtime version does not match the selected publication channel"
        )
    identities = set()
    for path in wheels:
        name, wheel_version, build, tags = parse_wheel_filename(path.name)
        if name != "vane-ai" or str(wheel_version) != version or build:
            raise builder.QualificationError(
                "runtime wheel does not identify the exact selected Vane version"
            )
        candidates = {
            tag.interpreter
            for tag in tags
            if tag.platform == builder.PROVIDER_PLATFORM_TAG
            and tag.abi == tag.interpreter
        }
        if len(candidates) != 1 or identities.intersection(candidates):
            raise builder.QualificationError(
                "runtime wheels require distinct supported CPython targets"
            )
        identities.update(candidates)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension-root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--vane-source", required=True, type=Path)
    parser.add_argument("--vane-revision", required=True)
    parser.add_argument("--vane-version", required=True)
    parser.add_argument("--unsigned-directory", required=True, type=Path)
    parser.add_argument("--signed-directory", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument(
        "--signing-profile", required=True, choices=("testpypi", "production")
    )
    parser.add_argument("--runtime-python", required=True, action="append", type=Path)
    parser.add_argument("--runtime-wheel", required=True, action="append", type=Path)
    args = parser.parse_args(argv)
    if len(args.runtime_python) != len(args.runtime_wheel):
        raise builder.QualificationError(
            "one exact runtime wheel is required per interpreter"
        )
    require_runtime_wheels(args.runtime_wheel, args.vane_version, args.signing_profile)
    extension_root = args.extension_root.resolve()
    vane_source = args.vane_source.resolve()
    builder._require_git_revision(vane_source, args.vane_revision, "Vane")
    builder._run(
        (
            sys.executable,
            "-I",
            str(extension_root / "vane-extension-ci-tools/scripts/vane_extension.py"),
            "--manifest",
            str(args.manifest.resolve()),
            "--extension-root",
            str(extension_root),
            "identity",
            "--vane-source",
            str(vane_source),
        )
    )
    # These two artifacts are downloaded independently by their original producer
    # IDs. Never use a copy of the unsigned data supplied by the signing job.
    artifact = require_signed_payload(
        args.unsigned_directory / "artifacts/vortex.duckdb_extension",
        args.signed_directory / "vortex.duckdb_extension",
    )
    licenses = require_licenses(args.unsigned_directory / "licenses/vortex")
    builder._require_no_undefined_duckdb_symbols(artifact)
    platform = builder._platform_tag()
    identity, _option = builder.SIGNING_PROFILES[args.signing_profile]
    output = args.output_directory.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise builder.QualificationError("packaging requires an empty output directory")
    with tempfile.TemporaryDirectory(
        prefix="vane-vortex-packaging-", dir=output.parent
    ) as value:
        staging = Path(value)
        emitted = []
        for index, (interpreter, runtime) in enumerate(
            zip(args.runtime_python, args.runtime_wheel, strict=True)
        ):
            python = builder._require_file(interpreter, "runtime interpreter")
            if not os.access(python, os.X_OK):
                raise builder.QualificationError(
                    "runtime interpreter must be executable"
                )
            runtime = builder._require_file(runtime, "indexed runtime wheel")
            environment, python = builder._builder_python(python, runtime, staging)
            try:
                directory = staging / str(index)
                directory.mkdir()
                wheel = builder._build_provider_wheel(
                    python=python,
                    vane_source=vane_source,
                    artifact=artifact.resolve(),
                    extension_name="vortex",
                    output_directory=directory,
                    platform_tag=platform,
                    trust_identity=identity,
                    license_files=(path.resolve() for path in licenses),
                )
                builder._run(
                    (
                        str(python),
                        "-I",
                        str(vane_source / "scripts/verify_extension_wheel.py"),
                        "--base-wheel",
                        str(runtime),
                        "--extension-wheel",
                        str(wheel),
                        "--extension-name",
                        "vortex",
                        "--trust-identity",
                        identity,
                    )
                )
                emitted.append(wheel)
            finally:
                environment.cleanup()
        if len({wheel.name for wheel in emitted}) != len(emitted):
            raise builder.QualificationError(
                "multiple runtime targets produced the same provider wheel"
            )
        for wheel in emitted:
            shutil.copyfile(wheel, output / wheel.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

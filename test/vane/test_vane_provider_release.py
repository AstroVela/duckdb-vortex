#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Exercise the shared release CLI with this repository's single-provider contract."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import tomllib
import unittest
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPOSITORY_ROOT / "vane-provider-release.toml"
VANE_VERSION = "0.2.0.dev612"
VERSIONS = {"vortex": "0.2.0.0.612.1"}
INTERPRETERS = ("cp310", "cp311", "cp312", "cp313", "cp314")
PLATFORM = "manylinux_2_28_x86_64"


def load_validator():
    path = REPOSITORY_ROOT / "vane-extension-ci-tools/scripts/vane_provider_release.py"
    specification = importlib.util.spec_from_file_location(
        "vane_vortex_release_validator", path
    )
    if specification is None or specification.loader is None:
        raise AssertionError(f"could not load {path}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def write_wheels(
    directory: Path, provider: str, *, vane_requirement: str | None = None
) -> list[Path]:
    distribution = f"vane_extension_{provider}"
    version = VERSIONS[provider]
    requirements = [vane_requirement or f"vane-ai==={VANE_VERSION}"]
    metadata = (
        "Metadata-Version: 2.4\n"
        f"Name: vane-extension-{provider}\n"
        f"Version: {version}\n"
        + "".join(f"Requires-Dist: {requirement}\n" for requirement in requirements)
        + "\n"
    )
    paths = []
    for interpreter in INTERPRETERS:
        path = directory / f"{distribution}-{version}-{interpreter}-none-{PLATFORM}.whl"
        with zipfile.ZipFile(path, "w") as wheel:
            wheel.writestr(f"{distribution}-{version}.dist-info/METADATA", metadata)
        paths.append(path)
    return paths


def source_arguments(directory: Path) -> list[str]:
    return [
        "--manifest",
        str(REPOSITORY_ROOT / "vane-extension.toml"),
        "--extension-root",
        str(REPOSITORY_ROOT),
        "--vane-source",
        str(directory / "vane"),
        "--ci-tools-version",
        "a" * 40,
        "--config",
        str(CONFIG_PATH),
        "--directory",
        str(directory),
    ]


class ProviderReleaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.validator = load_validator()
        cls.config = cls.validator.load_config(CONFIG_PATH)

    @classmethod
    def tearDownClass(cls) -> None:
        sys.modules.pop(cls.validator.__name__, None)

    def test_configured_matrix_and_dependency_graph(self) -> None:
        self.assertEqual(self.config.interpreters, INTERPRETERS)
        self.assertEqual(self.config.platforms, (PLATFORM,))
        self.assertEqual(self.config.max_wheel_bytes, 100000000)
        self.assertEqual(
            [
                (provider.name, provider.distribution, provider.dependencies)
                for provider in self.config.providers
            ],
            [("vortex", "vane-extension-vortex", ())],
        )

    def test_complete_release_cli_outputs(self) -> None:
        # Generic matrix/index edge cases live in the shared tools repository.
        # This smoke test uses the actual consumer config and workflow outputs.
        with tempfile.TemporaryDirectory(prefix="vane-vortex-release-") as value:
            directory = Path(value)
            for provider in VERSIONS:
                write_wheels(directory, provider)
            outputs = directory / "github-output"
            command = ["validate", *source_arguments(directory)]
            command += [
                "--vane-version",
                VANE_VERSION,
                "--github-output",
                str(outputs),
                "--channel",
                "testpypi-dev",
                "--require-publishable-on",
                "testpypi",
            ]
            output = io.StringIO()
            with (
                mock.patch.object(self.validator, "verify_sources") as verify,
                mock.patch.object(
                    self.validator, "_request_json", return_value=(404, None)
                ) as query,
                redirect_stdout(output),
            ):
                self.assertEqual(self.validator.main(command), 0)
            verify.assert_called_once_with(
                REPOSITORY_ROOT / "vane-extension.toml",
                REPOSITORY_ROOT,
                directory / "vane",
                "a" * 40,
            )
            self.assertEqual(query.call_count, 1)
            expected = {
                "vane_version": VANE_VERSION,
                **{f"{name}_version": version for name, version in VERSIONS.items()},
            }
            self.assertEqual(json.loads(output.getvalue()), expected)
            self.assertEqual(
                dict(line.split("=", 1) for line in outputs.read_text().splitlines()),
                expected,
            )

    def test_provider_requires_the_exact_vane_version(self) -> None:
        with tempfile.TemporaryDirectory(prefix="vane-vortex-dependency-") as value:
            directory = Path(value)
            for requirement in ("vane-ai>=0.2", "vane-ai===0.2.0.dev611"):
                with self.subTest(requirement=requirement):
                    write_wheels(directory, "vortex", vane_requirement=requirement)
                    command = [
                        "validate",
                        *source_arguments(directory),
                        "--vane-version",
                        VANE_VERSION,
                        "--channel",
                        "testpypi-dev",
                    ]
                    with (
                        mock.patch.object(self.validator, "verify_sources"),
                        redirect_stderr(io.StringIO()),
                    ):
                        self.assertEqual(self.validator.main(command), 2)

    def test_index_cli_accepts_each_assembled_provider_directory(self) -> None:
        # Verify the same complete artifact set that the upload job consumes.
        for provider, version in VERSIONS.items():
            with (
                self.subTest(provider=provider),
                tempfile.TemporaryDirectory(prefix="vane-vortex-index-") as value,
            ):
                directory = Path(value)
                paths = write_wheels(directory, provider)
                document = {
                    "urls": [
                        {
                            "filename": path.name,
                            "packagetype": "bdist_wheel",
                            "digests": {
                                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()
                            },
                            "yanked": False,
                        }
                        for path in paths
                    ]
                }
                command = ["verify-index", *source_arguments(directory)]
                command += [
                    "--index",
                    "testpypi",
                    "--provider",
                    provider,
                    "--version",
                    version,
                    "--attempts",
                    "1",
                    "--delay-seconds",
                    "0",
                ]
                with (
                    mock.patch.object(self.validator, "verify_sources") as verify,
                    mock.patch.object(
                        self.validator, "_request_json", return_value=(200, document)
                    ) as query,
                ):
                    self.assertEqual(self.validator.main(command), 0)
                verify.assert_called_once_with(
                    REPOSITORY_ROOT / "vane-extension.toml",
                    REPOSITORY_ROOT,
                    directory / "vane",
                    "a" * 40,
                )
                query.assert_called_once_with(
                    f"https://test.pypi.org/pypi/vane-extension-{provider}/{version}/json"
                )

    def test_integration_source_pins(self) -> None:
        manifest = tomllib.loads((REPOSITORY_ROOT / "vane-extension.toml").read_text())
        self.assertEqual(manifest["schema_version"], 2)
        self.assertEqual(manifest["vane"]["repository"], "AstroVela/vane")
        self.assertEqual(
            manifest["vane"]["revision"], "d1460a580455f01485e2e508e05d0049cb18a105"
        )
        self.assertIn(
            manifest["vane"]["revision"],
            (REPOSITORY_ROOT / "VANE_README.md").read_text(),
        )
        self.assertEqual(
            manifest["vcpkg"]["revision"], "74e6536215718009aae747d86d84b78376bf9e09"
        )
        entry = subprocess.check_output(
            [
                "git",
                "-C",
                str(REPOSITORY_ROOT),
                "ls-files",
                "--stage",
                "vane-extension-ci-tools",
            ],
            text=True,
        ).split()
        self.assertEqual(entry[0], "160000")
        actual = subprocess.check_output(
            [
                "git",
                "-C",
                str(REPOSITORY_ROOT / "vane-extension-ci-tools"),
                "rev-parse",
                "HEAD",
            ],
            text=True,
        ).strip()
        self.assertEqual(actual, entry[1])
        workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()
        integration = (
            REPOSITORY_ROOT / ".github/workflows/VaneIntegration.yml"
        ).read_text()
        self.assertIn(f"_vane_extension_ci.yml@{actual}", integration)
        self.assertIn(f"ci_tools_version: {actual}", integration)
        self.assertNotIn("scripts/validate_vane_provider_release.py", workflow)

    def test_shared_release_gates_and_explicit_publication(self) -> None:
        workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()
        self.assertEqual(workflow.count("scripts/vane_provider_release.py validate"), 3)
        self.assertEqual(
            workflow.count("scripts/vane_provider_release.py verify-index"), 2
        )
        self.assertIn("HEAD:vane-extension-ci-tools", workflow)
        self.assertIn("refs/heads/v1.5-variegata_vane", workflow)
        self.assertIn('test "$GITHUB_EVENT_NAME" = workflow_dispatch', workflow)
        self.assertIn("repository-url: https://test.pypi.org/legacy/", workflow)
        self.assertIn("skip-existing: true", workflow)
        self.assertEqual(workflow.count("--require-publishable-on testpypi"), 2)
        self.assertEqual(workflow.count("--require-publishable-on pypi"), 2)
        self.assertEqual(
            workflow.count("scripts/vane_provider_release.py verify-promotion"), 1
        )

    def test_rust_license_download_matches_the_pinned_manylinux_curl(self) -> None:
        workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()
        self.assertNotIn("--retry-all-errors", workflow)
        self.assertEqual(
            workflow.count("curl --fail --location --retry 5 --retry-delay 5"), 2
        )
        self.assertEqual(
            workflow.count(
                "9099a59e820c38a68b9d65f300662a567d56562f9a10f6aa4c7e86c17c2566af"
            ),
            2,
        )

    def test_artifact_downloads_require_digest_verification(self) -> None:
        download_action = (
            "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
        )
        for name, expected_downloads in (
            ("VaneExtension.yml", 13),
            ("VaneIntegration.yml", 2),
        ):
            workflow = (REPOSITORY_ROOT / ".github/workflows" / name).read_text()
            downloads = [
                step
                for step in workflow.split("\n      - ")
                if "\n        uses: actions/download-artifact@" in step
            ]
            with self.subTest(workflow=name):
                self.assertEqual(len(downloads), expected_downloads)
            for step in downloads:
                with self.subTest(workflow=name, step=step.splitlines()[0]):
                    self.assertIn(f"        uses: {download_action} # v8.0.1\n", step)
                    self.assertIn("\n          digest-mismatch: error\n", step)
                    self.assertNotIn("continue-on-error:", step)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Focused tests for Vortex's native provider build adapter."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shlex
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]


class DynamicWheelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = ROOT / "scripts/build_vane_dynamic_wheel.py"
        spec = importlib.util.spec_from_file_location("vortex_builder", path)
        cls.builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.builder)

    def test_vcpkg_comes_from_the_integration_manifest(self) -> None:
        self.assertEqual(
            self.builder._vcpkg_revision(ROOT),
            "74e6536215718009aae747d86d84b78376bf9e09",
        )
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            for manifest in (
                "schema_version = 1",
                'schema_version = 2\n[vcpkg]\nrepository = "fork/vcpkg"\nrevision = "'
                + "a" * 40
                + '"',
                'schema_version = 2\n[vcpkg]\nrepository = "microsoft/vcpkg"\nrevision = "main"',
            ):
                (root / "vane-extension.toml").write_text(manifest)
                with self.subTest(manifest=manifest), self.assertRaises(
                    self.builder.QualificationError
                ):
                    self.builder._vcpkg_revision(root)

    def test_build_environment_selects_only_dynamic_vortex(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            dependencies = root / "deps"
            for name in (
                "arrow/ArrowConfig.cmake",
                "arrowflight/ArrowFlightConfig.cmake",
            ):
                config = dependencies / "x64-linux/share" / name
                config.parent.mkdir(parents=True, exist_ok=True)
                config.touch()
            environment = {
                "PATH": os.environ["PATH"],
                "CMAKE_ARGS": "-DVORTEX_VANE_DISTRIBUTED=OFF",
                "DUCKDB_ICEBERG_DIRECTORY": "/unreviewed",
                "SKBUILD_BUILD_DIR": "/unreviewed",
                "SETUPTOOLS_SCM_PRETEND_VERSION": "9.9",
                "VANE_CMAKE_COMPILER_LAUNCHER": "ccache",
            }
            with mock.patch.dict(os.environ, environment, clear=True):
                result = self.builder._build_environment(
                    extension_root=ROOT,
                    build_directory=root,
                    vane_vcpkg_installed=dependencies,
                    vcpkg_toolchain=root / "vcpkg.cmake",
                    jobs=12,
                    signing_cmake_option="VANE_ENABLE_TEST_EXTENSION_SIGNING_KEY",
                )
            arguments = shlex.split(result["CMAKE_ARGS"])
            for argument in (
                "-DVANE_LOADABLE_EXTENSIONS=vortex",
                "-DEXTENSION_STATIC_BUILD=ON",
                "-DVORTEX_VANE_DISTRIBUTED=ON",
                "-DVORTEX_VANE_DYNAMIC_PROVIDER=ON",
                "-DUSE_SHARED_VORTEX=OFF",
                "-DENABLE_EXTENSION_AUTOLOADING=OFF",
                "-DENABLE_EXTENSION_AUTOINSTALL=OFF",
                "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",
            ):
                self.assertIn(argument, arguments)
            self.assertNotIn("DUCKDB_ICEBERG_DIRECTORY", result)
            self.assertNotIn("SETUPTOOLS_SCM_PRETEND_VERSION", result)
            self.assertEqual(result["DUCKDB_VORTEX_DIRECTORY"], str(ROOT))
            self.assertEqual(result["CMAKE_BUILD_PARALLEL_LEVEL"], "12")
            self.assertEqual(result["RUSTUP_TOOLCHAIN"], "1.97.1")
            self.assertEqual(result["CARGO_INCREMENTAL"], "0")

    def test_consumed_key_is_private_and_removed(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            path = Path(value) / "key.pem"
            path.write_bytes(b"test-key")
            path.chmod(0o600)
            contents = self.builder._read_signing_private_key(path, consume=True)
            self.assertEqual(contents, b"test-key")
            self.assertFalse(path.exists())
            self.builder._clear_key(contents)
            self.assertEqual(contents, bytearray())

    def test_unsafe_consumed_keys_are_not_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            path = root / "key.pem"
            path.write_bytes(b"test-key")
            path.chmod(0o644)
            link = root / "link.pem"
            link.symlink_to(path)
            for candidate in (path, link):
                with self.subTest(candidate=candidate), self.assertRaises(
                    self.builder.QualificationError
                ):
                    self.builder._read_signing_private_key(candidate, consume=True)
                self.assertEqual(path.read_bytes(), b"test-key")
            path.chmod(0o600)
            os.link(path, root / "hardlink.pem")
            with self.assertRaises(self.builder.QualificationError):
                self.builder._read_signing_private_key(path, consume=True)
            self.assertTrue(path.exists())

    def test_testpypi_cannot_package_an_unpublished_runtime(self) -> None:
        arguments = argparse.Namespace(
            jobs=12,
            package_local_runtime=True,
            runtime_python=[],
            runtime_wheel=[],
            signing_profile="testpypi",
            consume_signing_private_key=True,
        )
        with mock.patch.object(
            self.builder, "_parse_arguments", return_value=arguments
        ):
            with self.assertRaisesRegex(
                self.builder.QualificationError, "indexed runtimes"
            ):
                self.builder.main()

    def test_build_failure_consumes_and_clears_the_testpypi_key(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            key_path = root / "key.pem"
            key_path.write_bytes(b"test-key")
            key_path.chmod(0o600)
            arguments = argparse.Namespace(
                jobs=12,
                package_local_runtime=False,
                runtime_python=[Path("/runtime/python")],
                runtime_wheel=[Path("/runtime/vane.whl")],
                signing_profile="testpypi",
                consume_signing_private_key=True,
                signing_private_key=key_path,
                extension_root=ROOT,
                vane_source=root,
                vane_revision="a" * 40,
                vane_vcpkg_installed=root,
                vcpkg_toolchain=root / "toolchain.cmake",
                cargo_about=root / "cargo-about",
                build_directory=root / "build",
                output_directory=root / "dist",
            )
            contents = []
            read_key = self.builder._read_signing_private_key

            def consume_key(*args, **kwargs):
                result = read_key(*args, **kwargs)
                contents.append(result)
                return result

            def run(command, **kwargs):
                if "build" in command:
                    self.assertFalse(key_path.exists())
                    self.assertEqual(contents[0], b"test-key")
                    raise RuntimeError("simulated build failure")

            with (
                mock.patch.object(
                    self.builder, "_parse_arguments", return_value=arguments
                ),
                mock.patch.object(
                    self.builder,
                    "_require_vcpkg_toolchain",
                    return_value=root / "toolchain.cmake",
                ),
                mock.patch.object(self.builder, "_require_git_revision"),
                mock.patch.object(self.builder, "_require_rust_contract"),
                mock.patch.object(
                    self.builder,
                    "_require_file",
                    side_effect=lambda path, description: path,
                ),
                mock.patch.object(self.builder, "_capture", return_value="b" * 40),
                mock.patch.object(self.builder, "_build_environment", return_value={}),
                mock.patch.object(
                    self.builder, "_platform_tag", return_value="manylinux_2_28_x86_64"
                ),
                mock.patch.object(self.builder.os, "access", return_value=True),
                mock.patch.object(
                    self.builder, "_read_signing_private_key", side_effect=consume_key
                ),
                mock.patch.object(self.builder, "_run", side_effect=run),
                self.assertRaisesRegex(RuntimeError, "simulated build failure"),
            ):
                self.builder.main()
            self.assertEqual(contents, [bytearray()])
            self.assertFalse(key_path.exists())

    def test_toolchain_must_be_rooted_at_its_git_checkout(self) -> None:
        toolchain = Path("/repository/nested/scripts/buildsystems/vcpkg.cmake")
        with (
            mock.patch.object(self.builder, "_require_file", return_value=toolchain),
            mock.patch.object(self.builder, "_capture", return_value="/repository"),
            self.assertRaisesRegex(self.builder.QualificationError, "not rooted"),
        ):
            self.builder._require_vcpkg_toolchain(toolchain, "a" * 40)

    def test_artifact_rejects_external_libraries_and_duckdb_symbols(self) -> None:
        artifact = Path("/tmp/vortex.duckdb_extension")
        for dynamic, symbols in (
            ("Shared library: [libroaring.so]", ""),
            (
                "Shared library: [libutil.so.1]\nShared library: [libvortex.so]",
                "",
            ),
            ("(RPATH) [/tmp/build]", ""),
            ("(RUNPATH) [/tmp/build]", ""),
            ("Shared library: [libc.so.6]", "U duckdb::ClientContext::Something()"),
            ("Shared library: [libc.so.6]", "U duckdb_create_vector"),
        ):
            with self.subTest(dynamic=dynamic, symbols=symbols):
                with mock.patch.object(
                    self.builder,
                    "_capture",
                    side_effect=[
                        "ELF 64-bit stripped",
                        "vortex_duckdb_cpp_init T 0 1",
                        dynamic,
                        symbols,
                    ],
                ):
                    with self.assertRaises(self.builder.QualificationError):
                        self.builder._require_no_undefined_duckdb_symbols(artifact)

    def test_artifact_accepts_manylinux_glibc_libraries(self) -> None:
        artifact = Path("/tmp/vortex.duckdb_extension")
        for dynamic in (
            "Shared library: [libc.so.6]",
            "Shared library: [libc.so.6]\nShared library: [libutil.so.1]",
        ):
            with self.subTest(dynamic=dynamic), mock.patch.object(
                self.builder,
                "_capture",
                side_effect=[
                    "ELF 64-bit stripped",
                    "vortex_duckdb_cpp_init T 0 1",
                    dynamic,
                    "U forkpty@GLIBC_2.2.5",
                ],
            ):
                self.builder._require_no_undefined_duckdb_symbols(artifact)

    def test_export_and_strip_boundary(self) -> None:
        artifact = Path("/tmp/vortex.duckdb_extension")
        for outputs in (
            ["ELF 64-bit not stripped"],
            [
                "ELF 64-bit stripped",
                "vortex_duckdb_cpp_init T 0 1\nvortex_init_vane_rust T 2 1",
            ],
            ["ELF 64-bit stripped", ""],
        ):
            with self.subTest(outputs=outputs), mock.patch.object(
                self.builder, "_capture", side_effect=outputs
            ):
                with self.assertRaises(self.builder.QualificationError):
                    self.builder._require_no_undefined_duckdb_symbols(artifact)

    def test_reviewed_rust_and_native_dependency_contract(self) -> None:
        with mock.patch.object(
            self.builder, "_capture", return_value="release: 1.97.1"
        ):
            self.builder._require_rust_contract(ROOT)
        with mock.patch.object(
            self.builder, "_capture", return_value="release: 1.98.0"
        ):
            with self.assertRaisesRegex(self.builder.QualificationError, "rustc"):
                self.builder._require_rust_contract(ROOT)
        with mock.patch.object(
            self.builder.json, "loads", return_value={"dependencies": ["openssl"]}
        ):
            with mock.patch.object(
                self.builder, "_capture", return_value="release: 1.97.1"
            ):
                with self.assertRaisesRegex(
                    self.builder.QualificationError, "empty extension vcpkg"
                ):
                    self.builder._require_rust_contract(ROOT)
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            (root / "vortex-extension-vane").mkdir()
            for relative in (
                "rust-toolchain.toml",
                "vortex-extension-vane/Cargo.toml",
                "vortex-extension-vane/Cargo.lock",
            ):
                (root / relative).write_text((ROOT / relative).read_text())
            path = root / "vortex-extension-vane/Cargo.lock"
            path.write_text(
                path.read_text().replace(
                    self.builder.EXPECTED_VORTEX_REVISION, "a" * 40
                )
            )
            with self.assertRaisesRegex(self.builder.QualificationError, "Cargo.lock"):
                self.builder._require_rust_contract(root)

    def test_rust_notices_are_generated_from_the_exact_vane_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            stdlib = root / "share/doc/rust/COPYRIGHT-library.html"
            stdlib.parent.mkdir(parents=True)
            stdlib.write_text("Rust standard-library license")
            for name in ("LICENSE", "NOTICE"):
                (root / name).write_text(name)
            commands = []

            def run(command, **kwargs):
                commands.append((command, kwargs))
                Path(command[command.index("--output-file") + 1]).write_text(
                    "complete Rust dependency licenses"
                )

            with (
                mock.patch.object(
                    self.builder,
                    "_render_duckdb_license_bundle",
                    return_value="DuckDB notices",
                ),
                mock.patch.object(self.builder, "_capture", return_value=str(root)),
                mock.patch.object(self.builder, "_run", side_effect=run),
            ):
                files = self.builder._stage_license_files(
                    extension_root=ROOT,
                    vane_source=root,
                    build_directory=root / "build",
                    cargo_about=Path("/tools/cargo-about"),
                )
            self.assertEqual(len(files), 6)
            self.assertTrue(
                all(path.is_file() and path.stat().st_size > 0 for path in files)
            )
            command, kwargs = commands[0]
            self.assertEqual(command[0], "/tools/cargo-about")
            self.assertIn("--locked", command)
            self.assertIn("--fail", command)
            self.assertIn("--all-features", command)
            self.assertEqual(
                command[command.index("--manifest-path") + 1],
                str(ROOT / "vortex-extension-vane/Cargo.toml"),
            )
            self.assertEqual(kwargs["environment"]["RUSTUP_TOOLCHAIN"], "1.97.1")

    def test_license_collection_failure_does_not_silently_omit_rust(self) -> None:
        with tempfile.TemporaryDirectory() as value:
            root = Path(value)
            with (
                mock.patch.object(
                    self.builder,
                    "_render_duckdb_license_bundle",
                    return_value="DuckDB notices",
                ),
                mock.patch.object(self.builder, "_run"),
                self.assertRaisesRegex(self.builder.QualificationError, "cargo-about"),
            ):
                self.builder._stage_license_files(
                    extension_root=ROOT,
                    vane_source=root,
                    build_directory=root,
                    cargo_about=Path("/tools/cargo-about"),
                )


if __name__ == "__main__":
    unittest.main()

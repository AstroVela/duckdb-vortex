#!/usr/bin/env python3
"""Regress the native SourceID contract used by installed Vortex qualification."""

from __future__ import annotations

import importlib.util
import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SOURCE_TREE_ID = "edb8047859d9d5c443f46e86e6b5e507b5026944"
RUNTIME_SOURCE_ID = "edb8047859"
FORK_VERSION = "v1.5.5-vane.4bd8e72338"


def load_harness(filename: str) -> object:
    path = ROOT / "test/vane" / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RuntimeIdentityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.helpers = load_harness("vortex_wheel_test_support.py")
        cls.dynamic = load_harness("test_vane_dynamic_vortex.py")

    def setUp(self) -> None:
        self.vane = SimpleNamespace(
            __file__=str(Path(sys.prefix) / "lib/vane/__init__.py"),
            __git_revision__=RUNTIME_SOURCE_ID,
            __version__="0.2.0.dev662",
            runners=SimpleNamespace(
                get_or_create_runner=lambda: SimpleNamespace(name="ray")
            ),
        )
        self.connection = mock.Mock()
        self.connection.execute.return_value.fetchone.return_value = (
            FORK_VERSION,
            RUNTIME_SOURCE_ID,
        )

    def verify_identity(self, **kwargs: str) -> tuple[str, str]:
        expected = {
            "expected_fork_version": FORK_VERSION,
            "expected_source_id": SOURCE_TREE_ID,
            **kwargs,
        }
        return self.helpers.verify_duckdb_identity(
            self.vane, self.connection, **expected
        )

    def test_exact_native_source_id_matches_the_full_tree_pin(self) -> None:
        self.assertEqual(self.verify_identity(), (FORK_VERSION, RUNTIME_SOURCE_ID))

    def test_runtime_source_id_requires_exact_equality(self) -> None:
        for value in (
            SOURCE_TREE_ID,
            RUNTIME_SOURCE_ID[:-1],
            RUNTIME_SOURCE_ID + "0",
            "f" + RUNTIME_SOURCE_ID[1:],
            RUNTIME_SOURCE_ID.upper(),
            "",
        ):
            with self.subTest(source_id=value):
                self.connection.execute.return_value.fetchone.return_value = (
                    FORK_VERSION,
                    value,
                )
                with self.assertRaisesRegex(
                    AssertionError, "expected runtime SourceID"
                ):
                    self.verify_identity()

    def test_source_tree_pin_must_remain_full_and_canonical(self) -> None:
        for value in (RUNTIME_SOURCE_ID, SOURCE_TREE_ID.upper(), "g" * 40, ""):
            with self.subTest(source_tree_id=value):
                with self.assertRaisesRegex(AssertionError, "full lowercase SHA"):
                    self.verify_identity(expected_source_id=value)

    def test_python_and_sql_native_identities_must_agree(self) -> None:
        self.vane.__git_revision__ = "0" * 10
        with self.assertRaisesRegex(AssertionError, "installed Vane DuckDB SourceID"):
            self.verify_identity()

    def test_fork_version_must_match_exactly(self) -> None:
        with self.assertRaisesRegex(AssertionError, "fork version"):
            self.verify_identity(expected_fork_version="v1.5.5-vane.0000000000")

    def test_static_harness_accepts_the_native_runtime_identity(self) -> None:
        environment = {
            "VANE_EXPECTED_REVISION": "d1460a580455f01485e2e508e05d0049cb18a105",
            "VANE_EXPECTED_PACKAGE_VERSION": self.vane.__version__,
            "VANE_EXPECTED_FORK_VERSION": FORK_VERSION,
            "VANE_EXPECTED_DUCKDB_SOURCE_ID": SOURCE_TREE_ID,
            "VORTEX_EXPECTED_REVISION": "3da8a2848b5d10d028e69471c5c97bd3dc785a03",
            "VORTEX_EXPECTED_VERSION": "0.1.0",
            "VANE_WHEEL_SHA256": "a" * 64,
        }
        self.connection.execute.return_value.fetchone.side_effect = [
            (False,),
            (False,),
            ("STATICALLY_LINKED",),
            (False, "STATICALLY_LINKED", "0.1.0"),
            (True, "STATICALLY_LINKED", "0.1.0"),
            (FORK_VERSION, RUNTIME_SOURCE_ID),
        ]
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            redirect_stdout(io.StringIO()),
        ):
            identity = self.helpers.verify_installed_runtime(self.vane, self.connection)
        self.assertEqual(identity["source_id"], RUNTIME_SOURCE_ID)

    def test_dynamic_harness_checks_identity_before_provider_discovery(self) -> None:
        extensions = ModuleType("vane.extensions")
        extensions.LocalExtensionProvider = object
        with (
            mock.patch.dict(sys.modules, {"vane.extensions": extensions}),
            mock.patch.dict(
                os.environ,
                {
                    "VANE_EXPECTED_EXTENSION_TRUST_IDENTITY": "vane-ci-test-key",
                    "VANE_EXPECTED_PACKAGE_VERSION": self.vane.__version__,
                    "VANE_EXPECTED_FORK_VERSION": FORK_VERSION,
                    "VANE_EXPECTED_DUCKDB_SOURCE_ID": SOURCE_TREE_ID,
                },
            ),
            mock.patch.object(
                self.dynamic, "entry_points", return_value=[]
            ) as discover,
        ):
            # No Vane/native installation is needed: reaching discovery proves
            # the exact runtime identity passed the dynamic qualification gate.
            with self.assertRaisesRegex(
                AssertionError, "installed Vortex provider count"
            ):
                self.dynamic.load_dynamic_vortex(self.vane, self.connection)
            discover.assert_called_once_with(group="vane.dynamic_extension_providers")
            discover.reset_mock()
            self.connection.execute.return_value.fetchone.return_value = (
                FORK_VERSION,
                SOURCE_TREE_ID,
            )
            with self.assertRaisesRegex(AssertionError, "expected runtime SourceID"):
                self.dynamic.load_dynamic_vortex(self.vane, self.connection)
            discover.assert_not_called()

    def test_production_harness_uses_explicit_runtime_identity_without_dev_fallback(
        self,
    ) -> None:
        extensions = ModuleType("vane.extensions")
        extensions.LocalExtensionProvider = object
        self.vane.__version__ = "0.2.0rc1"
        source = "b" * 40
        fork = "v1.5.5-vane.aaaaaaaaaa"
        self.vane.__git_revision__ = source[:10]
        self.connection.execute.return_value.fetchone.return_value = (fork, source[:10])
        with (
            mock.patch.dict(sys.modules, {"vane.extensions": extensions}),
            mock.patch.dict(
                os.environ,
                {
                    "VANE_EXPECTED_EXTENSION_TRUST_IDENTITY": "astrovela/vane",
                    "VANE_EXPECTED_PACKAGE_VERSION": "0.2.0rc1",
                    "VANE_EXPECTED_FORK_VERSION": fork,
                    "VANE_EXPECTED_DUCKDB_SOURCE_ID": source,
                },
                clear=True,
            ),
            mock.patch.object(
                self.dynamic, "entry_points", return_value=[]
            ) as discover,
        ):
            with self.assertRaisesRegex(
                AssertionError, "installed Vortex provider count"
            ):
                self.dynamic.load_dynamic_vortex(self.vane, self.connection)
            discover.assert_called_once()
            discover.reset_mock()
            self.vane.__version__ = "0.2.0.dev612"
            with self.assertRaisesRegex(AssertionError, "exact provider runtime"):
                self.dynamic.load_dynamic_vortex(self.vane, self.connection)
            discover.assert_not_called()
            del os.environ["VANE_EXPECTED_PACKAGE_VERSION"]
            with self.assertRaises(KeyError):
                self.dynamic.load_dynamic_vortex(self.vane, self.connection)


if __name__ == "__main__":
    unittest.main()

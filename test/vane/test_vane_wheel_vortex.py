#!/usr/bin/env python3
"""Exercise statically linked Vortex from a clean installed Vane wheel."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

_HARNESS_ROOT = str(Path(__file__).resolve().parent)
sys.path.insert(0, _HARNESS_ROOT)
try:
    from vortex_wheel_test_support import (  # noqa: E402
        assert_exact_dataset,
        create_vortex_fixture,
        require_equal,
        require_true,
        scan_cases,
        sql_string,
        verify_installed_runtime,
        verify_known_case_results,
        verify_scan_schema,
        vortex_file_list,
    )
finally:
    sys.path.remove(_HARNESS_ROOT)

del _HARNESS_ROOT


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "local-fast":
        raise RuntimeError(
            "the local wheel qualification requires VANE_RUNNER=local-fast"
        )

    import vane

    connection = vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        verify_installed_runtime(vane, connection, "local-fast")
        with tempfile.TemporaryDirectory(prefix="vane-vortex-local-") as temporary:
            root = Path(temporary).resolve()
            files, empty_path = create_vortex_fixture(connection, root / "input")
            verify_scan_schema(connection, files, empty_path)
            verify_known_case_results(connection, files, empty_path)

            for description, query in scan_cases(files, empty_path):
                native_rows = connection.execute(query).fetchall()
                local_fast_rows = connection.sql(query).fetchall()
                require_equal(local_fast_rows, native_rows, f"local-fast {description}")

            repeated_query = (
                "SELECT id, payload FROM read_vortex("
                f"{vortex_file_list(files)}) WHERE id BETWEEN 41 AND 77 ORDER BY id"
            )
            repeated_relation = connection.sql(repeated_query)
            repeated_expected = connection.execute(repeated_query).fetchall()
            require_equal(
                repeated_relation.fetchall(),
                repeated_expected,
                "first prepared relation execution",
            )
            require_equal(
                repeated_relation.fetchall(),
                repeated_expected,
                "repeated prepared relation execution",
            )

            output_path = root / "local-copy.vortex"
            connection.sql(
                "SELECT id, part, payload, nullable_value "
                f"FROM read_vortex({vortex_file_list(files)})"
            ).write_file(str(output_path), format="vortex")
            require_true(
                output_path.is_file(),
                "local-fast Vortex COPY did not create its output",
            )
            assert_exact_dataset(
                connection, [output_path], "local-fast Vortex COPY readback"
            )

            empty_output = root / "local-empty-copy.vortex"
            connection.sql(
                "SELECT id, part, payload, nullable_value "
                f"FROM read_vortex({sql_string(empty_path)})"
            ).write_file(str(empty_output), format="vortex")
            require_true(
                empty_output.is_file(),
                "local-fast empty COPY did not create a Vortex file",
            )
            require_equal(
                connection.execute(
                    f"SELECT count(*)::BIGINT FROM read_vortex({sql_string(empty_output)})"
                ).fetchall(),
                [(0,)],
                "local-fast empty COPY readback",
            )
    finally:
        connection.close()


if __name__ == "__main__":
    main()

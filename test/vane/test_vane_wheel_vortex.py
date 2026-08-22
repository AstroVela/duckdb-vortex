#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Vortex contributors
# SPDX-License-Identifier: Apache-2.0

"""Exercise Vortex planning and reads through a packaged Vane wheel."""

from __future__ import annotations

import os
import tempfile
import uuid
from collections.abc import Callable
from pathlib import Path


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def run_scenario(description: str, operation: Callable[[], None]) -> None:
    print(f"[vane-local-vortex] START {description}", flush=True)
    operation()
    print(f"[vane-local-vortex] PASS  {description}", flush=True)


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def verify_extension_is_wheel_linked(connection: object) -> None:
    extension = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() " "WHERE extension_name = 'vortex'"
    ).fetchone()
    if extension is None:
        raise AssertionError("the packaged Vane wheel does not contain vortex")
    require_equal(extension[1], "STATICALLY_LINKED", "vortex install mode before LOAD")

    connection.execute("LOAD vortex")
    loaded = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() " "WHERE extension_name = 'vortex'"
    ).fetchone()
    require_equal(loaded, (True, "STATICALLY_LINKED"), "vortex after LOAD")


def split_batches(vane: object, connection: object, query: str) -> list[bytes]:
    plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        connection.sql(query),
        f"vortex-wheel-local-plan-{uuid.uuid4()}",
    ).to_physical_plan(connection)
    split_batch_map = dict(plan.scan_split_batch_map())
    require_equal(len(split_batch_map), 1, "physical Vortex scan count")
    return [bytes(batch) for batches in split_batch_map.values() for batch in batches]


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "local-fast":
        raise RuntimeError("the wheel integration test requires VANE_RUNNER=local-fast")

    import vane

    connection = vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        for setting in ("autoinstall_known_extensions", "autoload_known_extensions"):
            value = connection.execute(f"SELECT current_setting('{setting}')").fetchone()
            require_equal(str(value[0]).lower(), "false", f"{setting} setting")

        run_scenario(
            "statically linked extension identity",
            lambda: verify_extension_is_wheel_linked(connection),
        )

        with tempfile.TemporaryDirectory(prefix="vane-wheel-vortex-") as directory:
            root = Path(directory)
            files = [root / f"part-{index}.vortex" for index in range(3)]
            for index, path in enumerate(files):
                connection.execute(f"""
                    COPY (
                        SELECT
                            ({index * 10} + range)::BIGINT AS id,
                            {index}::INTEGER AS grp,
                            'row-' || ({index * 10} + range)::VARCHAR AS payload
                        FROM range(10)
                    ) TO {sql_string(path)} (FORMAT VORTEX)
                    """)

            source = "[" + ", ".join(sql_string(path) for path in files) + "]"
            query = f"""
                SELECT id, grp, strlen(payload) AS payload_len, file_index
                FROM read_vortex({source})
                WHERE id >= 7 AND id < 25
                ORDER BY id
            """

            def verify_bound_scan() -> None:
                expected = connection.execute(query).fetchall()
                require_equal(len(expected), 18, "filtered Vortex row count")
                batches = split_batches(vane, connection, query)
                require_equal(len(batches), len(files), "one split batch per Vortex file")
                require_equal(len(set(batches)), len(files), "unique Vortex split batches")
                require_equal(connection.sql(query).fetchall(), expected, "packaged Vortex scan")

            run_scenario("bound scan planning and projection/filter state", verify_bound_scan)

            def verify_absolute_glob() -> None:
                glob_query = f"""
                    SELECT id, file_index
                    FROM read_vortex({sql_string(root / 'part-*.vortex')})
                    WHERE id >= 7 AND id < 25
                    ORDER BY id
                """
                expected = connection.execute(glob_query).fetchall()
                require_equal(len(split_batches(vane, connection, glob_query)), 3, "absolute glob splits")
                require_equal(connection.sql(glob_query).fetchall(), expected, "absolute glob scan")

            run_scenario("absolute-path glob planning", verify_absolute_glob)

            def verify_empty_assignment() -> None:
                empty_query = f"""
                    SELECT id, file_index
                    FROM read_vortex({source})
                    WHERE file_index = 99
                """
                require_equal(len(split_batches(vane, connection, empty_query)), 1, "explicit empty split batch")
                require_equal(connection.sql(empty_query).fetchall(), [], "explicit empty scan")

            run_scenario("explicit empty split planning", verify_empty_assignment)
    finally:
        connection.close()


if __name__ == "__main__":
    main()

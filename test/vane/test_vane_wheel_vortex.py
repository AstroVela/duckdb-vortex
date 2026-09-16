#!/usr/bin/env python3
"""Exercise the statically linked Vortex wheel with Vane's default Ray runner."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

_harness_paths = [
    str(Path(__file__).resolve().parent),
    str(Path(__file__).resolve().parents[1] / "object_store"),
]
sys.path[:0] = _harness_paths
try:
    import test_vane_wheel_ray_vortex as ray_helpers
    import vortex_wheel_test_support as helpers
finally:
    for _path in _harness_paths:
        sys.path.remove(_path)
del _harness_paths, _path


def main() -> None:
    if "VANE_RUNNER" in os.environ:
        raise RuntimeError("leave VANE_RUNNER unset to qualify the default Ray runner")

    import ray
    import vane
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError("the wheel smoke must own its Ray cluster")
    cluster = ray_helpers.create_two_worker_cluster(ray)
    connection = None
    try:
        runner = runners.get_or_create_runner()
        helpers.require_equal(runner.name, "ray", "default runner")
        connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        helpers.verify_installed_runtime(vane, connection)
        with tempfile.TemporaryDirectory(prefix="vane-vortex-smoke-") as value:
            root = Path(value).resolve()
            files, empty_path = helpers.create_vortex_fixture(
                connection, root / "input"
            )
            helpers.verify_scan_schema(connection, files, empty_path)
            harness = ray_helpers.RayVortexHarness(vane, connection, runner)
            for description, query in helpers.scan_cases(files, empty_path):
                harness.require_query(
                    query, description, helpers.expected_scan_rows(description)
                )
            output = root / "copy"
            output.mkdir()
            receipt = harness.require_copy(
                "default Ray Vortex COPY",
                lambda: connection.sql(
                    "SELECT id, part, payload, nullable_value "
                    f"FROM read_vortex({helpers.vortex_file_list(files)})"
                ).write_file(str(output), format="vortex"),
                expected_rows=helpers.TOTAL_ROWS,
                minimum_files=1,
            )
            selected = [Path(entry["final_path"]) for entry in receipt["files"]]
            helpers.assert_exact_dataset(
                connection, selected, "default Ray COPY readback"
            )
            empty_output = root / "empty-copy"
            empty_output.mkdir()
            empty = harness.require_copy(
                "default Ray empty COPY",
                lambda: connection.sql(
                    f"SELECT * FROM read_vortex({helpers.sql_string(empty_path)})"
                ).write_file(str(empty_output), format="vortex"),
                expected_rows=0,
                minimum_files=1,
            )
            empty_files = [Path(entry["final_path"]) for entry in empty["files"]]
            harness.require_query(
                f"SELECT count(*) FROM read_vortex({helpers.vortex_file_list(empty_files)})",
                "default Ray empty COPY readback",
                [(0,)],
            )
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                vane.teardown_runner()
            finally:
                ray.shutdown()
                cluster.shutdown()


if __name__ == "__main__":
    main()

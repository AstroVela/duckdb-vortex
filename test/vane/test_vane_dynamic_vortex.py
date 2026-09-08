#!/usr/bin/env python3
"""Qualify the installed Vortex provider locally or on two actual Ray workers."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path

# These are driver-only fixtures and cluster/assertion helpers. Remove their
# search paths before importing Vane; no checkout path is sent to Ray workers.
_harness_paths = [
    str(Path(__file__).resolve().parent),
    str(Path(__file__).resolve().parents[1] / "object_store"),
]
sys.path[:0] = _harness_paths
try:
    import vortex_wheel_test_support as helpers
    import test_vane_wheel_ray_vortex as ray_helpers
finally:
    for _path in _harness_paths:
        sys.path.remove(_path)
del _harness_paths, _path

require_equal = helpers.require_equal
sql_string = helpers.sql_string


def require_installed_module(module: object) -> None:
    path = Path(module.__file__).resolve()
    if not path.is_relative_to(Path(sys.prefix).resolve()):
        raise AssertionError(
            f"module is not from the installed wheel environment: {path}"
        )


def load_dynamic_vortex(vane: object, connection: object) -> dict[str, object]:
    from vane.extensions import LocalExtensionProvider

    trust_identity = os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY")
    if trust_identity not in {"vane-ci-test-key", "astrovela/vane-testpypi"}:
        raise AssertionError(
            "VANE_EXPECTED_EXTENSION_TRUST_IDENTITY must select the explicit qualification trust root"
        )
    require_equal(str(vane.__version__), "0.2.0.dev612", "exact provider runtime")
    require_equal(
        connection.execute(
            "SELECT library_version, source_id FROM pragma_version()"
        ).fetchone(),
        ("v1.5.5-vane.4bd8e72338", "edb8047859d9d5c443f46e86e6b5e507b5026944"),
        "exact DuckDB fork and full SourceID",
    )
    require_installed_module(vane)
    matches = [
        candidate
        for candidate in entry_points(group="vane.dynamic_extension_providers")
        if candidate.name == "vortex"
    ]
    require_equal(len(matches), 1, "installed Vortex provider count")
    entry_point = matches[0]
    provider = entry_point.load()()
    if not isinstance(provider, LocalExtensionProvider):
        raise AssertionError(
            "the Vortex entry point must return LocalExtensionProvider"
        )
    module = import_module(entry_point.module)
    require_installed_module(module)
    descriptor = module.descriptor()
    require_equal(descriptor.name, "vortex", "provider extension name")
    require_equal(descriptor.trust_identity, trust_identity, "provider trust root")
    require_equal(descriptor.dependencies, (), "dynamic provider dependencies")
    require_equal(descriptor.vane_version, vane.__version__, "provider runtime version")
    artifact = provider.find(descriptor.identity)
    if artifact is None or artifact.descriptor != descriptor:
        raise AssertionError(
            "the installed provider does not own its exact Vortex descriptor"
        )
    digest = hashlib.sha256(descriptor.to_json().encode()).hexdigest()
    require_equal(
        entry_point.module,
        f"vane_extensions.vortex_{digest}",
        "packaged provider module",
    )

    security = connection.execute(
        "SELECT CAST(current_setting('allow_unsigned_extensions') AS BOOLEAN), "
        "CAST(current_setting('autoinstall_known_extensions') AS BOOLEAN), "
        "CAST(current_setting('autoload_known_extensions') AS BOOLEAN)"
    ).fetchone()
    require_equal(
        security, (False, False, False), "dynamic extension security settings"
    )
    query = "SELECT loaded, installed, install_mode FROM duckdb_extensions() WHERE extension_name = 'vortex'"
    state = connection.execute(query).fetchone()
    if state not in (None, (False, False, "NOT_INSTALLED")):
        raise AssertionError(
            f"Vortex must not be installed or statically linked before provider loading: {state!r}"
        )
    resolved = vane.load_installed_extension("vortex", connection=connection)
    require_equal(resolved.descriptor, descriptor, "loaded provider descriptor")
    require_equal(
        connection.execute(query).fetchone(),
        (True, False, "NOT_INSTALLED"),
        "dynamic Vortex load state",
    )
    manifest = [
        json.loads(entry)
        for entry in connection._export_dynamic_extension_snapshot_entries()
    ]
    require_equal(
        manifest, [descriptor.to_dict()], "trusted dynamic extension snapshot"
    )
    return descriptor.to_dict()


def provider_connection(vane: object) -> tuple[object, dict[str, object]]:
    connection = vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        connection.execute("LOAD httpfs")
        connection.execute("LOAD parquet")
        descriptor = load_dynamic_vortex(vane, connection)
        return connection, descriptor
    except BaseException:
        connection.close()
        raise


def exercise_local(vane: object, root: Path) -> None:
    connection, _descriptor = provider_connection(vane)
    try:
        files, empty_path = helpers.create_vortex_fixture(connection, root / "input")
        helpers.verify_scan_schema(connection, files, empty_path)
        helpers.verify_known_case_results(connection, files, empty_path)
        for description, query in helpers.scan_cases(files, empty_path):
            require_equal(
                connection.sql(query).fetchall(),
                connection.execute(query).fetchall(),
                f"local dynamic Vortex {description}",
            )
        output = root / "local-copy.vortex"
        connection.sql(
            "SELECT id, part, payload, nullable_value "
            f"FROM read_vortex({helpers.vortex_file_list(files)})"
        ).write_file(str(output), format="vortex")
        helpers.assert_exact_dataset(
            connection, [output], "local dynamic COPY readback"
        )
        empty_output = root / "local-empty.vortex"
        connection.sql(
            f"SELECT * FROM read_vortex({sql_string(empty_path)})"
        ).write_file(str(empty_output), format="vortex")
        require_equal(
            connection.execute(
                f"SELECT count(*) FROM read_vortex({sql_string(empty_output)})"
            ).fetchone(),
            (0,),
            "local empty dynamic COPY",
        )
    finally:
        connection.close()


def exercise_ray(vane: object, root: Path) -> None:
    import ray
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError("dynamic Vortex qualification must own its Ray cluster")
    cluster = ray_helpers.create_two_worker_cluster(ray)
    connection = None
    harness = None
    try:
        expected_nodes = ray_helpers.execution_node_ids(ray)
        connection, descriptor = provider_connection(vane)
        files, empty_path = helpers.create_vortex_fixture(connection, root / "input")
        vane.set_runner_ray(noop_if_initialized=True)
        runner = runners.get_or_create_runner()
        require_equal(getattr(runner, "name", None), "ray", "configured runner")
        harness = ray_helpers.RayVortexHarness(vane, connection, runner)
        source_query = (
            "SELECT id, part, payload, nullable_value "
            f"FROM read_vortex({helpers.vortex_file_list(files)})"
        )
        require_equal(
            harness.split_ids(source_query),
            ["0", "1", "2", "3"],
            "independent Vortex file splits",
        )
        plan = harness.physical_plan(source_query)
        require_equal(
            plan.__getstate__()[6]["dynamic_extensions"],
            [descriptor],
            "worker preparation manifest",
        )
        for description, query in helpers.scan_cases(files, empty_path):
            harness.require_query(query, f"dynamic provider {description}")

        # No UDF or unrelated source has run: both persistent workers must have
        # looked up and executed their Vortex scan fragments after preparation.
        ray_helpers.assert_vane_worker_topology(ray, runner, expected_nodes)
        output = root / "ray-copy"
        output.mkdir()
        result = harness.require_copy(
            "dynamic distributed Vortex COPY",
            lambda: connection.sql(source_query).write_file(
                str(output), format="vortex"
            ),
            expected_rows=helpers.TOTAL_ROWS,
            minimum_files=ray_helpers.WORKER_COUNT,
        )
        selected = [
            Path(str(entry["final_path"])).resolve() for entry in result["files"]
        ]
        helpers.assert_exact_dataset(
            connection, selected, "dynamic distributed COPY readback"
        )
        harness.require_query(
            "SELECT id, part, payload, nullable_value "
            f"FROM read_vortex({helpers.vortex_file_list(selected)}) ORDER BY id",
            "dynamic COPY Ray readback",
        )
        empty_output = root / "ray-empty-copy"
        empty_output.mkdir()
        empty = harness.require_copy(
            "dynamic empty distributed Vortex COPY",
            lambda: connection.sql(
                f"SELECT * FROM read_vortex({sql_string(empty_path)})"
            ).write_file(str(empty_output), format="vortex"),
            expected_rows=0,
            minimum_files=1,
        )
        empty_paths = [
            Path(str(entry["final_path"])).resolve() for entry in empty["files"]
        ]
        require_equal(
            connection.execute(
                f"SELECT count(*) FROM read_vortex({helpers.vortex_file_list(empty_paths)})"
            ).fetchone(),
            (0,),
            "dynamic empty distributed COPY readback",
        )
    finally:
        if harness is not None:
            harness.runner.run_iter_tables = harness._original_run_iter_tables
            harness.runner.run_write = harness._original_run_write
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                vane.teardown_runner()
            finally:
                try:
                    if ray.is_initialized():
                        ray.shutdown()
                finally:
                    cluster.shutdown()


def main() -> None:
    runner_kind = os.environ.get("VANE_RUNNER")
    if runner_kind not in {"local-fast", "ray"}:
        raise RuntimeError(
            "dynamic Vortex qualification requires VANE_RUNNER=local-fast or ray"
        )
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"
    import vane

    try:
        with tempfile.TemporaryDirectory(prefix="vane-dynamic-vortex-") as value:
            root = Path(value).resolve()
            if runner_kind == "local-fast":
                exercise_local(vane, root)
            else:
                exercise_ray(vane, root)
    finally:
        vane.teardown_runner()


if __name__ == "__main__":
    main()

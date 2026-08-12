# SPDX-FileCopyrightText: 2026 Vortex contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pyarrow as pa
import pytest
import ray
from ray.cluster_utils import Cluster

import vane
from vane import runners as vane_runners


def _sql_string(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _collect_tables(runner, relation, schema: pa.Schema) -> pa.Table:
    parts = list(runner.run_iter_tables(relation))
    if parts:
        table = pa.concat_tables(parts)
        # Vane's low-level Ray iterator intentionally exposes physical result
        # columns as c0, c1, ... (the same convention used by its own E2E
        # tests). Verify the physical types before restoring the relation's
        # logical names so schema comparisons remain meaningful.
        assert table.num_columns == len(schema)
        assert table.schema.types == schema.types
        return table.rename_columns(schema.names).replace_schema_metadata(schema.metadata)
    return pa.table([pa.array([], type=field.type) for field in schema], schema=schema)


def _descriptor_blobs(connection, sql: str) -> list[bytes]:
    logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        connection.sql(sql),
        f"vortex-ray-plan-{uuid.uuid4()}",
    )
    physical = logical.to_physical_plan(connection)
    descriptor_map = dict(physical.scan_task_descriptor_map())
    assert len(descriptor_map) == 1
    return [bytes(descriptor) for descriptors in descriptor_map.values() for descriptor in descriptors]


@pytest.fixture
def two_node_ray_runner(monkeypatch, tmp_path):
    if ray.is_initialized():
        ray.shutdown()

    package_parent = str(Path(vane.__file__).resolve().parent.parent)
    pythonpath = os.pathsep.join(dict.fromkeys([package_parent, os.environ.get("PYTHONPATH", "")])).strip(os.pathsep)
    env_vars = {
        "PYTHONPATH": pythonpath,
        "RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO": "0",
        "RAY_DEDUP_LOGS": "0",
        "VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION": "1",
        "VANE_RAY_SCAN_TASK_MIN_PARTITION_NUM": "4",
        "VANE_RAY_SCAN_TASK_SIZE_GROUPING": "0",
        "VANE_SHUFFLE_ALGORITHM": "flight_shuffle",
        "VANE_SHUFFLE_LOCAL_DIRS": str(tmp_path / "shuffle"),
    }
    for name, value in env_vars.items():
        monkeypatch.setenv(name, value)

    cluster = Cluster(shutdown_at_exit=False)
    try:
        cluster.add_node(
            include_dashboard=False,
            num_cpus=2,
            num_gpus=0,
            object_store_memory=256 * 1024 * 1024,
        )
        cluster.add_node(
            num_cpus=2,
            num_gpus=0,
            object_store_memory=256 * 1024 * 1024,
        )
        ray.init(
            address=cluster.address,
            ignore_reinit_error=True,
            log_to_driver=True,
            runtime_env={"env_vars": env_vars},
        )
        vane_runners.set_runner_ray(noop_if_initialized=True)
        runner = vane_runners.get_or_create_runner()
        assert runner.name == "ray"
        yield runner
    finally:
        try:
            vane.teardown_runner()
        finally:
            try:
                from vane.runners.ray import driver as ray_driver

                ray_driver.shutdown_background_event_loop()
            finally:
                ray.shutdown()
                cluster.shutdown()


def test_vortex_explicit_scan_runs_on_multiple_ray_workers(two_node_ray_runner, tmp_path):
    connection = vane.connect()
    try:
        rows_per_file = 1_000
        files = [tmp_path / f"part-{file_index}.vortex" for file_index in range(4)]
        for file_index, path in enumerate(files):
            start = file_index * rows_per_file
            connection.execute(
                f"""
                COPY (
                    SELECT
                        ({start} + range)::BIGINT AS id,
                        {file_index}::INTEGER AS grp,
                        'row-' || ({start} + range)::VARCHAR AS payload
                    FROM range({rows_per_file})
                ) TO {_sql_string(path)} (FORMAT VORTEX)
                """
            )

        source = "[" + ", ".join(_sql_string(path) for path in files) + "]"
        scan_sql = f"""
            SELECT id, grp, payload, file_index
            FROM read_vortex({source})
            WHERE id >= 750 AND id < 3250
        """
        expected = connection.execute(scan_sql).fetch_arrow_table().sort_by("id")
        assert expected.num_rows == 2_500
        assert expected.column("id").to_pylist()[0] == 750
        assert expected.column("id").to_pylist()[-1] == 3_249

        descriptors = _descriptor_blobs(connection, scan_sql)
        assert len(descriptors) == len(files)
        assert len(set(descriptors)) == len(files)

        actual = _collect_tables(two_node_ray_runner, connection.sql(scan_sql), expected.schema).sort_by("id")
        assert actual.schema == expected.schema
        assert actual.equals(expected)

        driver_client = two_node_ray_runner.query_driver_client
        assert driver_client is not None
        fragment_stats = ray.get(driver_client.runner.fragment_stats.remote())
        scan_workers = {
            worker_id: stats
            for worker_id, stats in fragment_stats["workers"].items()
            if stats.get("lookup_hits", 0) > 0
        }
        # Vane creates one persistent worker actor per node. A fragment lookup
        # happens only when that actor executes an FTE task, so this proves the
        # four distinct file descriptors were not all consumed by one worker.
        assert len(scan_workers) >= 2, fragment_stats
        scan_node_ids = {worker_id.rsplit(":", 2)[-2] for worker_id in scan_workers}
        assert len(scan_node_ids) >= 2, fragment_stats

        pruned_sql = f"""
            SELECT id, file_index
            FROM read_vortex({source})
            WHERE file_index = 2
              AND id >= {2 * rows_per_file + 100}
              AND id < {2 * rows_per_file + 200}
        """
        pruned_expected = connection.execute(pruned_sql).fetch_arrow_table().sort_by("id")
        assert pruned_expected.num_rows == 100
        assert len(_descriptor_blobs(connection, pruned_sql)) == 1
        pruned_actual = _collect_tables(
            two_node_ray_runner,
            connection.sql(pruned_sql),
            pruned_expected.schema,
        ).sort_by("id")
        assert pruned_actual.equals(pruned_expected)

        empty_task_sql = f"""
            SELECT id, file_index
            FROM read_vortex({source})
            WHERE file_index = 99
        """
        empty_task_expected = connection.execute(empty_task_sql).fetch_arrow_table()
        assert empty_task_expected.num_rows == 0
        # Vane transports a single legal descriptor whose extension task array
        # is empty after coordinator file-index pruning.
        assert len(_descriptor_blobs(connection, empty_task_sql)) == 1
        empty_task_actual = _collect_tables(
            two_node_ray_runner,
            connection.sql(empty_task_sql),
            empty_task_expected.schema,
        )
        assert empty_task_actual.schema == empty_task_expected.schema
        assert empty_task_actual.num_rows == 0

        aggregate_sql = f"""
            SELECT count(*) AS row_count, sum(id) AS id_sum,
                   min(id) AS min_id, max(grp) AS max_grp
            FROM read_vortex({source})
            WHERE id >= {rows_per_file * 3 // 4}
              AND id < {rows_per_file * 13 // 4}
        """
        expected_aggregate = connection.execute(aggregate_sql).fetch_arrow_table()
        actual_aggregate = _collect_tables(
            two_node_ray_runner,
            connection.sql(aggregate_sql),
            expected_aggregate.schema,
        )
        assert actual_aggregate.schema == expected_aggregate.schema
        assert actual_aggregate.equals(expected_aggregate)

        # All of these aggregates are supported by Vortex's aggregate
        # projection pushdown, so their bind state must survive plan cloning.
        pushed_aggregate_sql = f"""
            SELECT min(id) AS min_id, max(id) AS max_id, count(id) AS id_count
            FROM read_vortex({source})
            WHERE id >= {rows_per_file * 3 // 4}
              AND id < {rows_per_file * 13 // 4}
        """
        pushed_aggregate_expected = connection.execute(pushed_aggregate_sql).fetch_arrow_table()
        pushed_aggregate_actual = _collect_tables(
            two_node_ray_runner,
            connection.sql(pushed_aggregate_sql),
            pushed_aggregate_expected.schema,
        )
        assert pushed_aggregate_actual.schema == pushed_aggregate_expected.schema
        assert pushed_aggregate_actual.equals(pushed_aggregate_expected)

        repeated = _collect_tables(two_node_ray_runner, connection.sql(scan_sql), expected.schema).sort_by("id")
        assert repeated.equals(expected)

        empty_file = tmp_path / "empty.vortex"
        connection.execute(
            f"""
            COPY (
                SELECT range::BIGINT AS id, ''::VARCHAR AS payload
                FROM range(0)
            ) TO {_sql_string(empty_file)} (FORMAT VORTEX)
            """
        )
        empty_sql = f"SELECT id FROM read_vortex({_sql_string(empty_file)})"
        empty_expected = connection.execute(empty_sql).fetch_arrow_table()
        assert empty_expected.num_rows == 0
        assert len(_descriptor_blobs(connection, empty_sql)) == 1
        empty_actual = _collect_tables(
            two_node_ray_runner,
            connection.sql(empty_sql),
            empty_expected.schema,
        )
        assert empty_actual.schema == empty_expected.schema
        assert empty_actual.num_rows == 0
    finally:
        connection.close()

# SPDX-FileCopyrightText: 2026 Vortex contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pytest
import ray
from ray.cluster_utils import Cluster

import vane
from vane import runners as vane_runners

pytestmark = [pytest.mark.real_ray, pytest.mark.ray_cluster_owner]


def _sql_string(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _collect_tables(runner: object, relation: object, schema: pa.Schema) -> pa.Table:
    assert vane_runners.get_or_create_runner().name == runner.name == "ray"
    table = relation.to_arrow_table()
    # The public distributed-result path normalizes Arrow offset widths (for
    # example large_string to string) and restores the logical column names.
    assert table.schema == schema
    return table


def _split_batch_blobs(connection: object, sql: str) -> list[bytes]:
    logical = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        connection.sql(sql),
        f"vortex-ray-plan-{uuid.uuid4()}",
    )
    physical = logical.to_physical_plan(connection)
    split_batch_map = dict(physical.scan_split_batch_map())
    assert len(split_batch_map) == 1
    return [bytes(batch) for batches in split_batch_map.values() for batch in batches]


@dataclass
class RayVortexHarness:
    runner: object
    connection: object
    root: Path
    files: list[Path]
    source: str
    rows_per_file: int

    def require_query(self, sql: str, *, sort_by: str | None = None) -> pa.Table:
        expected = self.connection.execute(sql).to_arrow_table()
        actual = _collect_tables(
            self.runner,
            self.connection.sql(sql),
            expected.schema,
        )
        if sort_by is not None:
            expected = expected.sort_by(sort_by)
            actual = actual.sort_by(sort_by)
        assert actual.schema == expected.schema
        assert actual.equals(expected)
        return expected

    def base_scan_sql(self) -> str:
        return f"""
            SELECT id, grp, payload, file_index
            FROM read_vortex({self.source})
            WHERE id >= 750 AND id < 3250
        """


@pytest.fixture(scope="module")
def ray_vortex_harness(tmp_path_factory: pytest.TempPathFactory):
    if ray.is_initialized():
        ray.shutdown()

    root = tmp_path_factory.mktemp("vortex-ray")
    environment = pytest.MonkeyPatch()
    package_parent = str(Path(vane.__file__).resolve().parent.parent)
    pythonpath = os.pathsep.join(dict.fromkeys([package_parent, os.environ.get("PYTHONPATH", "")])).strip(os.pathsep)
    env_vars = {
        "PYTHONPATH": pythonpath,
        "RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO": "0",
        "RAY_DEDUP_LOGS": "0",
        "VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION": "1",
        "VANE_RAY_SCAN_SPLIT_MIN_COUNT": "4",
        "VANE_SHUFFLE_ALGORITHM": "flight_shuffle",
        "VANE_SHUFFLE_LOCAL_DIRS": str(root / "shuffle"),
    }
    for name, value in env_vars.items():
        environment.setenv(name, value)

    cluster = None
    connection = None
    try:
        cluster = Cluster(shutdown_at_exit=False)
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

        connection = vane.connect()
        rows_per_file = 1_000
        files = [root / f"part-{file_index}.vortex" for file_index in range(4)]
        for file_index, path in enumerate(files):
            start = file_index * rows_per_file
            connection.execute(f"""
                COPY (
                    SELECT
                        ({start} + range)::BIGINT AS id,
                        {file_index}::INTEGER AS grp,
                        'row-' || ({start} + range)::VARCHAR AS payload
                    FROM range({rows_per_file})
                ) TO {_sql_string(path)} (FORMAT VORTEX)
                """)

        source = "[" + ", ".join(_sql_string(path) for path in files) + "]"
        yield RayVortexHarness(
            runner=runner,
            connection=connection,
            root=root,
            files=files,
            source=source,
            rows_per_file=rows_per_file,
        )
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                vane.teardown_runner()
            finally:
                try:
                    from vane.runners.ray import driver as ray_driver

                    ray_driver.shutdown_background_event_loop()
                finally:
                    try:
                        if ray.is_initialized():
                            ray.shutdown()
                    finally:
                        try:
                            if cluster is not None:
                                cluster.shutdown()
                        finally:
                            environment.undo()


def test_explicit_splits_run_on_multiple_ray_workers(
    ray_vortex_harness: RayVortexHarness,
):
    harness = ray_vortex_harness
    scan_sql = harness.base_scan_sql()
    split_batches = _split_batch_blobs(harness.connection, scan_sql)
    assert len(split_batches) == len(harness.files)
    assert len(set(split_batches)) == len(harness.files)

    # Initialize the lazily-created query driver with a scan-free query, then
    # compare per-worker counters around this Vortex scan. Using deltas keeps
    # the topology assertion independent of test ordering and prior queries.
    if harness.runner.query_driver_client is None:
        harness.require_query("SELECT 1 AS ready")
    driver_client = harness.runner.query_driver_client
    assert driver_client is not None
    before_stats = ray.get(driver_client.runner.fragment_stats.remote())
    before_hits = {worker_id: int(stats.get("lookup_hits", 0)) for worker_id, stats in before_stats["workers"].items()}

    expected = harness.require_query(scan_sql, sort_by="id")
    assert expected.num_rows == 2_500
    assert expected.column("id").to_pylist()[0] == 750
    assert expected.column("id").to_pylist()[-1] == 3_249

    fragment_stats = ray.get(driver_client.runner.fragment_stats.remote())
    scan_workers = {
        worker_id: stats
        for worker_id, stats in fragment_stats["workers"].items()
        if int(stats.get("lookup_hits", 0)) > before_hits.get(worker_id, 0)
    }
    # Vane creates one persistent worker actor per node. A fragment lookup
    # happens only when that actor executes an FTE task, so this proves the
    # four distinct file splits were not all consumed by one worker.
    assert len(scan_workers) >= 2, fragment_stats
    scan_node_ids = {worker_id.rsplit(":", 2)[-2] for worker_id in scan_workers}
    assert len(scan_node_ids) >= 2, fragment_stats


def test_absolute_glob_has_stable_distributed_splits(
    ray_vortex_harness: RayVortexHarness,
):
    harness = ray_vortex_harness
    glob_sql = f"""
        SELECT id, file_index
        FROM read_vortex({_sql_string(harness.root / 'part-*.vortex')})
        WHERE id >= 750 AND id < 3250
    """
    list_sql = f"""
        SELECT id, file_index
        FROM read_vortex({harness.source})
        WHERE id >= 750 AND id < 3250
    """
    split_batches = _split_batch_blobs(harness.connection, glob_sql)
    assert len(split_batches) == len(harness.files)
    assert len(set(split_batches)) == len(harness.files)
    glob_result = harness.require_query(glob_sql, sort_by="id")
    list_result = harness.connection.execute(list_sql).to_arrow_table().sort_by("id")
    assert glob_result.equals(list_result)


def test_file_index_predicates_prune_distributed_splits(
    ray_vortex_harness: RayVortexHarness,
):
    harness = ray_vortex_harness
    pruned_sql = f"""
        SELECT id, file_index
        FROM read_vortex({harness.source})
        WHERE file_index = 2
          AND id >= {2 * harness.rows_per_file + 100}
          AND id < {2 * harness.rows_per_file + 200}
    """
    pruned_expected = harness.require_query(pruned_sql, sort_by="id")
    assert pruned_expected.num_rows == 100
    assert len(_split_batch_blobs(harness.connection, pruned_sql)) == 1

    not_equal_sql = f"""
        SELECT id, file_index
        FROM read_vortex({harness.source})
        WHERE file_index != 1
    """
    not_equal_expected = harness.require_query(not_equal_sql, sort_by="id")
    assert not_equal_expected.num_rows == harness.rows_per_file * 3
    assert len(_split_batch_blobs(harness.connection, not_equal_sql)) == 3


def test_projection_and_filter_state_survives_worker_plan_cloning(
    ray_vortex_harness: RayVortexHarness,
):
    harness = ray_vortex_harness
    # These operations live only in Vortex's portable bind after optimizer
    # pushdown. They must deserialize on Ray workers without re-binding the
    # original glob or losing the custom scalar-function registry.
    expression_sql = f"""
        SELECT id, strlen(payload) AS payload_len, file_index
        FROM read_vortex({harness.source})
        WHERE payload LIKE '%25%'
    """
    expression_expected = harness.require_query(expression_sql, sort_by="id")
    assert expression_expected.num_rows > 0

    # Vortex only claims native pushdown for file_index. Other virtual
    # predicates remain as DuckDB physical filters above the distributed scan.
    row_number_sql = f"""
        SELECT id, file_row_number
        FROM read_vortex({harness.source})
        WHERE file_row_number != 0
    """
    row_number_expected = harness.require_query(row_number_sql, sort_by="id")
    assert row_number_expected.num_rows == (harness.rows_per_file - 1) * len(harness.files)


def test_empty_splits_aggregates_and_files_are_legal(
    ray_vortex_harness: RayVortexHarness,
):
    harness = ray_vortex_harness
    empty_split_sql = f"""
        SELECT id, file_index
        FROM read_vortex({harness.source})
        WHERE file_index = 99
    """
    empty_split_expected = harness.require_query(empty_split_sql)
    assert empty_split_expected.num_rows == 0
    # Vane transports one legal explicit empty split after coordinator pruning.
    assert len(_split_batch_blobs(harness.connection, empty_split_sql)) == 1

    empty_aggregate_sql = f"""
        SELECT min(id) AS min_id, max(id) AS max_id, count(id) AS id_count
        FROM read_vortex({harness.source})
        WHERE file_index = 99
    """
    empty_aggregate_expected = harness.require_query(empty_aggregate_sql)
    assert empty_aggregate_expected.num_rows == 1
    assert empty_aggregate_expected.column("min_id").null_count == 1
    assert empty_aggregate_expected.column("max_id").null_count == 1
    assert empty_aggregate_expected.column("id_count").to_pylist() == [0]

    empty_file = harness.root / "empty.vortex"
    harness.connection.execute(f"""
        COPY (
            SELECT range::BIGINT AS id, ''::VARCHAR AS payload
            FROM range(0)
        ) TO {_sql_string(empty_file)} (FORMAT VORTEX)
        """)
    empty_file_sql = f"SELECT id FROM read_vortex({_sql_string(empty_file)})"
    empty_file_expected = harness.require_query(empty_file_sql)
    assert empty_file_expected.num_rows == 0
    assert len(_split_batch_blobs(harness.connection, empty_file_sql)) == 1


def test_aggregate_state_survives_distributed_execution(
    ray_vortex_harness: RayVortexHarness,
):
    harness = ray_vortex_harness
    aggregate_sql = f"""
        SELECT count(*) AS row_count, CAST(sum(id) AS BIGINT) AS id_sum,
               min(id) AS min_id, max(grp) AS max_grp
        FROM read_vortex({harness.source})
        WHERE id >= {harness.rows_per_file * 3 // 4}
          AND id < {harness.rows_per_file * 13 // 4}
    """
    harness.require_query(aggregate_sql)

    # With no DuckDB table filters these aggregates are pushed into the Vortex
    # scan, so their reader bind state must survive physical-plan cloning.
    pushed_aggregate_sql = f"""
        SELECT min(id) AS min_id, max(id) AS max_id, count(id) AS id_count
        FROM read_vortex({harness.source})
    """
    harness.require_query(pushed_aggregate_sql)


def test_topn_dynamic_filters_and_repeated_execution_are_stable(
    ray_vortex_harness: RayVortexHarness,
):
    harness = ray_vortex_harness
    # Vane assigns explicit splits independently per physical scan. The
    # distributed-capable function disables DuckDB's two-scan late-
    # materialization rewrite until grouped multi-scan assignments exist.
    late_materialization_sql = f"""
        SELECT id, grp, payload, file_index
        FROM read_vortex({harness.source})
        ORDER BY grp DESC, id DESC
        LIMIT 7
    """
    explain_rows = harness.connection.execute(f"EXPLAIN {late_materialization_sql}").fetchall()
    explain_text = "\n".join(str(value) for row in explain_rows for value in row)
    assert explain_text.count("READ_VORTEX") == 1, explain_text
    harness.require_query(late_materialization_sql, sort_by="id")

    # The required grp predicate and TopN's optional dynamic filter share a
    # conjunction. Ignoring the detached runtime hint must retain its sibling.
    filtered_topn_sql = f"""
        SELECT id, grp, payload, file_index
        FROM read_vortex({harness.source})
        WHERE grp >= 2
        ORDER BY grp ASC, id ASC
        LIMIT 7
    """
    filtered_topn_expected = harness.require_query(filtered_topn_sql, sort_by="id")
    assert filtered_topn_expected.column("grp").to_pylist() == [2] * 7

    first = harness.require_query(harness.base_scan_sql(), sort_by="id")
    repeated = harness.require_query(harness.base_scan_sql(), sort_by="id")
    assert repeated.equals(first)

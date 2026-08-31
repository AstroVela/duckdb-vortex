#!/usr/bin/env python3
"""Qualify packaged Vortex scans and COPY on a two-node Vane Ray runtime."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
import uuid
from collections.abc import Callable
from pathlib import Path

_HARNESS_ROOT = str(Path(__file__).resolve().parent)
sys.path.insert(0, _HARNESS_ROOT)
try:
    from vortex_wheel_test_support import (  # noqa: E402
        TOTAL_ROWS,
        assert_exact_dataset,
        create_vortex_fixture,
        require_equal,
        require_error,
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

WORKER_COUNT = 2


def create_two_worker_cluster(ray: object) -> object:
    from ray.cluster_utils import Cluster

    cluster = Cluster(shutdown_at_exit=False)
    try:
        cluster.add_node(
            include_dashboard=False,
            num_cpus=0,
            num_gpus=0,
            object_store_memory=100 * 1024 * 1024,
        )
        for _ in range(WORKER_COUNT):
            cluster.add_node(
                include_dashboard=False,
                num_cpus=1,
                num_gpus=0,
                object_store_memory=100 * 1024 * 1024,
            )
        ray.init(address=cluster.address, ignore_reinit_error=False, log_to_driver=True)
        return cluster
    except BaseException:
        cluster.shutdown()
        raise


def execution_node_ids(ray: object) -> set[str]:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        node_ids = {
            str(node["NodeID"])
            for node in ray.nodes()
            if node.get("Alive")
            and float((node.get("Resources") or {}).get("CPU", 0)) >= 1
        }
        if len(node_ids) == WORKER_COUNT:
            return node_ids
        time.sleep(0.25)
    raise AssertionError(f"expected {WORKER_COUNT} live Ray execution nodes")


def assert_vane_worker_topology(
    ray: object, runner: object, expected_nodes: set[str]
) -> None:
    client = runner.query_driver_client
    if client is None:
        raise AssertionError("the Ray runner did not create a query driver client")
    stats = ray.get(client.runner.fragment_stats.remote())
    workers = stats.get("workers") if isinstance(stats, dict) else None
    if not isinstance(workers, dict):
        raise AssertionError(
            f"Vane fragment statistics do not expose workers: {stats!r}"
        )
    require_equal(len(workers), WORKER_COUNT, "Vane Ray worker count")
    workers_by_node: dict[str, dict[str, object]] = {}
    for worker_id, worker_stats in workers.items():
        matching_nodes = [
            node_id for node_id in expected_nodes if f":{node_id}:" in f":{worker_id}:"
        ]
        require_equal(
            len(matching_nodes),
            1,
            f"Vane worker {worker_id!r} execution-node identity",
        )
        if not isinstance(worker_stats, dict):
            raise AssertionError(
                f"Vane worker {worker_id!r} statistics are invalid: {worker_stats!r}"
            )
        workers_by_node[matching_nodes[0]] = worker_stats
    require_equal(
        set(workers_by_node), expected_nodes, "Vane persistent worker node coverage"
    )
    for node_id, worker_stats in workers_by_node.items():
        require_true(
            int(worker_stats.get("registered_total", 0)) > 0,
            f"Vane worker on {node_id} registered no Vortex fragments",
        )
        require_true(
            int(worker_stats.get("lookup_hits", 0)) > 0,
            f"Vane worker on {node_id} executed no registered Vortex fragments",
        )


class AnnotateWorkerNode:
    """Record the installed Vane package and Ray node that consume each batch."""

    def __call__(self, table: object) -> object:
        import pyarrow as pa
        import ray
        import vane

        module_path = Path(vane.__file__).resolve()
        prefix = Path(sys.prefix).resolve()
        try:
            module_path.relative_to(prefix)
        except ValueError as error:
            raise RuntimeError(
                f"Ray worker did not import Vane from its wheel environment: {module_path}"
            ) from error
        time.sleep(0.05)
        node_id = str(ray.get_runtime_context().get_node_id())
        return pa.table(
            {
                "id": table.column("id"),
                "worker_node_id": [node_id] * table.num_rows,
                "vane_module": [str(module_path)] * table.num_rows,
            }
        )


class FailSelectedVortexWorker:
    """Fail one COPY-plan source task after peer tasks have time to run."""

    def __call__(self, table: object) -> object:
        import pyarrow.compute as pc

        if table.num_rows and bool(pc.any(pc.equal(table.column("id"), 95)).as_py()):
            time.sleep(0.75)
            raise RuntimeError("intentional distributed Vortex COPY task failure")
        return table


class RayVortexHarness:
    def __init__(self, vane: object, connection: object, runner: object):
        self.vane = vane
        self.connection = connection
        self.runner = runner
        self.read_dispatch_count = 0
        self.write_dispatch_count = 0
        self.last_write_result: dict[str, object] | None = None
        self._original_run_iter_tables = runner.run_iter_tables
        self._original_run_write = runner.run_write

        def record_distributed_read(*args: object, **kwargs: object) -> object:
            self.read_dispatch_count += 1
            return self._original_run_iter_tables(*args, **kwargs)

        def record_distributed_write(*args: object, **kwargs: object) -> object:
            self.write_dispatch_count += 1
            result = self._original_run_write(*args, **kwargs)
            self.last_write_result = result
            return result

        runner.run_iter_tables = record_distributed_read
        runner.run_write = record_distributed_write

    def require_query(self, query: str, description: str) -> list[tuple[object, ...]]:
        native_rows = self.connection.execute(query).fetchall()
        previous_count = self.read_dispatch_count
        distributed_rows = self.connection.sql(query).fetchall()
        require_equal(
            self.read_dispatch_count, previous_count + 1, f"{description} Ray dispatch"
        )
        require_equal(distributed_rows, native_rows, f"{description} native comparison")
        return distributed_rows

    def physical_plan(self, query: str) -> object:
        relation = self.connection.sql(query)
        return self.vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            f"vane-wheel-ray-vortex-plan-{uuid.uuid4().hex}",
        ).to_physical_plan(self.connection)

    def split_ids(self, query: str) -> list[str]:
        plan = self.physical_plan(query)
        return sorted(
            split_id
            for batches in plan.scan_split_batch_map().values()
            for batch in batches
            for split_id, _singleton_batch, _estimated_bytes in self.vane.ray_cxx.split_scan_split_batch(
                batch
            )
        )

    def require_copy(
        self,
        description: str,
        operation: Callable[[], object],
        expected_rows: int,
        minimum_files: int,
    ) -> dict[str, object]:
        previous_count = self.write_dispatch_count
        self.last_write_result = None
        operation()
        require_equal(
            self.write_dispatch_count, previous_count + 1, f"{description} Ray dispatch"
        )
        if self.last_write_result is None:
            raise AssertionError(
                f"{description}: Vane returned no distributed COPY result"
            )
        result = self.last_write_result
        require_equal(
            result.get("copy_output_committed"), True, f"{description} committed marker"
        )
        require_equal(
            result.get("copy_output_direct_write"),
            True,
            f"{description} direct-write mode",
        )
        require_equal(
            result.get("rows_copied"), expected_rows, f"{description} row count"
        )
        files = result.get("files")
        if not isinstance(files, list):
            raise AssertionError(f"{description}: COPY files are not a list: {files!r}")
        require_true(
            len(files) >= minimum_files, f"{description} selected too few worker files"
        )
        require_equal(
            result.get("copy_selected_file_count"),
            len(files),
            f"{description} selected file count",
        )
        require_equal(
            result.get("copy_duplicate_file_count"),
            0,
            f"{description} duplicate file count",
        )
        run_id = str(result.get("copy_output_run_id") or "")
        require_true(bool(run_id), f"{description} has no run id")
        manifest_path = Path(str(result.get("copy_output_manifest_path") or ""))
        marker_path = Path(str(result.get("copy_output_committed_marker_path") or ""))
        require_true(manifest_path.is_file(), f"{description} manifest does not exist")
        require_true(
            marker_path.is_file(), f"{description} committed marker does not exist"
        )
        selected_paths = sorted(
            Path(str(entry["final_path"])).resolve() for entry in files
        )
        require_equal(
            len(set(selected_paths)),
            len(selected_paths),
            f"{description} unique selected paths",
        )
        require_true(
            all(path.is_file() for path in selected_paths),
            f"{description} selected file is missing",
        )
        require_true(
            all(path.name.startswith(f"{run_id}_") for path in selected_paths),
            f"{description} selected path does not carry the run identity",
        )

        committed = self.vane.ray_cxx.read_committed_copy_direct_write_result(
            str(result.get("copy_output_base_path") or ""),
            run_id,
            self.connection,
        )
        committed_paths = sorted(
            Path(str(entry["final_path"])).resolve() for entry in committed["files"]
        )
        require_equal(
            committed.get("rows_copied"),
            expected_rows,
            f"{description} manifest row count",
        )
        require_equal(
            committed_paths, selected_paths, f"{description} selected-attempt manifest"
        )
        return result

    def require_copy_failure(
        self,
        description: str,
        operation: Callable[[], object],
        expected_message: str,
    ) -> None:
        previous_count = self.write_dispatch_count
        self.last_write_result = None
        require_error(description, operation, expected_message)
        require_equal(
            self.write_dispatch_count, previous_count + 1, f"{description} Ray dispatch"
        )
        require_equal(
            self.last_write_result,
            None,
            f"{description} did not return a committed result",
        )


class _NativeTaskCaptureBackend:
    """Capture tasks produced by Vane's real distributed plan translator."""

    def __init__(self, query_id: str) -> None:
        self.query_id = query_id
        self.tasks: list[object] = []
        self.exhausted_source_ids: set[str] = set()

    def register_query_owner(self, query_id: str, owner_query_id: str) -> None:
        require_equal(query_id, self.query_id, "captured task execution query")
        require_equal(owner_query_id, self.query_id, "captured task resource query")

    def worker_snapshots(self) -> list[dict[str, object]]:
        return [
            {
                "worker_id": "packaged-vortex-task-capture",
                "num_cpus": 1.0,
                "num_gpus": 0.0,
                "total_memory_bytes": 1 << 30,
            }
        ]

    def submit_tasks(self, tasks: list[object]) -> list[object]:
        self.tasks.extend(tasks)
        return []

    def task_input_stream_exhausted(
        self, query_id: str, source_node_ids: list[str]
    ) -> list[object]:
        require_equal(query_id, self.query_id, "captured task exhausted query")
        self.exhausted_source_ids.update(str(node_id) for node_id in source_node_ids)
        return []

    def fte_query_status(
        self, query_id: str, task_contexts: object | None = None
    ) -> dict[str, object]:
        require_equal(query_id, self.query_id, "captured task status query")
        captured = bool(self.tasks)
        status: dict[str, object] = {
            "failed": False,
            "finished": captured,
            "selected_attempt_task_ids": [],
        }
        if task_contexts is not None:
            status.update(
                {
                    "matched": captured,
                    "registration_pending": not captured,
                }
            )
        return status

    def drop_query(self, query_id: str) -> None:
        require_equal(query_id, self.query_id, "captured task dropped query")

    def shutdown(self) -> None:
        return None


async def collect_native_result_stream(stream: object) -> list[object]:
    loop = asyncio.get_running_loop()
    ready = asyncio.Event()
    stream.set_ready_callback(loop, ready.set)
    results: list[object] = []
    try:
        while True:
            try:
                item = stream.next_nowait()
            except (StopIteration, StopAsyncIteration):
                return results
            except RuntimeError as error:
                if "StopIteration" in str(error):
                    return results
                raise
            if item is not None:
                results.append(item)
                continue
            ready.clear()
            stream.arm_ready_notification()
            await ready.wait()
    finally:
        stream.clear_ready_callback()


def produce_native_scan_worker_task(
    vane: object,
    connection: object,
    query: str,
    query_id: str,
) -> tuple[object, str, object]:
    """Return the worker task emitted by Vane's production plan pipeline."""

    plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
        connection.sql(query),
        query_id,
    ).to_physical_plan(connection)
    backend = _NativeTaskCaptureBackend(query_id)
    runner = vane.ray_cxx.DistributedPhysicalPlanRunner(backend)
    try:
        stream = runner.run_plan(plan, connection)
        require_equal(
            asyncio.run(collect_native_result_stream(stream)),
            [],
            "captured Vortex worker task produced coordinator output",
        )
        require_equal(len(backend.tasks), 1, "captured Vortex worker task count")
        task = backend.tasks[0]
        context = dict(task.context())
        require_equal(context.get("query_id"), query_id, "captured task query id")
        require_equal(
            context.get("resource_query_id"),
            query_id,
            "captured task resource query id",
        )
        inputs = dict(task.Inputs())
        scan_inputs = {
            str(node_id): dict(entry)
            for node_id, entry in inputs.items()
            if entry.get("kind") == "scan_split_batch"
        }
        require_equal(len(scan_inputs), 1, "captured Vortex scan input count")
        source_node_id, scan_input = next(iter(scan_inputs.items()))
        split_batch = bytes(scan_input["data"])
        split_ids = [
            str(split_id)
            for split_id, _batch, _estimated_bytes in vane.ray_cxx.split_scan_split_batch(
                split_batch
            )
        ]
        require_equal(split_ids, ["0"], "captured Vortex scan split identity")
        require_true(
            source_node_id in backend.exhausted_source_ids,
            "Vane did not exhaust the captured Vortex scan source",
        )
        return task, source_node_id, runner
    except BaseException:
        try:
            runner.drop_query_fragments(query_id)
        finally:
            runner.shutdown()
        raise


def clear_fault_injection_state() -> None:
    import vane.runners.ray.worker_handle as worker_handle
    from vane.runners.ray.fte_fragment_scheduler import _stop_fte_status_watchers
    from vane.runners.ray.query_resource_runtime import clear_query_resource_managers

    _stop_fte_status_watchers()
    clear_query_resource_managers()
    worker_handle._FTE_FRAGMENT_EXECUTION_IDS.clear()
    worker_handle._FTE_QUERY_NEXT_FRAGMENT_EXECUTION_ID.clear()
    worker_handle._FTE_STABLE_TASK_IDENTITY_KEYS_BY_RESOURCE_QUERY.clear()
    worker_handle._FTE_FRAGMENT_EXECUTIONS.clear()
    worker_handle._FTE_PARTITION_OWNERS.clear()
    worker_handle._FTE_SEQUENCES.clear()
    worker_handle._FTE_FRAGMENT_STATES.clear()
    worker_handle._FTE_WORKER_HANDLES.clear()
    worker_handle._FTE_RETRY_DELAYS.clear()
    worker_handle._FTE_SCHEDULERS.clear()
    worker_handle._FTE_CLOSING_QUERIES.clear()
    worker_handle._FTE_ACTIVE_OPERATIONS_BY_QUERY.clear()
    worker_handle._FTE_ACTIVE_TEARDOWN_OPERATIONS_BY_QUERY.clear()


def register_fault_query(task: object) -> None:
    from vane.runners.ray.query_resource_graph import (
        QueryAllocation,
        QueryResourceGraph,
        ResourceUnitSpec,
        ResourceVector,
    )
    from vane.runners.ray.query_resource_graph_builder import (
        native_fragment_unit_id_for_fragment,
    )
    from vane.runners.ray.query_resource_runtime import register_query_resource_graph

    context = dict(task.context())
    query_id = str(context.get("query_id") or "")
    fragment_node_id = str(context.get("node_id") or "")
    resource_query_id = str(context.get("resource_query_id") or "")
    require_true(bool(query_id), "captured task has no query id")
    require_true(bool(fragment_node_id), "captured task has no fragment node id")
    fragment_id = f"{query_id}:node:{fragment_node_id}"
    resource_unit_id = native_fragment_unit_id_for_fragment(query_id, fragment_id)
    require_equal(
        resource_query_id,
        query_id,
        "captured task resource query identity",
    )
    require_equal(
        context.get("resource_unit_id"),
        resource_unit_id,
        "captured task resource unit identity",
    )
    target_output_block_bytes = 1024 * 1024
    unit = ResourceUnitSpec(
        query_id=query_id,
        resource_unit_id=resource_unit_id,
        physical_node_id=f"node:{fragment_node_id}:native-fragment",
        unit_kind="native_fragment",
        backend="ray_worker",
        input_unit_ids=(),
        per_task=ResourceVector(),
        target_output_block_bytes=target_output_block_bytes,
        generator_buffer_blocks=1,
        max_concurrency=1,
    )
    manager = register_query_resource_graph(
        QueryResourceGraph(
            query_id=query_id,
            plan_digest=f"sha256:packaged-vortex-fault:{query_id}",
            units=(unit,),
            terminal_unit_ids=(unit.resource_unit_id,),
        ),
        QueryAllocation(
            resources=ResourceVector(
                cpu=1,
                heap_bytes=64 * 1024 * 1024,
                object_store_bytes=(target_output_block_bytes * 4 + 2) // 3,
            ),
            generation=1,
        ),
    )
    manager.update_unit_state(unit.resource_unit_id, runnable=True)


def wait_for_replayable_scan_attempt(
    ray: object,
    actor: object,
    task_handle: object,
    description: str,
) -> dict[str, object]:
    """Wait until an asynchronously registered dynamic scan is safe to fault."""

    deadline = time.monotonic() + 15
    watcher_started = False
    last_status: dict[str, object] = {}
    while time.monotonic() < deadline:
        last_status = dict(
            ray.get(actor.fte_get_task_status.remote(task_handle.task_id.to_dict()))
        )
        state = str(last_status.get("state") or "UNKNOWN")
        if state != "UNKNOWN" and not watcher_started:
            task_handle.done()
            watcher_started = True
        if state == "RUNNING" and int(last_status.get("queued_split_count", 0)) == 0:
            return last_status
        if state in {"FAILED", "CANCELED", "ABORTED", "FINISHED"}:
            raise AssertionError(
                f"{description} became terminal before fault injection: {last_status!r}"
            )
        time.sleep(0.05)
    raise AssertionError(
        f"{description} did not reach its replayable RUNNING state: {last_status!r}"
    )


def exercise_real_actor_loss_scan_replay(
    vane: object,
    ray: object,
    connection: object,
    source_path: Path,
    expected_nodes: set[str],
) -> None:
    import vane.runners.ray.worker_handle as worker_handle
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    from vane.runners.ray import worker as worker_module
    from vane.runners.ray.worker_handle import RayWorkerActorHandle

    clear_fault_injection_state()
    query = f"SELECT id FROM read_vortex({sql_string(source_path)})"
    query_id = f"vane-wheel-vortex-actor-loss-{uuid.uuid4().hex}"
    task, source_node_id, capture_runner = produce_native_scan_worker_task(
        vane,
        connection,
        query,
        query_id,
    )
    actor0 = None
    actor1 = None
    try:
        register_fault_query(task)
        first_node, second_node = sorted(expected_nodes)
        actor0 = worker_module.RayWorkerActor.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=first_node, soft=False
            ),
        ).remote(1, 0, 1 << 30, 1 << 60)
        actor1 = worker_module.RayWorkerActor.options(
            num_cpus=0,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=second_node, soft=False
            ),
        ).remote(1, 0, 1 << 30, 1 << 60)
        handle0 = RayWorkerActorHandle(
            actor0,
            memory_capacity_bytes=1 << 60,
            node_id=first_node,
            worker_id="packaged-vortex-worker-0-before-loss",
            host=first_node,
        )
        task_handle = handle0.submit_tasks([task])[0]
        require_equal(
            str(task_handle.task_id), f"{query_id}.0.0.0", "initial Vortex scan attempt"
        )
        require_equal(
            task_handle.worker_id,
            handle0.worker_id,
            "initial Vortex scan worker",
        )
        require_equal(
            [
                str(handle.task_id)
                for handle in handle0.pop_fte_result_handles(query_id)
            ],
            [f"{query_id}.0.0.0"],
            "initial Vortex result handle",
        )
        wait_for_replayable_scan_attempt(
            ray,
            actor0,
            task_handle,
            "the initial Vortex scan",
        )

        handle1 = RayWorkerActorHandle(
            actor1,
            memory_capacity_bytes=1 << 60,
            node_id=second_node,
            worker_id="packaged-vortex-worker-1-after-loss",
            host=second_node,
        )
        ray.kill(actor0, no_restart=True)
        try:
            asyncio.run(asyncio.wait_for(task_handle.get_result(), timeout=10))
        except Exception:
            pass
        else:
            raise AssertionError("killing the Vortex scan actor did not fail attempt 0")

        handle0.wait_fte_worker_failure_reconciliation(timeout_s=10)
        retry_handles = handle1.pop_fte_result_handles(query_id)
        require_equal(len(retry_handles), 1, "replacement Vortex scan attempt count")
        retry_handle = retry_handles[0]
        require_equal(
            str(retry_handle.task_id),
            f"{query_id}.0.0.1",
            "replayed Vortex scan attempt",
        )
        require_equal(
            retry_handle.worker_id,
            handle1.worker_id,
            "replayed Vortex scan worker",
        )
        wait_for_replayable_scan_attempt(
            ray,
            actor1,
            retry_handle,
            "the replayed Vortex scan",
        )
        handle1.task_input_stream_exhausted([str(source_node_id)])
        result = asyncio.run(asyncio.wait_for(retry_handle.get_result(), timeout=20))
        require_true(
            result.ok and result.has_output, "replayed Vortex scan produced no output"
        )

        final_info = ray.get(
            actor1.fte_get_task_info.remote(retry_handle.task_id.to_dict())
        )
        raw_result = final_info["result"]
        if isinstance(raw_result, dict):
            raw_result = raw_result["result"]
        output_refs, metadata, *_ = raw_result
        require_equal(
            sum(entry[0] for entry in metadata), 32, "replayed Vortex output row count"
        )
        replayed_ids = sorted(
            value
            for output_ref in output_refs
            for value in ray.get(output_ref).column(0).to_pylist()
        )
        require_equal(
            replayed_ids,
            list(range(32)),
            "replayed Vortex rows",
        )
        require_true(
            handle0.worker_id not in worker_handle._FTE_WORKER_HANDLES,
            "the lost Vortex worker remained registered",
        )
    finally:
        try:
            for actor in (actor0, actor1):
                if actor is None:
                    continue
                try:
                    ray.kill(actor, no_restart=True)
                except Exception:
                    pass
            clear_fault_injection_state()
        finally:
            try:
                capture_runner.drop_query_fragments(query_id)
            finally:
                capture_runner.shutdown()


def exercise_worker_topology(
    harness: RayVortexHarness,
    ray: object,
    expected_nodes: set[str],
    files: list[Path],
) -> None:
    vane = harness.vane
    # The runner has only executed Vortex reads at this point, so these
    # counters cannot be satisfied by the UDF fragment below.
    assert_vane_worker_topology(ray, harness.runner, expected_nodes)
    previous_count = harness.read_dispatch_count
    relation = harness.connection.sql(
        f"SELECT id FROM read_vortex({vortex_file_list(files)})"
    ).map_batches(
        AnnotateWorkerNode,
        schema={
            "id": vane.sqltype("BIGINT"),
            "worker_node_id": vane.sqltype("VARCHAR"),
            "vane_module": vane.sqltype("VARCHAR"),
        },
        batch_size=8,
        cpus=1.0,
        execution_backend="ray_actor",
        actor_number=WORKER_COUNT,
        target_max_batch_bytes=4096,
    )
    rows = relation.fetchall()
    require_equal(
        harness.read_dispatch_count, previous_count + 1, "worker topology Ray dispatch"
    )
    require_equal(
        sorted(row[0] for row in rows),
        list(range(TOTAL_ROWS)),
        "worker topology Vortex rows",
    )
    # Ray Core owns UDF actor placement and may let one ready actor consume all
    # batches. The persistent-worker check above is the authoritative proof
    # that Vane executed Vortex fragments on both cluster nodes.
    udf_nodes = {str(row[1]) for row in rows}
    require_true(bool(udf_nodes), "Vortex worker topology exposed no UDF node")
    require_true(
        udf_nodes <= expected_nodes,
        f"Vortex UDF ran outside the execution nodes: {udf_nodes!r}",
    )
    require_equal(
        {str(row[2]) for row in rows},
        {str(Path(harness.vane.__file__).resolve())},
        "worker installed-wheel identity",
    )


def exercise_distributed_copy(
    harness: RayVortexHarness,
    root: Path,
    files: list[Path],
    empty_path: Path,
) -> None:
    connection = harness.connection
    source_query = (
        "SELECT id, part, payload, nullable_value "
        f"FROM read_vortex({vortex_file_list(files)})"
    )

    output = root / "distributed-copy"
    output.mkdir()
    result = harness.require_copy(
        "distributed Vortex COPY",
        lambda: connection.sql(source_query).write_file(str(output), format="vortex"),
        expected_rows=TOTAL_ROWS,
        minimum_files=WORKER_COUNT,
    )
    committed = harness.vane.ray_cxx.read_committed_copy_direct_write_result(
        str(result["copy_output_base_path"]),
        str(result["copy_output_run_id"]),
        connection,
    )
    committed_paths = sorted(
        Path(str(entry["final_path"])).resolve() for entry in committed["files"]
    )
    assert_exact_dataset(
        connection, committed_paths, "distributed Vortex COPY committed readback"
    )

    loser_path = output / f"{result['copy_output_run_id']}_w_unselected_data.vortex"
    connection.execute(
        f"COPY (SELECT 999::BIGINT AS id, 7::INTEGER AS part, 'loser'::VARCHAR AS payload, "
        f"1::INTEGER AS nullable_value) TO {sql_string(loser_path)} (FORMAT VORTEX)"
    )
    require_equal(
        connection.execute(
            f"SELECT count(*)::BIGINT FROM read_vortex({sql_string(output / '*.vortex')})"
        ).fetchall(),
        [(TOTAL_ROWS + 1,)],
        "raw output prefix includes an unselected attempt",
    )
    selected_again = harness.vane.ray_cxx.read_committed_copy_direct_write_result(
        str(result["copy_output_base_path"]),
        str(result["copy_output_run_id"]),
        connection,
    )
    selected_again_paths = sorted(
        Path(str(entry["final_path"])).resolve() for entry in selected_again["files"]
    )
    require_equal(
        selected_again_paths,
        committed_paths,
        "manifest excludes the visible unselected attempt",
    )
    assert_exact_dataset(
        connection, selected_again_paths, "selected-attempt Vortex manifest readback"
    )
    loser_path.unlink()

    harness.require_query(
        "SELECT id, part, payload, nullable_value "
        f"FROM read_vortex({vortex_file_list(committed_paths)}) ORDER BY id",
        "distributed COPY Ray readback",
    )

    empty_output = root / "distributed-empty-copy"
    empty_output.mkdir()
    empty_result = harness.require_copy(
        "empty distributed Vortex COPY",
        lambda: connection.sql(
            "SELECT id, part, payload, nullable_value "
            f"FROM read_vortex({sql_string(empty_path)})"
        ).write_file(str(empty_output), format="vortex"),
        expected_rows=0,
        minimum_files=1,
    )
    empty_committed = harness.vane.ray_cxx.read_committed_copy_direct_write_result(
        str(empty_result["copy_output_base_path"]),
        str(empty_result["copy_output_run_id"]),
        connection,
    )
    empty_paths = [
        Path(str(entry["final_path"])).resolve() for entry in empty_committed["files"]
    ]
    require_equal(
        connection.execute(
            f"SELECT count(*)::BIGINT FROM read_vortex({vortex_file_list(empty_paths)})"
        ).fetchall(),
        [(0,)],
        "empty distributed Vortex COPY readback",
    )

    failed_output = root / "failed-copy-retry"
    failed_output.mkdir()
    failing_relation = connection.sql(source_query).map_batches(
        FailSelectedVortexWorker,
        schema={
            "id": harness.vane.sqltype("BIGINT"),
            "part": harness.vane.sqltype("INTEGER"),
            "payload": harness.vane.sqltype("VARCHAR"),
            "nullable_value": harness.vane.sqltype("INTEGER"),
        },
        batch_size=8,
        cpus=1.0,
        execution_backend="ray_actor",
        actor_number=WORKER_COUNT,
        target_max_batch_bytes=4096,
    )
    harness.require_copy_failure(
        "distributed Vortex COPY task failure",
        lambda: failing_relation.write_file(str(failed_output), format="vortex"),
        "intentional distributed Vortex COPY task failure",
    )
    require_true(
        not Path(str(failed_output) + ".duckdb_commit").exists(),
        "failed Vortex COPY retained an uncommitted lifecycle",
    )
    require_equal(
        list(failed_output.rglob("*.vortex")), [], "failed Vortex COPY attempt cleanup"
    )

    retry_result = harness.require_copy(
        "distributed Vortex COPY explicit retry",
        lambda: connection.sql(source_query).write_file(
            str(failed_output), format="vortex"
        ),
        expected_rows=TOTAL_ROWS,
        minimum_files=WORKER_COUNT,
    )
    retry_committed = harness.vane.ray_cxx.read_committed_copy_direct_write_result(
        str(retry_result["copy_output_base_path"]),
        str(retry_result["copy_output_run_id"]),
        connection,
    )
    retry_paths = [
        Path(str(entry["final_path"])).resolve() for entry in retry_committed["files"]
    ]
    assert_exact_dataset(
        connection, retry_paths, "retried Vortex COPY has no duplicate or lost rows"
    )


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "ray":
        raise RuntimeError(
            "the distributed wheel qualification requires VANE_RUNNER=ray"
        )
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"
    os.environ["VANE_FTE_RETRY_INITIAL_DELAY_S"] = "0"
    os.environ["VANE_FTE_STATUS_WAIT_TIMEOUT_S"] = "5"
    os.environ["VANE_FTE_CONTROL_RPC_INITIAL_BACKOFF_S"] = "0"
    os.environ["VANE_FTE_SPLIT_QUEUE_SPACE_WAIT_TIMEOUT_S"] = "0.1"

    import ray
    import vane
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError("the Ray wheel qualification must own its Ray cluster")

    cluster = create_two_worker_cluster(ray)
    connection = None
    runner_configured = False
    try:
        expected_nodes = execution_node_ids(ray)
        connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        verify_installed_runtime(vane, connection, "ray")
        with tempfile.TemporaryDirectory(prefix="vane-vortex-ray-") as temporary:
            root = Path(temporary).resolve()
            files, empty_path = create_vortex_fixture(connection, root / "input")
            verify_scan_schema(connection, files, empty_path)
            verify_known_case_results(connection, files, empty_path)

            exercise_real_actor_loss_scan_replay(
                vane,
                ray,
                connection,
                files[0],
                expected_nodes,
            )

            vane.set_runner_ray(noop_if_initialized=True)
            runner_configured = True
            runner = runners.get_or_create_runner()
            require_equal(
                getattr(runner, "name", None), "ray", "configured Vane runner"
            )
            harness = RayVortexHarness(vane, connection, runner)

            files_sql = vortex_file_list(files)
            all_files_query = f"SELECT id FROM read_vortex({files_sql})"
            require_equal(
                harness.split_ids(all_files_query),
                ["0", "1", "2", "3"],
                "Vortex elementary split identities",
            )
            aggregate_query = (
                "SELECT count(*)::BIGINT, min(id)::BIGINT, max(id)::BIGINT "
                f"FROM read_vortex({files_sql})"
            )
            require_equal(
                sum(
                    len(batches)
                    for batches in harness.physical_plan(aggregate_query)
                    .scan_split_batch_map()
                    .values()
                ),
                1,
                "Vortex final aggregate indivisible split count",
            )
            pruned_query = (
                "SELECT id, file_index "
                f"FROM read_vortex({files_sql}) WHERE file_index IN (1, 3)"
            )
            require_equal(
                harness.split_ids(pruned_query),
                ["1", "3"],
                "Vortex file_index-pruned split identities",
            )
            empty_assignment_query = (
                "SELECT id, file_index "
                f"FROM read_vortex({files_sql}) WHERE file_index = 99"
            )
            require_equal(
                harness.split_ids(empty_assignment_query),
                ["empty"],
                "Vortex explicit empty assignment",
            )

            for description, query in scan_cases(files, empty_path):
                harness.require_query(query, f"distributed {description}")

            repeated_query = (
                "SELECT id, payload FROM read_vortex("
                f"{vortex_file_list(files)}) WHERE id BETWEEN 41 AND 77 ORDER BY id"
            )
            repeated_expected = connection.execute(repeated_query).fetchall()
            repeated_relation = connection.sql(repeated_query)
            previous_reads = harness.read_dispatch_count
            require_equal(
                repeated_relation.fetchall(),
                repeated_expected,
                "first Ray prepared relation execution",
            )
            require_equal(
                repeated_relation.fetchall(),
                repeated_expected,
                "replayed Ray prepared relation execution",
            )
            require_equal(
                harness.read_dispatch_count,
                previous_reads + 2,
                "repeated Ray relation dispatch count",
            )

            exercise_worker_topology(harness, ray, expected_nodes, files)
            exercise_distributed_copy(harness, root, files, empty_path)
            require_true(
                harness.read_dispatch_count >= 13,
                "Ray suite did not exercise enough Vortex reads",
            )
            require_true(
                harness.write_dispatch_count >= 4,
                "Ray suite did not exercise enough Vortex writes",
            )
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                if runner_configured:
                    vane.teardown_runner()
            finally:
                if ray.is_initialized():
                    ray.shutdown()
                cluster.shutdown()


if __name__ == "__main__":
    main()

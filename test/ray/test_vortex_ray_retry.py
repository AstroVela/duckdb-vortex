# SPDX-FileCopyrightText: 2026 Vortex contributors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import contextlib
import importlib
import sys
import time
import uuid
from pathlib import Path

import pytest
import ray

import vane
from vane import runners as vane_runners

ROOT = Path(__file__).resolve().parents[2]
VANE_ROOT = ROOT / "vane"
VANE_TESTS = VANE_ROOT / "tests"

# Reuse Vane's production-actor fault harness. It owns the retry scheduler,
# worker replacement, and dynamic split replay mechanics that this extension
# integration test needs to exercise. Keep Vane's test directories scoped to
# these imports: tests/fast contains a pandas namespace that would otherwise
# shadow PyArrow's optional pandas dependency during the Vortex result tests.
_original_sys_path = list(sys.path)
try:
    sys.path.insert(0, str(VANE_ROOT))
    sys.path.insert(0, str(VANE_TESTS))
    sys.path.insert(0, str(VANE_TESTS / "fast"))
    fault = importlib.import_module("test_ray_fte_fault_injection")
    result_contract = importlib.import_module("test_ray_result_contract")
finally:
    sys.path[:] = _original_sys_path

pytestmark = [pytest.mark.real_ray, pytest.mark.ray_cluster_owner]


def _sql_string(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


class _CapturingWorker:
    def __init__(self, *, fail_query=False, materialize_tasks=False):
        self.tasks = []
        self.fail_query = fail_query
        self.materialize_tasks = materialize_tasks

    def submit_tasks(self, tasks):
        if self.materialize_tasks:
            tasks = [_MaterializedWorkerTask(task) for task in tasks]
        self.tasks.extend(tasks)
        return []

    def fte_query_status(self, _query_id, _task_contexts=None):
        return {
            "failed": self.fail_query,
            "finished": True,
            "matched": True,
            "selected_attempt_task_ids": [],
        }

    def pop_fte_result_handles(self, _query_id):
        return []

    def stats_fragments(self):
        return {"registered_total": 0, "existing_total": 0, "lookup_hits": 0}

    def fte_prepare_drop_query(self, _query_id):
        return {"tasks_removed": 0, "tasks_canceled": 0, "fragments_removed": 0}

    def fte_cleanup_query(self, _query_id):
        return {}

    def task_input_stream_exhausted_for_query(self, _query_id, _source_node_ids):
        return []

    def prepare_shutdown(self):
        return None

    def finish_shutdown(self):
        return None

    def abort_shutdown(self):
        return None


class _MaterializedWorkerTask:
    """Own the Python plan wrapper after its registration scope is gone."""

    def __init__(self, task):
        self._name = task.name()
        self._context = dict(task.context() or {})
        self._task_context = dict(task.task_context() or {})
        self._inputs = dict(task.Inputs() or {})
        self._exchange_sink_instance = task.exchange_sink_instance()
        # RayWorkerTask.plan() snapshots connection/session resources into the
        # returned wrapper. Materialize it while coordinator registration is
        # alive so the self-contained task can be retried after teardown.
        self._plan = task.plan()

    def name(self):
        return self._name

    def context(self):
        return dict(self._context)

    def task_context(self):
        return dict(self._task_context)

    def Inputs(self):
        return dict(self._inputs)

    def exchange_sink_instance(self):
        return self._exchange_sink_instance

    def plan(self):
        return self._plan


def _extract_worker_scan_split_batch(monkeypatch, connection, coordinator_plan):
    """Capture the detached scan plan that the production translator submits."""
    import vane.runners.ray.worker_handle as ray_worker_handle

    worker = _CapturingWorker()
    with monkeypatch.context() as capture_patch:
        capture_patch.setattr(
            ray_worker_handle,
            "start_ray_workers",
            lambda _existing_ids, _manager_instance_id: [
                vane.ray_cxx.RayWorkerRuntime(
                    "capture-node",
                    worker,
                    4.0,
                    0.0,
                    8 << 30,
                )
            ],
        )
        capture_patch.setattr(
            ray_worker_handle,
            "try_autoscale",
            lambda _bundles: None,
        )
        runner = vane.ray_cxx.DistributedPhysicalPlanRunner()
        with result_contract._registered_low_level_plan(
            coordinator_plan,
            connection,
            node_id="capture-node",
        ):
            stream = runner.run_plan(coordinator_plan, connection)
            assert result_contract.collect_result_stream(stream) == []

    scan_split_batches = []
    for task in worker.tasks:
        for node_id, entry in task.Inputs().items():
            if entry["kind"] == "scan_split_batch":
                scan_split_batches.append((task.plan(), str(node_id), bytes(entry["data"])))
    assert len(scan_split_batches) == 1
    return scan_split_batches[0]


def _capture_vortex_write_relation(monkeypatch, relation, output_path):
    captured = []

    class _CapturingRunner:
        name = "ray"

        def run_write(self, write_relation):
            captured.append(write_relation)
            return {"captured": True}

    with monkeypatch.context() as capture_patch:
        capture_patch.setenv("VANE_RUNNER", "ray")
        capture_patch.setattr(
            vane_runners,
            "set_runner_ray",
            lambda *_args, **_kwargs: _CapturingRunner(),
        )
        relation.write_file(str(output_path), format="vortex")

    assert len(captured) == 1
    assert captured[0].type == "WRITE_FILE_RELATION"
    return captured[0]


def _extract_worker_copy_task(monkeypatch, connection, coordinator_plan):
    """Capture a production COPY task with its output and scan-split context."""
    import vane.runners.ray.worker_handle as ray_worker_handle

    # Fail after submission so the production runner cleans up instead of
    # publishing an empty manifest for the run whose task we are capturing.
    worker = _CapturingWorker(fail_query=True, materialize_tasks=True)
    with monkeypatch.context() as capture_patch:
        capture_patch.setattr(
            ray_worker_handle,
            "start_ray_workers",
            lambda _existing_ids, _manager_instance_id: [
                vane.ray_cxx.RayWorkerRuntime(
                    "capture-copy-node",
                    worker,
                    4.0,
                    0.0,
                    8 << 30,
                )
            ],
        )
        capture_patch.setattr(
            ray_worker_handle,
            "try_autoscale",
            lambda _bundles: None,
        )
        runner = vane.ray_cxx.DistributedPhysicalPlanRunner()
        with result_contract._registered_low_level_plan(
            coordinator_plan,
            connection,
            node_id="capture-copy-node",
        ):
            with pytest.raises(ValueError, match="FTE query failed"):
                runner.run_copy_plan(coordinator_plan, connection)
            copy_tasks = [
                task
                for task in worker.tasks
                if task.context().get("copy_output_run_id")
                and any(entry["kind"] == "scan_split_batch" for entry in task.Inputs().values())
            ]
            assert len(copy_tasks) == 1
            return copy_tasks[0]


def test_vortex_split_replays_after_real_ray_worker_loss(monkeypatch, tmp_path):
    monkeypatch.setenv("VANE_FTE_STATUS_WAIT_TIMEOUT_S", "5")
    monkeypatch.setenv("VANE_FTE_CONTROL_RPC_INITIAL_BACKOFF_S", "0")
    monkeypatch.setenv("VANE_FTE_SPLIT_QUEUE_SPACE_WAIT_TIMEOUT_S", "0.1")
    fault._clear_fte_state()

    connection = vane.connect()
    actor0 = None
    actor1 = None
    try:
        path = tmp_path / "retry.vortex"
        connection.execute(f"""
            COPY (
                SELECT range::BIGINT AS id, 'row-' || range::VARCHAR AS payload
                FROM range(10)
            ) TO {_sql_string(path)} (FORMAT VORTEX)
            """)
        relation = connection.sql(f"SELECT sum(id) AS total FROM read_vortex({_sql_string(path)})")
        plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            f"vortex-retry-plan-{uuid.uuid4()}",
        ).to_physical_plan(connection)
        split_batch_map = dict(plan.scan_split_batch_map())
        assert len(split_batch_map) == 1
        coordinator_node_id, split_batches = next(iter(split_batch_map.items()))
        assert len(split_batches) == 1
        coordinator_split_batch = bytes(split_batches[0])

        worker_plan, node_id, split_batch = _extract_worker_scan_split_batch(
            monkeypatch,
            connection,
            plan,
        )
        assert node_id == str(coordinator_node_id)
        assert split_batch == coordinator_split_batch
        fault._clear_fte_state()
        fault._init_ray_for_fault_test(monkeypatch)

        actor0 = fault.worker_mod.RayWorkerActor.options(num_cpus=0).remote(1, 0, 1 << 30, 1 << 60)
        actor1 = fault.worker_mod.RayWorkerActor.options(num_cpus=0).remote(1, 0, 1 << 30, 1 << 60)
        handle0 = fault.RayWorkerActorHandle(
            actor0,
            memory_capacity_bytes=1 << 60,
            worker_id="worker-vortex-a",
        )
        handle1 = fault.RayWorkerActorHandle(
            actor1,
            memory_capacity_bytes=1 << 60,
            worker_id="worker-vortex-b",
        )

        query_id = str(worker_plan.idx())
        task = fault._NativeDynamicScanWorkerTask(
            query_id=query_id,
            node_id=str(node_id),
            split_batch=split_batch,
            plan=worker_plan,
        )
        fault._register_fault_query([task])
        first_handle = handle0.submit_tasks([task])[0]
        assert str(first_handle.task_id) == f"{query_id}.0.0.0"
        assert [str(handle.task_id) for handle in handle0.pop_fte_result_handles(query_id)] == [f"{query_id}.0.0.0"]
        first_handle.done()

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            info = ray.get(actor0.fte_get_task_info.remote(first_handle.task_id.to_dict()))
            status = info["status"]
            if status.get("state") == "RUNNING" and int(status.get("queued_split_count", 0)) == 0:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("Vortex scan did not enter the retryable RUNNING state")

        ray.kill(actor0, no_restart=True)
        with pytest.raises(ray.exceptions.RayError):
            asyncio.run(asyncio.wait_for(first_handle.get_result(), timeout=10.0))

        handle0.wait_fte_worker_failure_reconciliation(timeout_s=10.0)
        retry_handles = handle1.pop_fte_result_handles(query_id)
        assert len(retry_handles) == 1
        retry_handle = retry_handles[0]
        assert str(retry_handle.task_id) == f"{query_id}.0.0.1"
        retry_info = ray.get(actor1.fte_get_task_info.remote(retry_handle.task_id.to_dict()))
        if retry_info["status"].get("state") == "RUNNING":
            handle1.task_input_stream_exhausted([str(node_id)])
        result = asyncio.run(asyncio.wait_for(retry_handle.get_result(), timeout=20.0))

        assert result.ok
        assert result.has_output
        final_info = ray.get(actor1.fte_get_task_info.remote(retry_handle.task_id.to_dict()))
        raw_result = final_info["result"]
        if isinstance(raw_result, dict):
            raw_result = raw_result["result"]
        output_refs, metadata, *_ = raw_result
        assert metadata[0][0] == 1
        output = ray.get(output_refs[0])
        assert output.column(0).to_pylist() == [45]
        assert "worker-vortex-a" not in fault.worker_handle_mod._FTE_WORKER_HANDLES
    finally:
        for actor in (actor0, actor1):
            if actor is not None:
                with contextlib.suppress(ray.exceptions.RayError):
                    ray.kill(actor, no_restart=True)
        connection.close()
        fault._clear_fte_state()


def test_vortex_write_split_replays_after_real_ray_worker_loss(monkeypatch, tmp_path):
    monkeypatch.setenv("VANE_FTE_STATUS_WAIT_TIMEOUT_S", "5")
    monkeypatch.setenv("VANE_FTE_CONTROL_RPC_INITIAL_BACKOFF_S", "0")
    monkeypatch.setenv("VANE_FTE_SPLIT_QUEUE_SPACE_WAIT_TIMEOUT_S", "0.1")
    fault._clear_fte_state()

    connection = vane.connect()
    actor0 = None
    actor1 = None
    try:
        source = tmp_path / "write-retry-source.vortex"
        output = tmp_path / "write-retry-output"
        connection.execute(f"""
            COPY (
                SELECT range::BIGINT AS id, 'row-' || range::VARCHAR AS payload
                FROM range(100)
            ) TO {_sql_string(source)} (FORMAT VORTEX)
            """)
        write_relation = _capture_vortex_write_relation(
            monkeypatch,
            connection.sql(f"SELECT id, payload FROM read_vortex({_sql_string(source)})"),
            output,
        )
        coordinator_plan = vane.ray_cxx.PyLogicalPlan.from_duckdb_write_relation(
            write_relation,
            f"vortex-write-retry-plan-{uuid.uuid4()}",
        ).to_physical_plan(connection)
        split_batch_map = dict(coordinator_plan.scan_split_batch_map())
        assert len(split_batch_map) == 1
        assert len(next(iter(split_batch_map.values()))) == 1

        task = _extract_worker_copy_task(monkeypatch, connection, coordinator_plan)
        split_node_ids = [
            str(node_id) for node_id, entry in task.Inputs().items() if entry["kind"] == "scan_split_batch"
        ]
        assert len(split_node_ids) == 1
        split_node_id = split_node_ids[0]

        fault._clear_fte_state()
        fault._init_ray_for_fault_test(monkeypatch)
        actor0 = fault.worker_mod.RayWorkerActor.options(num_cpus=0).remote(1, 0, 1 << 30, 1 << 60)
        actor1 = fault.worker_mod.RayWorkerActor.options(num_cpus=0).remote(1, 0, 1 << 30, 1 << 60)
        handle0 = fault.RayWorkerActorHandle(
            actor0,
            memory_capacity_bytes=1 << 60,
            worker_id="worker-vortex-write-a",
        )
        handle1 = fault.RayWorkerActorHandle(
            actor1,
            memory_capacity_bytes=1 << 60,
            worker_id="worker-vortex-write-b",
        )

        query_id = str(task.context()["query_id"])
        fault._register_fault_query([task])
        first_handle = handle0.submit_tasks([task])[0]
        assert [str(handle.task_id) for handle in handle0.pop_fte_result_handles(query_id)] == [
            str(first_handle.task_id)
        ]
        first_handle.done()

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            info = ray.get(actor0.fte_get_task_info.remote(first_handle.task_id.to_dict()))
            status = info["status"]
            if status.get("state") == "RUNNING" and int(status.get("queued_split_count", 0)) == 0:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("Vortex COPY did not enter the retryable RUNNING state")

        ray.kill(actor0, no_restart=True)
        with pytest.raises(ray.exceptions.RayError):
            asyncio.run(asyncio.wait_for(first_handle.get_result(), timeout=10.0))

        handle0.wait_fte_worker_failure_reconciliation(timeout_s=10.0)
        retry_handles = handle1.pop_fte_result_handles(query_id)
        assert len(retry_handles) == 1
        retry_handle = retry_handles[0]
        assert retry_handle.task_id.query_id == first_handle.task_id.query_id
        assert retry_handle.task_id.fragment_execution_id == first_handle.task_id.fragment_execution_id
        assert retry_handle.task_id.partition_id == first_handle.task_id.partition_id
        assert retry_handle.task_id.attempt_id == first_handle.task_id.attempt_id + 1

        retry_info = ray.get(actor1.fte_get_task_info.remote(retry_handle.task_id.to_dict()))
        if retry_info["status"].get("state") == "RUNNING":
            handle1.task_input_stream_exhausted([split_node_id])
        result = asyncio.run(asyncio.wait_for(retry_handle.get_result(), timeout=30.0))

        assert result.ok
        assert result.has_output
        final_info = ray.get(actor1.fte_get_task_info.remote(retry_handle.task_id.to_dict()))
        raw_result = final_info["result"]
        if isinstance(raw_result, dict):
            raw_result = raw_result["result"]
        output_refs, metadata, *_ = raw_result
        assert metadata[0][0] == 1
        stats = ray.get(output_refs[0])
        assert stats.num_rows == 1
        retry_output = Path(stats.column(0)[0].as_py())
        assert retry_output.is_file()
        assert stats.column(1)[0].as_py() == 100
        assert stats.column(2)[0].as_py() == retry_output.stat().st_size > 0
        rows = connection.execute(
            f"SELECT id, payload FROM read_vortex({_sql_string(retry_output)}) ORDER BY id"
        ).fetchall()
        assert rows == [(index, f"row-{index}") for index in range(100)]
        assert "worker-vortex-write-a" not in fault.worker_handle_mod._FTE_WORKER_HANDLES
    finally:
        for actor in (actor0, actor1):
            if actor is not None:
                with contextlib.suppress(Exception):
                    ray.kill(actor, no_restart=True)
        with contextlib.suppress(Exception):
            if ray.is_initialized():
                ray.shutdown()
        connection.close()
        fault._clear_fte_state()
        if ray.is_initialized() or fault._FAULT_RAY_CLUSTER is not None:
            fault._shutdown_ray_for_fault_test()

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

ROOT = Path(__file__).resolve().parents[2]
VANE_TESTS = ROOT / "vane" / "tests"
sys.path.insert(0, str(VANE_TESTS))
sys.path.insert(0, str(VANE_TESTS / "fast"))

# Reuse Vane's production-actor fault harness. It owns the retry scheduler,
# worker replacement, and dynamic split replay mechanics that this extension
# integration test needs to exercise.
fault = importlib.import_module("test_ray_fte_fault_injection")
result_contract = importlib.import_module("test_ray_result_contract")

pytestmark = [pytest.mark.real_ray, pytest.mark.ray_cluster_owner]


def _sql_string(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


class _CapturingWorker:
    def __init__(self):
        self.tasks = []

    def submit_tasks(self, tasks):
        self.tasks.extend(tasks)
        return []

    def fte_query_status(self, _query_id, _task_contexts=None):
        return {
            "failed": False,
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


def _extract_worker_scan_task(monkeypatch, connection, coordinator_plan):
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
            with pytest.raises(StopIteration):
                stream.blocking_next()

    scan_tasks = []
    for task in worker.tasks:
        for node_id, entry in task.Inputs().items():
            if entry["kind"] == "scan_task":
                scan_tasks.append((task.plan(), str(node_id), bytes(entry["data"])))
    assert len(scan_tasks) == 1
    return scan_tasks[0]


def test_vortex_descriptor_replays_after_real_ray_worker_loss(monkeypatch, tmp_path):
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
        descriptor_map = dict(plan.scan_task_descriptor_map())
        assert len(descriptor_map) == 1
        coordinator_node_id, descriptors = next(iter(descriptor_map.items()))
        assert len(descriptors) == 1
        coordinator_descriptor = bytes(descriptors[0])

        worker_plan, node_id, descriptor = _extract_worker_scan_task(
            monkeypatch,
            connection,
            plan,
        )
        assert node_id == str(coordinator_node_id)
        assert descriptor == coordinator_descriptor
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
        task = fault._NativeDynamicScanTask(
            query_id=query_id,
            node_id=str(node_id),
            descriptor=descriptor,
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

        retry_handle = fault._wait_for_result_handles(
            handle1,
            query_id,
            1,
        )[0]
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
        if ray.is_initialized() or fault._FAULT_RAY_CLUSTER is not None:
            fault._shutdown_ray_for_fault_test()

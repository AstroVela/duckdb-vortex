# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import importlib
import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def resources(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("bench_index_resources")


def test_proc_stat_preserves_parenthesized_process_names(resources):
    fields = ["S"] + ["0"] * 49
    for index, value in ((7, 19), (9, 2), (11, 51), (12, 7), (19, 100)):
        fields[index] = str(value)
    result = resources.parse_stat("123 (name with ) spaces) " + " ".join(fields), 100)
    assert result == {
        "minor_faults": 19,
        "major_faults": 2,
        "cpu_user_seconds": 0.51,
        "cpu_system_seconds": 0.07,
        "start_ticks": 100,
    }


def test_phase_sequence_has_isolated_first_search_and_all_rounds(resources):
    assert resources.phases(1, 2) == [
        "before_open",
        "ready",
        "before_round_0",
        "before_first_search",
        "after_first_search",
        "after_round_0",
        "before_round_1",
        "after_round_1",
        "before_round_2",
        "after_round_2",
        "after_close",
    ]


def test_resource_delta_never_uses_peak_rss_as_a_counter(resources):
    first = {
        "process": {"minor_faults": 7, "major_faults": 1},
        "io": {"rchar": 99, "read_bytes": 0},
    }
    last = {
        "process": {"minor_faults": 12, "major_faults": 1},
        "io": {"rchar": 119, "read_bytes": 4096},
    }
    assert resources.delta(first, last) == {
        "process": {"minor_faults": 5, "major_faults": 0},
        "io": {"rchar": 20, "read_bytes": 4096},
    }


def test_negative_counter_delta_is_rejected(resources):
    with pytest.raises(RuntimeError, match="decreased"):
        resources.delta({"io": {"rchar": 3}}, {"io": {"rchar": 2}})


def test_limits_are_verified_from_live_kernel_state(resources):
    snapshot = {
        "cpu_affinity": [8],
        "cgroup": {
            "memory.max": "2147483648",
            "memory.swap.max": "0",
            "cpu.max": "100000 100000",
            "memory.events": {"oom": 0, "oom_kill": 0},
        },
    }
    resources.verify_limits(snapshot, 8, 2048)
    snapshot["cgroup"]["memory.max"] = "max"
    with pytest.raises(RuntimeError, match="memory"):
        resources.verify_limits(snapshot, 8, 2048)


def test_cpu_and_oom_constraints_are_not_silently_ignored(resources):
    for field, value in (("cpu.max", "max 100000"), ("memory.swap.max", "max")):
        snapshot = {
            "cpu_affinity": [8],
            "cgroup": {
                "memory.max": "2147483648",
                "memory.swap.max": "0",
                "cpu.max": "100000 100000",
                "memory.events": {"oom": 0, "oom_kill": 0},
            },
        }
        snapshot["cgroup"][field] = value
        with pytest.raises(RuntimeError):
            resources.verify_limits(snapshot, 8, 2048)


def test_oom_disqualifies_a_completed_phase(resources):
    snapshot = {
        "cpu_affinity": [8],
        "cgroup": {
            "memory.max": "2147483648",
            "memory.swap.max": "0",
            "cpu.max": "100000 100000",
            "memory.events": {"oom": 1, "oom_kill": 0},
        },
    }
    with pytest.raises(RuntimeError, match="OOM"):
        resources.verify_limits(snapshot, 8, 2048)


@pytest.mark.skipif(
    os.environ.get("VORTEX_SIFT_SYSTEMD_TESTS") != "1",
    reason="Set VORTEX_SIFT_SYSTEMD_TESTS=1 on a cgroup-v2/systemd-user host",
)
def test_real_systemd_worker_constraints_and_phase_handshake(resources, tmp_path):
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import os, sys\n"
        "for phase in sys.argv[1:]:\n"
        " print(f'SIFT_BENCH_PHASE {os.getpid()} {phase}', flush=True)\n"
        " assert sys.stdin.readline() == 'continue\\n'\n"
    )
    cpu = min(os.sched_getaffinity(0))
    result = resources.run_constrained(
        [sys.executable, str(worker), *resources.phases(1, 1)],
        tmp_path,
        {"SIFT_BENCH_PHASE_BARRIER": "1"},
        tmp_path,
        cpu,
        256,
        30,
        1,
        1,
    )
    assert result["verified"]
    assert len(result["snapshots"]) == 9
    assert result["snapshots"][0]["cpu_affinity"] == [cpu]
    assert not result["snapshots"][-1]["cgroup"]["memory.events"]["oom_kill"]
    assert json.loads((tmp_path / "constraints.json").read_text())["verified"]


@pytest.mark.skipif(
    os.environ.get("VORTEX_SIFT_SYSTEMD_TESTS") != "1",
    reason="Set VORTEX_SIFT_SYSTEMD_TESTS=1 on a cgroup-v2/systemd-user host",
)
def test_private_store_copy_is_inside_the_cgroup_and_keeps_original(
    resources, tmp_path
):
    source = tmp_path / "original"
    source.mkdir()
    (source / "data").write_bytes(b"immutable index")
    target = tmp_path / "private"
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import os, sys\n"
        "from pathlib import Path\n"
        "assert Path(sys.argv[1], 'data').read_bytes() == b'immutable index'\n"
        "for phase in sys.argv[2:]:\n"
        " print(f'SIFT_BENCH_PHASE {os.getpid()} {phase}', flush=True)\n"
        " assert sys.stdin.readline() == 'continue\\n'\n"
    )
    cpu = min(os.sched_getaffinity(0))
    result = resources.run_constrained(
        [sys.executable, str(worker), str(target), *resources.phases(1, 1)],
        tmp_path,
        {"SIFT_BENCH_PHASE_BARRIER": "1"},
        tmp_path,
        cpu,
        256,
        30,
        1,
        1,
        (source, target),
    )
    assert result["verified"]
    assert (source / "data").stat().st_ino != (target / "data").stat().st_ino
    assert (source / "data").read_bytes() == b"immutable index"


@pytest.mark.skipif(
    os.environ.get("VORTEX_SIFT_SYSTEMD_TESTS") != "1",
    reason="Set VORTEX_SIFT_SYSTEMD_TESTS=1 on a cgroup-v2/systemd-user host",
)
def test_missing_phase_does_not_leave_a_running_service(resources, tmp_path):
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import os, sys\n"
        "print(f'SIFT_BENCH_PHASE {os.getpid()} unexpected', flush=True)\n"
        "sys.stdin.readline()\n"
    )
    with pytest.raises(RuntimeError, match="resource phase"):
        resources.run_constrained(
            [sys.executable, str(worker)],
            tmp_path,
            {},
            tmp_path,
            min(os.sched_getaffinity(0)),
            256,
            30,
            1,
            1,
        )
    assert json.loads((tmp_path / "constraints.json").read_text())["verified"] is False

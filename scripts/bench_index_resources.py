#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Verified Linux cgroup limits and phase-boundary diagnostics for SIFT workers."""

import math
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import bench_index_sql as bench


def parse_stat(text, ticks):
    fields = text.rsplit(")", 1)[1].split()
    return {
        "minor_faults": int(fields[7]),
        "major_faults": int(fields[9]),
        "cpu_user_seconds": int(fields[11]) / ticks,
        "cpu_system_seconds": int(fields[12]) / ticks,
        "start_ticks": int(fields[19]),
    }


def key_values(text):
    return {
        key.rstrip(":"): int(value) for key, value in map(str.split, text.splitlines())
    }


def capture(pid, phase):
    proc = Path("/proc") / str(pid)
    (membership,) = [
        line.removeprefix("0::")
        for line in (proc / "cgroup").read_text().splitlines()
        if line.startswith("0::")
    ]
    cgroup = Path("/sys/fs/cgroup") / membership.lstrip("/")
    status = {
        key: value.strip()
        for key, value in (
            line.split(":", 1) for line in (proc / "status").read_text().splitlines()
        )
    }
    return {
        "phase": phase,
        "pid": pid,
        "monotonic_seconds": time.monotonic(),
        "process": parse_stat((proc / "stat").read_text(), os.sysconf("SC_CLK_TCK")),
        "io": key_values((proc / "io").read_text()),
        "cpu_affinity": sorted(os.sched_getaffinity(pid)),
        "memory": {
            key: int(status[key].split()[0]) * 1024
            for key in ("VmRSS", "VmHWM", "RssAnon", "RssFile", "RssShmem")
            if key in status
        },
        "threads": int(status["Threads"]),
        "cpu": key_values((cgroup / "cpu.stat").read_text()),
        "cgroup_path": membership,
        "cgroup": {
            **{
                key: (cgroup / key).read_text().strip()
                for key in (
                    "memory.max",
                    "memory.swap.max",
                    "memory.current",
                    "memory.peak",
                    "cpu.max",
                )
            },
            **{
                key: key_values((cgroup / key).read_text())
                for key in ("memory.events", "memory.stat")
            },
        },
    }


def verify_limits(snapshot, cpu, memory_mib):
    bench.require(snapshot["cpu_affinity"] == [cpu], "Worker CPU affinity differs")
    cgroup = snapshot["cgroup"]
    bench.require(
        cgroup["memory.max"] == str(memory_mib * 1024**2), "Worker memory limit differs"
    )
    bench.require(cgroup["memory.swap.max"] == "0", "Worker swap limit differs")
    quota, period = cgroup["cpu.max"].split()
    bench.require(
        quota != "max" and int(quota) == int(period) > 0, "Worker CPU quota differs"
    )
    bench.require(
        not any(
            cgroup["memory.events"].get(key, 0)
            for key in ("oom", "oom_kill", "oom_group_kill")
        ),
        "Worker reached OOM; timing is not qualified",
    )


def phases(warmup, rounds):
    result = ["before_open", "ready"]
    for round_id in range(warmup + rounds):
        result.append(f"before_round_{round_id}")
        if round_id == 0:
            result.extend(("before_first_search", "after_first_search"))
        result.append(f"after_round_{round_id}")
    return result + ["after_close"]


def delta(first, last):
    result = {}
    for group in ("process", "io", "cpu"):
        if group not in first:
            continue
        result[group] = {}
        for key, start in first[group].items():
            if key == "start_ticks":
                continue
            value = last[group][key] - start
            bench.require(value >= 0, f"Resource counter decreased: {group}.{key}")
            result[group][key] = value
    return result


def phase_deltas(snapshots, warmup, rounds):
    by_phase = {snapshot["phase"]: snapshot for snapshot in snapshots}
    result = {
        "startup": delta(by_phase["before_open"], by_phase["ready"]),
        "first_search": delta(
            by_phase["before_first_search"], by_phase["after_first_search"]
        ),
        "rounds": [
            {
                "round": i,
                "warmup": i < warmup,
                **delta(by_phase[f"before_round_{i}"], by_phase[f"after_round_{i}"]),
            }
            for i in range(warmup + rounds)
        ],
    }
    measured = {}
    for item in result["rounds"][warmup:]:
        for group in ("process", "io", "cpu"):
            for key, value in item.get(group, {}).items():
                counts = measured.setdefault(group, {})
                counts[key] = counts.get(key, 0) + value
    result["measured"] = measured
    return result


def run_constrained(
    command,
    cwd,
    env,
    output,
    cpu,
    memory_mib,
    timeout,
    warmup,
    rounds,
    private_store=None,
    controller_cpu=None,
):
    original_affinity = os.sched_getaffinity(0)
    bench.require(cpu in original_affinity, "CPU is not available to this caller")
    if controller_cpu is not None:
        bench.require(
            controller_cpu in original_affinity and controller_cpu != cpu,
            "Controller requires a separate available CPU",
        )
    bench.require(128 <= memory_mib <= 65536, "Expected 128..=65536 MiB cgroup budget")
    unit = f"vane-sift-{uuid.uuid4().hex[:16]}.service"
    worker_command = [
        "systemd-run",
        "--user",
        "--quiet",
        "--wait",
        "--pipe",
        f"--unit={unit}",
        f"--property=MemoryMax={memory_mib * 1024**2}",
        "--property=MemorySwapMax=0",
        f"--property=CPUAffinity={cpu}",
        "--property=CPUQuota=100%",
        f"--property=WorkingDirectory={cwd}",
        f"--property=RuntimeMaxSec={math.ceil(timeout)}",
        "--property=TimeoutStopSec=10",
    ]
    # Do not copy unrelated shell credentials into a logged command line.
    worker_command.extend(
        f"--setenv={key}={value}"
        for key, value in sorted(env.items())
        if key
        in (
            "OMP_NUM_THREADS",
            "RAYON_NUM_THREADS",
            "TOKIO_WORKER_THREADS",
            "RUST_LOG",
            "VORTEX_INDEX_TIMING",
            "VORTEX_SPFRESH_POSTING_VIEW",
        )
        or key.startswith("SIFT_BENCH_")
    )
    if private_store is not None:
        source, target = private_store
        bench.require(
            source.is_dir() and not target.exists(),
            "Invalid private store source/target",
        )
        worker_command.extend(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "stage-store",
                str(source),
                str(target),
            ]
        )
    worker_command.extend(command)
    expected = phases(warmup, rounds)
    result = {
        "unit": unit,
        "command": worker_command,
        "verified": False,
        "snapshots": [],
    }
    if controller_cpu is not None:
        result["controller_cpu"] = controller_cpu
    if private_store is not None:
        result["private_store"] = {
            "source": str(source),
            "target": str(target),
            "copy": "cp --archive --reflink=auto inside the worker cgroup; private inodes, no index rebuild",
        }
    process = None
    try:
        if controller_cpu is not None:
            os.sched_setaffinity(0, {controller_cpu})
        with (output / "stdout.log").open("x") as stdout, (output / "stderr.log").open(
            "x"
        ) as stderr:
            process = subprocess.Popen(
                worker_command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=stderr,
                text=True,
            )
            for line in process.stdout:
                stdout.write(line)
                stdout.flush()
                if not line.startswith("SIFT_BENCH_PHASE "):
                    continue
                _, pid_text, phase = line.split()
                bench.require(
                    len(result["snapshots"]) < len(expected)
                    and phase == expected[len(result["snapshots"])],
                    "Missing, duplicate or reordered resource phase",
                )
                snapshot = capture(int(pid_text), phase)
                verify_limits(snapshot, cpu, memory_mib)
                bench.require(
                    snapshot["cgroup_path"].endswith(f"/{unit}"),
                    "Worker is outside its own cgroup",
                )
                if result["snapshots"]:
                    first = result["snapshots"][0]
                    bench.require(
                        snapshot["pid"] == first["pid"]
                        and snapshot["process"]["start_ticks"]
                        == first["process"]["start_ticks"],
                        "Worker identity changed",
                    )
                result["snapshots"].append(snapshot)
                bench.write_json(output / "constraints.json", result)
                process.stdin.write("continue\n")
                process.stdin.flush()
            bench.require(
                process.wait(timeout=15) == 0,
                f"Constrained worker failed: {(output / 'stderr.log').read_text()[-3000:]}",
            )
            bench.require(
                len(result["snapshots"]) == len(expected),
                "Incomplete resource phase protocol",
            )
        result.update(
            verified=True,
            cpu=cpu,
            memory_max_bytes=memory_mib * 1024**2,
            swap_max_bytes=0,
            cpu_quota_cores=1,
            phase_deltas=phase_deltas(result["snapshots"], warmup, rounds),
            diagnostic_scope="round CPU/IO includes untimed binding, validation and sample output; first_search is isolated; rchar is logical read bytes, read_bytes is kernel-accounted storage IO",
        )
        return result
    finally:
        if controller_cpu is not None:
            os.sched_setaffinity(0, original_affinity)
        subprocess.run(
            ["systemctl", "--user", "stop", unit],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if process is not None:
            if process.stdin is not None:
                process.stdin.close()
            if process.stdout is not None:
                process.stdout.close()
            process.wait(timeout=15)
        bench.write_json(output / "constraints.json", result)
        subprocess.run(
            ["systemctl", "--user", "reset-failed", unit],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )


if __name__ == "__main__":
    bench.require(
        len(sys.argv) >= 5 and sys.argv[1] == "stage-store",
        "Expected stage-store <source> <target> <command...>",
    )
    source, target = map(Path, sys.argv[2:4])
    bench.require(
        source.is_dir() and not target.exists(), "Invalid private store source/target"
    )
    bench.require(
        not any(path.is_symlink() for path in source.rglob("*")),
        "Private store must not retain symlinks to shared data",
    )
    subprocess.run(
        ["cp", "--archive", "--reflink=auto", str(source), str(target)], check=True
    )
    os.execvpe(sys.argv[4], sys.argv[4:], os.environ)

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare full-record SIFT API latency: OpenData and prepared DuckDB C API."""

import argparse
import csv
import itertools
import json
import math
import os
import shutil
import struct
import subprocess
import time
import tomllib
from pathlib import Path

import bench_index_layers as layers
import bench_index_resources as resources
import bench_index_sift as sift
import bench_index_sql as bench
import numpy as np


def build_capi(manifest, sdk, include, output, budget_mib):
    command = json.loads(manifest.read_text())
    bench.require(
        isinstance(command, list)
        and all(isinstance(part, str) for part in command)
        and command.count("-o") == 1,
        "Invalid link manifest",
    )
    objects = [part for part in command if part.endswith(".o")]
    bench.require(
        objects and all(part.startswith("tools/shell/") for part in objects),
        "Expected a qualified DuckDB shell link manifest",
    )
    archives = [part for part in command if part.endswith("/libvortex_duckdb.a")]
    bench.require(
        len(archives) == 1 and 1 <= budget_mib <= 1024, "Invalid archive/budget"
    )
    output.mkdir(mode=0o700)
    source = Path(__file__).parent / "native/bench_index_sift_capi.cpp"
    obj = output / "benchmark.o"
    compile_command = [
        command[0],
        "-std=c++17",
        "-O3",
        "-DNDEBUG",
        "-Wall",
        "-Wextra",
        "-Werror",
        "-I",
        str(include.resolve()),
        "-c",
        str(source.resolve()),
        "-o",
        str(obj.resolve()),
    ]
    subprocess.run(compile_command, check=True)
    command = [part for part in command if part not in objects]
    command.insert(command.index("-o"), str(obj.resolve()))
    binary = output / "bench_index_sift_capi"
    command[command.index("-o") + 1] = str(binary.resolve())
    bench.write_json(output / "link-command.json", command)
    subprocess.run(command, cwd=sdk, check=True)
    result = {
        "binary": str(binary.resolve()),
        "sha256": bench.fingerprint(binary),
        "source_sha256": bench.fingerprint(source),
        "compile_command": compile_command,
        "link_manifest": str(manifest.resolve()),
        "link_manifest_sha256": bench.fingerprint(manifest),
        "sdk_root": str(sdk.resolve()),
        "archive_sha256": bench.fingerprint((sdk / archives[0]).resolve()),
        "retained_artifact_budget_bytes": budget_mib * 1024 * 1024,
        "budget": "provenance of the compiled archive, not a runtime setting",
    }
    bench.write_json(output / "build.json", result)
    return result


def query_sql(reference, probes, pages, mode="snapshot"):
    bench.require(mode in ("snapshot", "strict"), "Invalid validation mode")
    options = json.dumps(
        {"max_check": 32768, "internal_results": probes, "search_pages": pages}
    )
    return (
        'SELECT "row".id::UBIGINT AS id, "row".embedding::FLOAT[128] AS embedding, '
        "distance::FLOAT AS distance FROM vortex_index_search("
        f"{bench.quote(reference)}, $1::FLOAT[128]::FLOAT[], 10, "
        f"validation_mode := {bench.quote(mode)}, backend_options := {bench.quote(options)}) "
        "ORDER BY rank;\n"
    )


def opendata_config(
    original, entry, count, settings_path=None, nprobe_override=None, store_path=None
):
    config = tomllib.loads(original.read_text())
    storage = config["data"]["storage"]
    (params,) = config["params"]["recall"]
    bench.require(
        storage["type"] == "SlateDb"
        and storage["object_store"]["type"] == "Local"
        and params["dataset"] == entry["name"]
        and int(params["query_concurrency"]) == 1
        and int(params["block_cache_bytes"]) == 1024**3,
        "Expected the existing local single-consumer 1 GiB baseline",
    )
    # The original bencher appends the parameter-set ordinal; this runner does not.
    path = str(Path(storage["path"]) / "0")
    lines = [
        "[data.storage]",
        'type = "SlateDb"',
        f"path = {json.dumps(path)}",
        "",
        "[data.storage.object_store]",
        'type = "Local"',
        f"path = {json.dumps(str(store_path or storage['object_store']['path']))}",
        "",
        "[[params.recall]]",
    ]
    effective_settings = settings_path or storage.get("settings_path")
    if effective_settings:
        lines.insert(3, f"settings_path = {json.dumps(str(effective_settings))}")
    values = {
        "dataset": entry["name"],
        "num_queries": str(count),
        "query_concurrency": "1",
        "block_cache_bytes": str(1024**3),
        "phases": "WARM",
        "base_file": entry["files"]["base"]["path"],
        "query_file": entry["files"]["queries"]["path"],
        "ground_truth_file": entry["files"]["groundtruth"]["path"],
    }
    if nprobe_override is not None:
        bench.require(
            type(nprobe_override) is int and 1 <= nprobe_override <= 16384,
            "Invalid nprobe override",
        )
        values["nprobe"] = str(nprobe_override)
    elif "nprobe" in params:
        values["nprobe"] = params["nprobe"]
    lines.extend(f"{key} = {json.dumps(value)}" for key, value in values.items())
    text = "\n".join(lines) + "\n"
    tomllib.loads(text)
    return text, int(values["nprobe"]) if "nprobe" in values else None


def parse_capi(path, dimension=128):
    lifecycle = {}
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        bench.require(
            reader.fieldnames
            == [
                "event",
                "round",
                "query",
                "warmup",
                "latency_ms",
                "rank",
                "id",
                "distance",
            ]
            + [f"v{i}" for i in range(dimension)],
            "Invalid C API header",
        )
        current = None
        for record in reader:
            elapsed = float(record["latency_ms"])
            if record["event"] != "sample":
                event = record["event"]
                bench.require(
                    event in ("open", "prepare", "close")
                    and event not in lifecycle
                    and math.isfinite(elapsed)
                    and elapsed >= 0,
                    "Invalid lifecycle event",
                )
                lifecycle[event] = elapsed
                continue
            bench.require(record["warmup"] in ("0", "1"), "Invalid warmup marker")
            key = int(record["round"]), int(record["query"])
            if current is not None and key != (current["round"], current["query"]):
                yield current
                current = None
            if current is None:
                current = {
                    "round": key[0],
                    "query": key[1],
                    "warmup": record["warmup"] == "1",
                    "latency_ms": elapsed,
                    "hits": [],
                }
            bench.require(
                int(record["rank"]) == len(current["hits"])
                and elapsed == current["latency_ms"]
                and (record["warmup"] == "1") == current["warmup"],
                "Invalid rank or sample timing",
            )
            current["hits"].append(
                {
                    "id": int(record["id"]),
                    "distance": float(record["distance"]),
                    "embedding": [float(record[f"v{i}"]) for i in range(dimension)],
                }
            )
        if current is not None:
            yield current
    bench.require(
        set(lifecycle) == {"open", "prepare", "close"}, "Missing C API lifecycle"
    )
    yield {
        "type": "lifecycle",
        **{f"{key}_ms": value for key, value in lifecycle.items()},
    }


def parse_opendata(path):
    with path.open() as stream:
        for line in stream:
            yield json.loads(line)


def validate_records(hits, query, base, k=10):
    bench.require(
        len(hits) == k
        and all(type(hit["id"]) is int and 0 <= hit["id"] < len(base) for hit in hits)
        and len({hit["id"] for hit in hits}) == k,
        "Invalid returned IDs/count",
    )
    returned = np.asarray([hit["embedding"] for hit in hits], dtype=np.float64)
    original = base[[hit["id"] for hit in hits]].astype(np.float64)
    bench.require(
        returned.shape == original.shape and np.array_equal(returned, original),
        "Original vectors differ",
    )
    actual = np.sum((original - np.asarray(query, dtype=np.float64)) ** 2, axis=1)
    bench.require(
        all(
            math.isfinite(hit["distance"])
            and math.isclose(
                hit["distance"], float(distance), rel_tol=3e-5, abs_tol=1e-4
            )
            for hit, distance in zip(hits, actual)
        )
        and all(a["distance"] <= b["distance"] for a, b in itertools.pairwise(hits)),
        "Invalid squared L2 distances/order",
    )


def summarize_records(records, base, queries, truth, warmup, rounds, expected=None):
    samples, first, identity, lifecycle = [], {}, None, None
    for record in records:
        kind = record.get("type", "sample")
        if kind == "identity":
            bench.require(
                identity is None and not samples, "Duplicate or late identity"
            )
            identity = record
            continue
        if kind == "lifecycle":
            bench.require(lifecycle is None, "Duplicate lifecycle")
            lifecycle = record
            continue
        bench.require(kind == "sample" and lifecycle is None, "Unknown or late sample")
        round_id, query_id = divmod(len(samples), len(queries))
        bench.require(
            type(record["round"]) is int
            and type(record["query"]) is int
            and (record["round"], record["query"]) == (round_id, query_id)
            and type(record["warmup"]) is bool
            and record["warmup"] == (round_id < warmup),
            "Missing, reordered or repeated sample",
        )
        latency = record["latency_ms"]
        bench.require(
            type(latency) in (int, float) and math.isfinite(latency) and latency > 0,
            "Invalid latency",
        )
        hits = record["hits"]
        validate_records(hits, queries[query_id], base)
        ranked = [{"id": hit["id"], "distance": hit["distance"]} for hit in hits]
        if query_id in first:
            layers.compare_hits(ranked, first[query_id])
        else:
            first[query_id] = ranked
        if expected is not None:
            layers.compare_hits(ranked, expected[query_id])
        samples.append(
            {key: record[key] for key in ("round", "query", "warmup", "latency_ms")}
        )
    bench.require(
        len(samples) == len(queries) * (warmup + rounds) and lifecycle is not None,
        "Incomplete measurements",
    )
    score, _ = sift.recall([first[i] for i in range(len(queries))], truth)
    return {
        **sift.statistics(samples),
        "recall_at_10": score,
        "lifecycle": lifecycle,
        "identity": identity,
        "full_records_verified": len(samples) * 10,
        "repeat_parity": True,
    }, samples


def run_worker(
    root,
    name,
    binary,
    arguments,
    cwd,
    timeout,
    limits=None,
    worker_threads=4,
    sql_timing=False,
    private_store=None,
    opendata_counters=False,
):
    directory = root / name
    directory.mkdir(mode=0o700)
    env = dict(
        os.environ,
        OMP_NUM_THREADS="1",
        RUST_LOG="warn",
        VORTEX_INDEX_TIMING="1" if sql_timing else "0",
        RAYON_NUM_THREADS=str(worker_threads),
        TOKIO_WORKER_THREADS=str(worker_threads),
    )
    for key in (
        "SIFT_BENCH_PHASE_BARRIER",
        "SIFT_BENCH_METRICS_PATH",
        "SIFT_BENCH_RECORD_COUNTERS",
    ):
        env.pop(key, None)
    if limits:
        env.update(
            SIFT_BENCH_PHASE_BARRIER="1",
            SIFT_BENCH_METRICS_PATH=str(directory / "metrics.jsonl"),
            SIFT_BENCH_RECORD_COUNTERS="1" if opendata_counters else "0",
        )
    command = [
        "/usr/bin/time",
        "-f",
        "%M %U %S %P %e",
        "-o",
        str(directory / "time.txt"),
        str(binary),
        *map(str, arguments(directory)),
    ]
    before = os.getloadavg()
    start = time.perf_counter()
    constraints = None
    if limits:
        constraints = resources.run_constrained(
            command,
            cwd,
            env,
            directory,
            limits["cpu"],
            limits["memory_mib"],
            timeout,
            limits["warmup"],
            limits["rounds"],
            private_store,
        )
    else:
        with (directory / "stdout.log").open("x") as stdout, (
            directory / "stderr.log"
        ).open("x") as stderr:
            process = subprocess.run(
                command,
                cwd=cwd,
                env=env,
                stdout=stdout,
                stderr=stderr,
                timeout=timeout,
                check=False,
            )
        bench.require(
            process.returncode == 0,
            f"{name} failed: {(directory / 'stderr.log').read_text()[-3000:]}",
        )
    rss, user, system, cpu, elapsed = (
        (directory / "time.txt").read_text().strip().split()
    )
    metadata = {
        "command": command,
        "cwd": str(cwd),
        "peak_rss_mib": int(rss) / 1024,
        "cpu_user_seconds": float(user),
        "cpu_system_seconds": float(system),
        "cpu_percent": cpu,
        "wall_seconds": float(elapsed),
        "orchestrator_wall_seconds": time.perf_counter() - start,
        "load_average_before": before,
        "load_average_after": os.getloadavg(),
        "environment": {
            key: env[key]
            for key in (
                "OMP_NUM_THREADS",
                "RAYON_NUM_THREADS",
                "TOKIO_WORKER_THREADS",
                "VORTEX_INDEX_TIMING",
                *(("SIFT_BENCH_RECORD_COUNTERS",) if limits else ()),
            )
        },
        "logged_errors": [
            line
            for filename in ("stdout.log", "stderr.log")
            for line in (directory / filename).read_text().splitlines()
            if "ERROR" in line
        ],
        "constraints": constraints,
    }
    bench.write_json(directory / "resources.json", metadata)
    return directory, metadata


def cache_counters(path, warmup, rounds):
    snapshots = list(parse_opendata(path))
    bench.require(
        [item["phase"] for item in snapshots] == resources.phases(warmup, rounds),
        "Incomplete cache counter phases",
    )
    previous = {}
    for item in snapshots:
        bench.require(
            item["type"] == "counter_snapshot"
            and item["format_version"] == 1
            and item.get("recording", True)
            and all(
                type(value) in (int, float) and math.isfinite(value) and value >= 0
                for value in item["counters"].values()
            ),
            "Invalid cache counters",
        )
        counters = item["counters"]
        missing = previous.keys() - counters.keys()
        bench.require(
            not missing,
            f"Metric counter disappeared at {item['phase']}: {sorted(missing)}",
        )
        decreased = [key for key, value in previous.items() if counters[key] < value]
        bench.require(
            not decreased,
            f"Metric counter decreased at {item['phase']}: {sorted(decreased)}",
        )
        previous = counters
    by_phase = {item["phase"]: item["counters"] for item in snapshots}
    deltas = []
    for round_id in range(warmup + rounds):
        first, last = (
            by_phase[f"before_round_{round_id}"],
            by_phase[f"after_round_{round_id}"],
        )
        counts = {key: value - first.get(key, 0) for key, value in last.items()}
        deltas.append(
            {"round": round_id, "warmup": round_id < warmup, "counters": counts}
        )
    measured = {}
    for item in deltas[warmup:]:
        for key, value in item["counters"].items():
            measured[key] = measured.get(key, 0) + value
    cache_access = {}
    for key, value in measured.items():
        identity = json.loads(key)
        if identity["name"] == "slatedb.db_cache.access_count":
            labels = identity["labels"]
            kind, outcome = labels["entry_kind"], labels["result"]
            bench.require(outcome in ("hit", "miss"), "Unexpected cache outcome")
            counts = cache_access.setdefault(kind, {"hit": 0, "miss": 0})
            counts[outcome] += value
    bench.require(
        cache_access.get("data_block"), "Missing SlateDB data-block cache metrics"
    )
    for counts in cache_access.values():
        total = counts["hit"] + counts["miss"]
        counts["hit_fraction"] = counts["hit"] / total if total else None
    return {
        "snapshots": snapshots,
        "rounds": deltas,
        "measured": measured,
        "measured_cache_access": cache_access,
        "semantics": "SlateDB fetch hits include requests served by an existing in-flight load; these are not query-answer cache hits",
    }


def sql_cache_hits(path, count):
    events = [
        json.loads(line)
        for line in path.read_text().splitlines()
        if line.startswith('{"event":"vortex_index_search_timing"')
    ]
    bench.require(
        len(events) == count * 2, "Missing or duplicate SQL cache diagnostics"
    )
    bench.require(
        all(event["validation_mode"] == "snapshot" for event in events),
        "Diagnostic validation mode differs",
    )
    for field in ("provider_cache_hit", "snapshot_cache_hit"):
        bench.require(
            events[0].get(field) is False
            and all(event.get(field) is True for event in events[1:]),
            f"Expected {field}: initial cache miss followed by hits",
        )
    measured = events[count:]
    return {
        "queries": count,
        "warmup_rounds": 1,
        "rounds": 1,
        "provider_cache_hits": sum(event["provider_cache_hit"] for event in measured),
        "snapshot_cache_hits": sum(event["snapshot_cache_hit"] for event in measured),
        "initial_provider_cache_hit": events[0]["provider_cache_hit"],
        "initial_snapshot_cache_hit": events[0]["snapshot_cache_hit"],
        "semantics": "Separate diagnostic run; handle/source hits, not posting-block or OS page-cache hit rates",
    }


def qualify_recall(modes, minimum, tolerance):
    values = [result["recall_at_10"] for result in modes.values()]
    bench.require(
        len(values) == 2 and min(values) >= minimum,
        "Recall below requested quality floor",
    )
    gap = max(values) - min(values)
    bench.require(
        tolerance is None or gap <= tolerance + 1e-12,
        f"Recall gap {gap:.6f} exceeds tolerance {tolerance}; calibrate before comparing",
    )
    return {
        "minimum": minimum,
        "max_absolute_gap": tolerance,
        "observed_gap": gap,
        "qualified": True,
    }


def run(cli):
    bench.require(
        1 <= cli.queries <= 1000
        and 1 <= cli.warmup_rounds <= 100
        and 1 <= cli.rounds <= 100,
        "Invalid rounds/count",
    )
    bench.require(math.isfinite(cli.timeout) and cli.timeout > 0, "Invalid timeout")
    bench.require(
        (cli.cpu is None) == (cli.memory_mib is None),
        "CPU and memory limits must be specified together",
    )
    bench.require(
        0 < cli.minimum_recall <= 1
        and (cli.recall_tolerance is None or 0 <= cli.recall_tolerance <= 1),
        "Invalid recall qualification",
    )
    limits = (
        None
        if cli.cpu is None
        else {
            "cpu": cli.cpu,
            "memory_mib": cli.memory_mib,
            "warmup": cli.warmup_rounds,
            "rounds": cli.rounds,
        }
    )
    worker_threads = 1 if limits else 4
    tolerance = (
        cli.recall_tolerance
        if cli.recall_tolerance is not None
        else (0.002 if limits else None)
    )
    root = cli.output_dir.resolve()
    root.mkdir(mode=0o700)
    shutil.copyfile(Path(__file__), root / "benchmark-script.py")
    shutil.copyfile(Path(resources.__file__), root / "resource-script.py")
    entry, base, queries, truth = sift.load_inputs(
        cli.input_manifest, cli.dataset, cli.queries
    )
    fixture = sift.load_fixture(cli.fixture.resolve(), entry)
    bench.require(
        json.loads((cli.fixture / "inputs.json").read_text()) == entry,
        "Fixture input identity differs",
    )
    bench.require(fixture["reference"]["snapshot"]["files"], "Empty snapshot")
    build = json.loads((cli.capi.parent / "build.json").read_text())
    bench.require(
        build["sha256"] == bench.fingerprint(cli.capi), "C API binary changed"
    )
    bench.require(
        fixture["artifact_bytes"] <= build["retained_artifact_budget_bytes"],
        "Snapshot exceeds compiled budget",
    )
    parity_samples = json.loads(cli.parity_samples.read_text())
    expected = sift.expected_results(
        parity_samples, len({sample["query"] for sample in parity_samples})
    )
    bench.require(len(expected) >= cli.queries, "Missing query results")
    expected = expected[: cli.queries]
    bench.write_json(root / "inputs.json", entry)
    settings_path = None
    if cli.opendata_maintenance == "off":
        settings_path = root / "slatedb-no-maintenance.json"
        bench.write_json(
            settings_path,
            {"compactor_options": None, "garbage_collector_options": None},
        )
    private_store = None
    if limits:
        original_storage = tomllib.loads(cli.opendata_config.read_text())["data"][
            "storage"
        ]
        private_store = (
            (cli.opendata_root / original_storage["object_store"]["path"]).resolve(),
            root / "opendata" / "private-store",
        )
    config, nprobe = opendata_config(
        cli.opendata_config,
        entry,
        cli.queries,
        settings_path,
        cli.opendata_nprobe,
        private_store[1] if private_store else None,
    )
    (root / "opendata.toml").write_text(config)
    sql_path = root / "query.sql"
    sql_path.write_text(
        query_sql(cli.fixture.resolve() / "index.json", cli.probes, fixture["pages"])
    )
    query_path = root / "queries.f32bin"
    with query_path.open("xb") as output:
        output.write(struct.pack("<II", len(queries), 128))
        for query in queries:
            output.write(struct.pack("<128f", *query))
    report = {
        "status": "running",
        "dataset": cli.dataset,
        "inputs": entry,
        "queries": len(queries),
        "warmup_rounds": cli.warmup_rounds,
        "rounds": cli.rounds,
        "method": {
            "output": "top-10 original ID, squared L2 distance, complete Float32[128] embedding; no label",
            "timing": "search API through caller-owned full records; profiler disabled; binding/query construction, validation, output and teardown excluded",
            "reuse": "one OpenData database / one DuckDB connection and prepared handle for all rounds",
            "concurrency": f"one synchronous caller; OpenData Rayon/Tokio={worker_threads}, DuckDB threads=1, OMP=1",
            "warmup": "one or more complete query-set passes; no OS cache eviction",
            "cache": "OpenData block cache versus DuckDB retained-artifact budget; not equivalent cache capacities or RSS caps",
            "store": "existing local writable SlateDB; no ingest; background compaction may run; Vortex generation sealed",
            "opendata_maintenance": cli.opendata_maintenance,
            "resource_limits": limits,
            "cache_diagnostics": (
                "separate one-warmup/one-measured runs; both main workers disable recorder/tracing"
                if limits
                else None
            ),
            "quality_floor": cli.minimum_recall,
            "recall_tolerance": tolerance,
        },
        "binaries": {
            "duckdb_capi": build,
            "opendata": {
                "path": str(cli.opendata.resolve()),
                "sha256": bench.fingerprint(cli.opendata),
                "source_base_revision": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=cli.opendata_root, text=True
                ).strip(),
                "benchmark_sources": {
                    name: bench.fingerprint(cli.opendata_root / name)
                    for name in (
                        "vector/bench/src/recall.rs",
                        "vector/bench/src/recall/repeat.rs",
                        "vector/bench/src/bin/sift_repeat.rs",
                        "vector/bench/src/recall/repeat/diagnostics.rs",
                        "vector/bench/Cargo.toml",
                        "Cargo.lock",
                    )
                },
            },
        },
        "parity_samples": {
            "path": str(cli.parity_samples.resolve()),
            "sha256": bench.fingerprint(cli.parity_samples),
        },
        "configuration": {
            "nprobe": nprobe,
            "internal_results": cli.probes,
            "max_check": 32768,
            "search_pages": fixture["pages"],
            "opendata_block_cache_bytes": 1024**3,
            "snapshot_artifact_bytes": fixture["artifact_bytes"],
        },
        "modes": {},
        "workers": {},
    }
    bench.write_json(root / "summary.json", report)
    for mode in cli.order:
        print(
            f"Measuring {mode}: {len(queries)} queries, {cli.warmup_rounds} warmup + {cli.rounds} measured",
            flush=True,
        )
        if mode == "sql-snapshot-capi":
            directory, resource = run_worker(
                root,
                mode,
                cli.capi.resolve(),
                lambda output: [
                    sql_path,
                    query_path,
                    output / "samples.csv",
                    cli.warmup_rounds,
                    cli.rounds,
                    10,
                ],
                root,
                cli.timeout,
                limits,
                worker_threads,
            )
            records = parse_capi(directory / "samples.csv")
        else:
            directory, resource = run_worker(
                root,
                mode,
                cli.opendata.resolve(),
                lambda output: [
                    root / "opendata.toml",
                    output / "samples.jsonl",
                    cli.warmup_rounds,
                    cli.rounds,
                ],
                cli.opendata_root.resolve(),
                cli.timeout,
                limits,
                worker_threads,
                private_store=private_store,
            )
            records = parse_opendata(directory / "samples.jsonl")
        result, samples = summarize_records(
            records,
            base,
            queries,
            truth,
            cli.warmup_rounds,
            cli.rounds,
            expected if mode == "sql-snapshot-capi" else None,
        )
        if mode == "opendata":
            identity = result["identity"]
            bench.require(
                identity is not None
                and (nprobe is None or identity["nprobe"] == nprobe)
                and identity["block_cache_bytes"] == 1024**3
                and identity["queries"] == len(queries)
                and identity["warmup_rounds"] == cli.warmup_rounds
                and identity["rounds"] == cli.rounds,
                "OpenData effective configuration differs",
            )
            nprobe = identity["nprobe"]
            report["configuration"]["nprobe"] = nprobe
            if limits:
                bench.require(
                    identity["rayon_threads"] == worker_threads
                    and identity["tokio_workers"] == worker_threads,
                    "OpenData worker thread counts differ",
                )
                bench.require(
                    not identity["storage_counters_enabled"],
                    "Diagnostic recorder enabled in primary timing",
                )
        bench.write_json(directory / "timings.json", samples)
        report["modes"][mode] = result
        report["workers"][mode] = resource
        bench.write_json(root / "summary.json", report)
        print(json.dumps({"mode": mode, **result}), flush=True)
    report["quality"] = qualify_recall(report["modes"], cli.minimum_recall, tolerance)
    bench.write_json(root / "summary.json", report)
    if limits:
        diagnostic_limits = {**limits, "warmup": 1, "rounds": 1}
        directory, resource = run_worker(
            root,
            "sql-cache-diagnostic",
            cli.capi.resolve(),
            lambda output: [sql_path, query_path, output / "samples.csv", 1, 1, 10],
            root,
            cli.timeout,
            diagnostic_limits,
            worker_threads,
            sql_timing=True,
        )
        _, samples = summarize_records(
            parse_capi(directory / "samples.csv"), base, queries, truth, 1, 1, expected
        )
        bench.write_json(directory / "timings.json", samples)
        report["sql_cache_diagnostic"] = sql_cache_hits(
            directory / "stderr.log", len(queries)
        )
        report["workers"]["sql-cache-diagnostic"] = resource
        diagnostic_store = (
            private_store[0],
            root / "opendata-cache-diagnostic" / "private-store",
        )
        diagnostic_config, _ = opendata_config(
            cli.opendata_config,
            entry,
            cli.queries,
            settings_path,
            nprobe,
            diagnostic_store[1],
        )
        diagnostic_path = root / "opendata-diagnostic.toml"
        diagnostic_path.write_text(diagnostic_config)
        directory, resource = run_worker(
            root,
            "opendata-cache-diagnostic",
            cli.opendata.resolve(),
            lambda output: [diagnostic_path, output / "samples.jsonl", 1, 1],
            cli.opendata_root.resolve(),
            cli.timeout,
            diagnostic_limits,
            worker_threads,
            private_store=diagnostic_store,
            opendata_counters=True,
        )
        diagnostic_result, samples = summarize_records(
            parse_opendata(directory / "samples.jsonl"), base, queries, truth, 1, 1
        )
        bench.require(
            diagnostic_result["identity"]["storage_counters_enabled"]
            and diagnostic_result["identity"]["nprobe"] == nprobe
            and diagnostic_result["identity"]["rayon_threads"] == worker_threads
            and diagnostic_result["identity"]["tokio_workers"] == worker_threads
            and diagnostic_result["recall_at_10"]
            == report["modes"]["opendata"]["recall_at_10"],
            "OpenData diagnostic identity/recall differs",
        )
        bench.write_json(directory / "timings.json", samples)
        report["opendata_cache_diagnostic"] = {
            "cache_counters": cache_counters(directory / "metrics.jsonl", 1, 1),
            "timing_not_headline": diagnostic_result,
        }
        report["workers"]["opendata-cache-diagnostic"] = resource
    sift.load_fixture(cli.fixture.resolve(), entry)
    report["status"] = "ok"
    bench.write_json(root / "summary.json", report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser(
        "build-capi",
        help="Relink qualified libraries; does not rebuild or change the extension",
    )
    for name in ("link-manifest", "sdk-root", "duckdb-include", "output-dir"):
        build.add_argument(f"--{name}", type=Path, required=True)
    build.add_argument("--artifact-budget-mib", type=int, required=True)
    compare = commands.add_parser("run")
    compare.add_argument("--dataset", choices=("sift100k", "sift1m"), required=True)
    for name in (
        "input-manifest",
        "fixture",
        "capi",
        "opendata",
        "opendata-root",
        "opendata-config",
        "parity-samples",
        "output-dir",
    ):
        compare.add_argument(f"--{name}", type=Path, required=True)
    compare.add_argument("--probes", type=int, choices=(64, 512), required=True)
    compare.add_argument("--queries", type=int, default=1000)
    compare.add_argument("--warmup-rounds", type=int, default=1)
    compare.add_argument("--rounds", type=int, default=3)
    compare.add_argument("--timeout", type=float, default=7200)
    compare.add_argument(
        "--cpu", type=int, help="Pin both workers to this Linux logical CPU"
    )
    compare.add_argument(
        "--memory-mib",
        type=int,
        help="Equal cgroup MemoryMax; disables swap, requires --cpu",
    )
    compare.add_argument(
        "--opendata-nprobe",
        type=int,
        help="Explicit quality calibration; original config is not changed",
    )
    compare.add_argument("--minimum-recall", type=float, default=0.99)
    compare.add_argument(
        "--recall-tolerance",
        type=float,
        help="Maximum absolute Recall@10 difference; constrained default: 0.002",
    )
    compare.add_argument(
        "--opendata-maintenance", choices=("default", "off"), default="default"
    )
    compare.add_argument(
        "--order",
        nargs=2,
        choices=("opendata", "sql-snapshot-capi"),
        default=["sql-snapshot-capi", "opendata"],
    )
    cli = parser.parse_args(argv)
    if cli.command == "build-capi":
        report = build_capi(
            cli.link_manifest,
            cli.sdk_root,
            cli.duckdb_include,
            cli.output_dir,
            cli.artifact_budget_mib,
        )
    else:
        bench.require(
            set(cli.order) == {"opendata", "sql-snapshot-capi"},
            "Measure both engines once",
        )
        bench.require(not cli.output_dir.exists(), "Output directory already exists")
        try:
            report = run(cli)
        except Exception as error:
            path = cli.output_dir / "summary.json"
            if cli.output_dir.is_dir():
                partial = json.loads(path.read_text()) if path.is_file() else {}
                partial.update(status="failed", error=str(error))
                bench.write_json(path, partial)
            raise
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

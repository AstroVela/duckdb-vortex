#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Measure static index SQL against exact Vortex scans in persistent processes."""

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import random
import statistics
import struct
import subprocess
import time
from pathlib import Path

PHASES = (
    "reference_check_ms",
    "source_validation_ms",
    "provider_open_ms",
    "native_search_ms",
    "hit_validation_ms",
    "take_ms",
    "result_materialization_ms",
)
BUILD_VECTOR_LIMIT = 256 * 1024 * 1024
POSTING_PAGE_BYTES = 4096
POSTING_BUFFER_LIMIT_BYTES = 256 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def fingerprint(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def f32(value):
    return struct.unpack("<f", struct.pack("<f", value))[0]


def generate(root, rows, dimension, query_count, seed, clusters):
    centers_rng, data_rng, query_rng = (random.Random(seed + i) for i in range(3))
    centers = [
        [centers_rng.gauss(0, 4) for _ in range(dimension)] for _ in range(clusters)
    ]

    def vector(rng, center):
        return [f32(value + rng.gauss(0, 1)) for value in center]

    paths = [root / "even.csv", root / "odd.csv"]
    with paths[0].open("w", newline="") as even, paths[1].open("w", newline="") as odd:
        writers = [csv.writer(even), csv.writer(odd)]
        for writer in writers:
            writer.writerow(["id", "embedding"])
        for row in range(rows):
            writers[row % 2].writerow(
                [row, json.dumps(vector(data_rng, centers[row % clusters]))]
            )
    queries = [
        vector(query_rng, centers[query_rng.randrange(clusters)])
        for _ in range(query_count)
    ]
    write_json(root / "queries.json", queries)
    return paths, queries


def percentile(values, fraction):
    values = sorted(values)
    require(bool(values), "Cannot summarize an empty sample")
    position = (len(values) - 1) * fraction
    low, high = math.floor(position), math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


def summarize(values):
    require(all(math.isfinite(v) and v >= 0 for v in values), "Invalid timings")
    return {
        "samples": len(values),
        "p50_ms": statistics.median(values),
        "p95_ms": percentile(values, 0.95),
        "min_ms": min(values),
        "max_ms": max(values),
    }


def validate_hits(hits, query, k, rows):
    require(len(hits) == k, "Search did not return k rows")
    ids = [hit["id"] for hit in hits]
    require(len(set(ids)) == k, "Search returned duplicate IDs")
    for hit in hits:
        require(
            isinstance(hit["id"], int)
            and 0 <= hit["id"] < rows
            and hit["label"] == f"row-{hit['id']}",
            "Ranked row identity mismatch",
        )
        require(len(hit["embedding"]) == len(query), "Vector dimension mismatch")
        actual = math.fsum((a - b) ** 2 for a, b in zip(hit["embedding"], query))
        require(
            math.isfinite(hit["distance"])
            and math.isclose(hit["distance"], actual, rel_tol=3e-5, abs_tol=1e-4),
            "Squared-L2 distance does not match the returned original row",
        )
    require(
        all(a["distance"] <= b["distance"] for a, b in zip(hits, hits[1:])),
        "Search results are not ordered by distance",
    )
    return [(hit["id"], hit["distance"]) for hit in hits]


def _valid_timing_ms(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def parse_phases(stderr, count):
    events = []
    for line in stderr.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            isinstance(event, dict)
            and event.get("event") == "vortex_index_search_timing"
        ):
            version = event.get("format_version")
            require(
                type(version) is int and version == 1, "Unsupported timing event format"
            )
            phases = event.get("phases")
            require(isinstance(phases, dict), "Timing event phases must be an object")
            require(set(phases) == set(PHASES), "Incomplete timing phases")
            require(
                all(_valid_timing_ms(ms) for ms in phases.values()),
                "Invalid phase timings",
            )
            total_ms = event.get("total_ms")
            require(_valid_timing_ms(total_ms), "Invalid total timing")
            if "provider_cache_hit" in event:
                require(
                    type(event["provider_cache_hit"]) is bool,
                    "Invalid provider cache status",
                )
            require(
                math.isclose(
                    sum(float(ms) for ms in phases.values()), total_ms, abs_tol=1e-6
                ),
                "Phase timings do not sum to the Rust operation total",
            )
            events.append(event)
    require(
        len(events) == count,
        "Expected one timing event per query. Rebuild with the timing-enabled Vortex "
        "revision, or use --skip-stage-timings explicitly for older artifacts.",
    )
    return events


def search_workload(engine, args, queries, inventory, reference, rounds):
    def select(vector):
        vector = f"{vector}::FLOAT[{args.dimension}]"
        if engine == "exact":
            return (
                "SELECT id, label, embedding, "
                f"pow(array_distance(embedding, {vector}), 2) AS distance "
                f"FROM read_vortex({inventory}) ORDER BY distance, id LIMIT {args.k};"
            )
        return (
            'SELECT "row".id AS id, "row".label AS label, '
            '"row".embedding AS embedding, distance '
            f"FROM vortex_index_search({quote(reference)}, {vector}::FLOAT[], {args.k}) "
            "ORDER BY rank;"
        )

    prepared = args.execution_mode == "prepared"
    setup = [f"PREPARE measured_query AS {select('$1')}"] if prepared else []
    statements, labels = [], []
    for round_id in range(rounds):
        for query_id, query in enumerate(queries):
            label = f"round-{round_id}-query-{query_id}"
            vector = "[" + ",".join(map(str, query)) + "]"
            sql = (
                f"EXECUTE measured_query({vector}::FLOAT[{args.dimension}]);"
                if prepared
                else select(vector)
            )
            statements.append((label, sql))
            labels.append((label, round_id, query_id))
    return setup, statements, labels


class Runner:
    def __init__(self, args, root):
        self.args, self.root = args, root
        self.workers = {}

    def run(self, name, statements, timings=False, setup=()):
        directory = self.root / name
        directory.mkdir()
        script = [
            "SET autoload_known_extensions=false;",
            "SET autoinstall_known_extensions=false;",
            f"SET threads={self.args.threads};",
        ]
        if self.args.extension:
            script.append(f"LOAD {quote(self.args.extension.resolve())};")
        script += [
            ".mode json",
            f".output {json.dumps(str(directory / 'identity.json'))}",
            "PRAGMA version;",
            ".output",
        ]
        script += list(setup)
        for label, sql in statements:
            script += [
                "SET enable_profiling='json';",
                f"SET profiling_output={quote(directory / (label + '.profile.json'))};",
                f".output {json.dumps(str(directory / (label + '.result.json')))}",
                sql,
                ".output",
                "PRAGMA disable_profiling;",
            ]
        script_path = directory / "queries.sql"
        script_path.write_text("\n".join(script) + "\n")
        command = [
            str(self.args.time_binary),
            "-f",
            "%M",
            "-o",
            str(directory / "peak-rss-kib.txt"),
            str(self.args.duckdb.resolve()),
            "-batch",
            "-bail",
        ]
        if self.args.extension:
            command.append("-unsigned")
        command.append(":memory:")
        env = dict(os.environ, OMP_NUM_THREADS="1", RUST_LOG="warn")
        env["VORTEX_INDEX_TIMING"] = "1" if timings else "0"
        print(f"Running {name}: {len(statements)} statements", flush=True)
        load_before = os.getloadavg()
        start = time.perf_counter()
        with (
            script_path.open() as stdin,
            (directory / "stdout.log").open("w") as stdout,
            (directory / "stderr.log").open("w") as stderr,
        ):
            result = subprocess.run(
                command,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                env=env,
                timeout=self.args.timeout,
            )
        require(
            result.returncode == 0,
            f"{name} failed; see {directory}: "
            + (directory / "stderr.log").read_text()[-4000:],
        )
        self.workers[name] = {
            "wall_seconds_including_startup": time.perf_counter() - start,
            "peak_rss_mib": int((directory / "peak-rss-kib.txt").read_text()) / 1024,
            "load_average_before": load_before,
            "load_average_after": os.getloadavg(),
            "engine": json.loads((directory / "identity.json").read_text()),
        }
        print(f"Finished {name}: {self.workers[name]}", flush=True)
        return directory


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, required=True)
    parser.add_argument("--extension", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--dimension", type=int, default=128)
    parser.add_argument("--queries", type=int, default=30)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup-rounds", type=int, default=1)
    parser.add_argument("--seed", type=int, default=20261001)
    parser.add_argument("--clusters", type=int, default=64)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--head-count", type=int, default=512)
    parser.add_argument("--posting-page-limit", type=int, default=256)
    parser.add_argument("--replicas", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument("--time-binary", type=Path, default=Path("/usr/bin/time"))
    parser.add_argument("--skip-stage-timings", action="store_true")
    parser.add_argument(
        "--execution-mode", choices=["ad-hoc", "prepared"], default="ad-hoc"
    )
    args = parser.parse_args(argv)
    require(64 <= args.rows <= 1_000_000, "Rows must be between 64 and 1,000,000")
    require(1 <= args.dimension <= 4096, "Invalid dimension")
    require(
        args.rows * args.dimension * 4 <= BUILD_VECTOR_LIMIT,
        "Input exceeds the current SQL factory's 256 MiB vector budget. "
        "1,000,000 x 128 needs a separately reviewed builder-budget change.",
    )
    require(32 <= args.head_count < args.rows, "Head count must be in 32..rows")
    require(1 <= args.posting_page_limit <= 4096, "Invalid posting page limit")
    require(1 <= args.replicas <= 8, "Invalid replica count")
    require(1 <= args.k <= min(4096, args.rows), "Invalid k")
    require(
        args.posting_page_limit * max(64, args.k) * POSTING_PAGE_BYTES
        <= POSTING_BUFFER_LIMIT_BYTES,
        "Query exceeds the current SQL factory's 256 MiB posting buffer budget",
    )
    require(
        args.queries > 0
        and args.rounds > 0
        and args.warmup_rounds >= 0
        and args.threads > 0
        and args.clusters > 0
        and math.isfinite(args.timeout)
        and args.timeout > 0,
        "Invalid run counts or timeout",
    )
    require(args.duckdb.is_file() and args.time_binary.is_file(), "Missing executable")
    if args.extension:
        require(args.extension.is_file(), "Missing extension artifact")
    root = args.output_dir.resolve()
    require(not any(char in str(root) for char in "\n\r\0"), "Unsupported output path")
    root.mkdir(parents=True, exist_ok=False)
    print(f"Generating {args.rows} x {args.dimension}, seed={args.seed}", flush=True)
    csv_paths, queries = generate(
        root, args.rows, args.dimension, args.queries, args.seed, args.clusters
    )
    files = [root / "even.vortex", root / "odd.vortex"]
    reference = root / "index.json"
    runner = Runner(args, root)
    prepare = []
    for index, (csv_path, file) in enumerate(zip(csv_paths, files)):
        prepare.append(
            (
                f"file-{index}",
                f"COPY (SELECT id, embedding::FLOAT[{args.dimension}] AS embedding, "
                f"'row-' || id AS label FROM read_csv({quote(csv_path)}, header=true, "
                "columns={'id':'BIGINT','embedding':'VARCHAR'})) "
                f"TO {quote(file)} (FORMAT vortex);",
            )
        )
    runner.run("prepare", prepare)
    build_options = {
        "format_version": 1,
        "dimension": args.dimension,
        "head_count": args.head_count,
        "posting_page_limit": args.posting_page_limit,
        "replicas": args.replicas,
    }
    inventory = "[" + ",".join(quote(file) for file in files) + "]"
    build_dir = runner.run(
        "build",
        [
            (
                "build",
                f"SELECT * FROM vortex_index_build({inventory}, {quote(reference)}, "
                f"'embedding', 'spfresh.static', {quote(json.dumps(build_options))});",
            )
        ],
    )
    profiles = json.loads((build_dir / "build.profile.json").read_text())
    build_ms = profiles["latency"] * 1000
    samples, results = [], {}
    for engine in ("exact", "ann"):
        setup, statements, labels = search_workload(
            engine,
            args,
            queries,
            inventory,
            reference,
            args.warmup_rounds + args.rounds,
        )
        directory = runner.run(engine, statements, setup=setup)
        for index, (label, round_id, query_id) in enumerate(labels):
            profile = json.loads((directory / (label + ".profile.json")).read_text())
            require(
                profile["query_name"].strip().rstrip(";")
                == statements[index][1].strip().rstrip(";"),
                "Profiling output does not match its query",
            )
            hits = json.loads((directory / (label + ".result.json")).read_text())
            identity = validate_hits(hits, queries[query_id], args.k, args.rows)
            key = (engine, query_id)
            if key in results:
                require(results[key] == identity, f"Unstable repeated results: {key}")
            else:
                results[key] = identity
            ms = profile["latency"] * 1000
            require(math.isfinite(ms) and ms > 0, "Missing or invalid query latency")
            samples.append(
                {
                    "engine": engine,
                    "round": round_id,
                    "query": query_id,
                    "warmup": round_id < args.warmup_rounds,
                    "first_in_process": index == 0,
                    "latency_ms": ms,
                }
            )

    recalls = [
        len(
            {row for row, _ in results["ann", q]}
            & {row for row, _ in results["exact", q]}
        )
        / args.k
        for q in range(args.queries)
    ]
    engines = {}
    for engine in ("exact", "ann"):
        measured = [s for s in samples if s["engine"] == engine and not s["warmup"]]
        values = [s["latency_ms"] for s in measured]
        engines[engine] = {
            **summarize(values),
            "serial_qps_from_query_latency": len(values) * 1000 / sum(values),
            "first_query_in_fresh_process_ms": next(
                s["latency_ms"] for s in samples if s["engine"] == engine
            ),
            "round_p50_ms": [
                statistics.median(
                    [s["latency_ms"] for s in measured if s["round"] == r]
                )
                for r in range(args.warmup_rounds, args.warmup_rounds + args.rounds)
            ],
        }
    write_json(root / "samples.json", samples)
    phase_report = None
    phase_first_query = None
    phase_after_first_query = None
    cache_status = None
    if not args.skip_stage_timings:
        setup, statements, labels = search_workload(
            "ann",
            args,
            queries,
            inventory,
            reference,
            2 if args.execution_mode == "prepared" else 1,
        )
        diagnostic = runner.run(
            "diagnostic",
            statements,
            timings=True,
            setup=setup,
        )
        events = parse_phases((diagnostic / "stderr.log").read_text(), len(labels))
        for (label, _, query_id), event in zip(labels, events):
            hits = json.loads((diagnostic / f"{label}.result.json").read_text())
            require(
                validate_hits(hits, queries[query_id], args.k, args.rows)
                == results["ann", query_id],
                "Diagnostics changed ranked results",
            )
        write_json(root / "stages.json", events)
        write_json(
            root / "stage-samples.json",
            [
                {"round": r, "query": q, "first_in_process": i == 0, "event": event}
                for i, ((_, r, q), event) in enumerate(zip(labels, events))
            ],
        )
        phase_report = {
            phase: summarize([event["phases"][phase] for event in events])
            for phase in PHASES
        }
        phase_first_query = {
            "total_ms": events[0]["total_ms"],
            "phases": events[0]["phases"],
        }
        if len(events) > 1:
            phase_after_first_query = {
                phase: summarize([event["phases"][phase] for event in events[1:]])
                for phase in PHASES
            }
        if all("provider_cache_hit" in event for event in events):
            cache_status = {
                "samples": len(events),
                "hits": sum(event["provider_cache_hit"] for event in events),
                "first_query_hit": events[0]["provider_cache_hit"],
            }

    descriptor = json.loads(reference.read_text())
    generation = root / descriptor["generation"]["generation"]
    report = {
        "format_version": 1,
        "status": "ok",
        "parameters": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "method": {
            "generator": "Float32 Gaussian mixtures; shared centers, independent RNG streams",
            "latency": "DuckDB JSON profile latency; materialized SELECT; profiler enabled",
            "warmup": "Complete query-set rounds excluded from measured latency",
            "first_query": "Fresh process; OS page cache is uncontrolled",
            "qps": "Serial inverse query latency; not concurrent/client throughput",
            "rss": "GNU time peak RSS per worker, including engine startup",
            "stages": "Separate diagnostic process; successful Rust execution only; excludes SQL bind",
            "order": "Exact scan before ANN; timings are not run concurrently",
            "execution_mode": args.execution_mode,
            "prepare": "One owner per search worker in prepared mode; setup excluded from query latency",
        },
        "platform": {
            "python": platform.python_version(),
            "system": platform.platform(),
            "logical_cpus": os.cpu_count(),
            "allowed_cpus": len(os.sched_getaffinity(0)),
        },
        "inputs": {
            str(path.name): {"sha256": fingerprint(path), "bytes": path.stat().st_size}
            for path in csv_paths + files + [root / "queries.json"]
        },
        "artifacts": {
            "benchmark_script_sha256": fingerprint(Path(__file__)),
            "duckdb_sha256": fingerprint(args.duckdb),
            "extension_sha256": fingerprint(args.extension) if args.extension else None,
        },
        "build_ms": build_ms,
        "index_bytes": sum(
            path.stat().st_size for path in generation.rglob("*") if path.is_file()
        )
        + reference.stat().st_size,
        "recall_at_k": statistics.mean(recalls),
        "per_query_recall": recalls,
        "repeat_results_equal": True,
        "engines": engines,
        "stages": phase_report,
        "stages_first_query": phase_first_query,
        "stages_after_first_query": phase_after_first_query,
        "diagnostic_provider_cache": cache_status,
        "workers": runner.workers,
    }
    write_json(root / "summary.json", report)
    print(
        json.dumps(
            {
                "report": str(root / "summary.json"),
                "build_ms": build_ms,
                "index_bytes": report["index_bytes"],
                "recall_at_k": report["recall_at_k"],
                "engines": engines,
                "stages": phase_report,
            },
            indent=2,
            allow_nan=False,
        ),
        flush=True,
    )
    return report


if __name__ == "__main__":
    main()

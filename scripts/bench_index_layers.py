#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare native SPFresh, its Rust Provider and SQL on one frozen SQL fixture."""

import argparse
import csv
import json
import math
import os
import statistics
import struct
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import bench_index_sql as bench

REVISION = "5893eb61ee3b18610b6b00f1939be7dae1af8904"
NATIVE_MODES = ("cpp-reset", "cpp-reuse", "bridge")


def checked_artifact(path, artifact):
    bench.require(path.is_file(), f"Missing artifact: {path}")
    bench.require(
        path.stat().st_size == artifact["size"], f"Artifact size changed: {path}"
    )
    bench.require(
        f"sha256:{bench.fingerprint(path)}" == artifact["checksum"],
        f"Artifact content changed: {path}",
    )


def verify_fixture(fixture):
    root, source = fixture["root"], fixture["source"]
    for name, identity in source["inputs"].items():
        path = root / name
        bench.require(
            path.stat().st_size == identity["bytes"], f"Input size changed: {name}"
        )
        bench.require(
            bench.fingerprint(path) == identity["sha256"],
            f"Input content changed: {name}",
        )
    bench.require(
        bench.fingerprint(root / "index.json") == fixture["reference_sha256"],
        "Index reference changed",
    )
    checked_artifact(
        fixture["generation"] / "manifest.json",
        fixture["reference"]["generation"]["manifest"],
    )
    for artifact in fixture["manifest"]["artifacts"]:
        checked_artifact(
            fixture["generation"] / "artifacts" / artifact["path"], artifact
        )


def load_fixture(report_path):
    source = json.loads(report_path.read_text())
    bench.require(
        source.get("status") == "ok", "SQL source benchmark must have succeeded"
    )
    params = source["parameters"]
    bench.require(
        params["execution_mode"] == "prepared", "Use a prepared SQL source benchmark"
    )
    bench.require(params["threads"] == 1, "This comparison requires one thread")
    bench.require(
        0 <= params["warmup_rounds"] <= 100 and 1 <= params["rounds"] <= 100,
        "Invalid round counts",
    )
    bench.require(
        1 <= params["queries"] <= 256 and 1 <= params["k"] <= 4096,
        "Invalid query count or k",
    )
    root = report_path.resolve().parent
    reference = json.loads((root / "index.json").read_text())
    bench.require(reference["format_version"] == 1, "Unsupported reference format")
    generation_name = reference["generation"]["generation"]
    bench.require(
        generation_name not in ("", ".", "..")
        and Path(generation_name).name == generation_name,
        "Invalid generation path",
    )
    generation = root / generation_name
    checked_artifact(generation / "manifest.json", reference["generation"]["manifest"])
    manifest = json.loads((generation / "manifest.json").read_text())
    bench.require(
        manifest["snapshot"] == reference["snapshot"], "Manifest snapshot mismatch"
    )
    bench.require(
        manifest["generation"] == generation_name
        and manifest["backend"] == "spfresh.static"
        and manifest["backend_version"] == 1,
        "Unsupported generation",
    )
    for artifact in manifest["artifacts"]:
        path = Path(artifact["path"])
        bench.require(
            not path.is_absolute() and ".." not in path.parts, "Invalid artifact path"
        )
        checked_artifact(generation / "artifacts" / path, artifact)
    bundle = json.loads((generation / "artifacts/spfresh/bundle.json").read_text())
    bench.require(
        bundle["revision"] == REVISION
        and bundle["metric"] == "squared_l2"
        and bundle["value_type"] == "float32",
        "Unsupported native format",
    )
    bench.require(
        bundle["snapshot"] == reference["snapshot"], "Native snapshot mismatch"
    )
    shape = bundle["bundle"]
    bench.require(
        (shape["dimension"], shape["rows"], shape["posting_page_limit"])
        == (params["dimension"], params["rows"], params["posting_page_limit"]),
        "Native shape/options mismatch",
    )
    queries = json.loads((root / "queries.json").read_text())
    bench.require(
        len(queries) == params["queries"]
        and all(
            len(query) == params["dimension"]
            and all(
                type(value) in (int, float)
                and math.isfinite(value)
                and bench.f32(value) == value
                for value in query
            )
            for query in queries
        ),
        "Invalid Float32 queries",
    )
    mapping_bytes = (generation / "artifacts/spfresh/rows.bin").read_bytes()
    bench.require(
        mapping_bytes[:8] == b"VXSFROW1"
        and len(mapping_bytes) == 16 + 16 * params["rows"]
        and struct.unpack("<Q", mapping_bytes[8:16])[0] == params["rows"],
        "Invalid native row mapping",
    )
    mapping = list(struct.iter_unpack("<QQ", mapping_bytes[16:]))
    addresses = {}
    for entry in reference["snapshot"]["files"]:
        path = Path(entry["uri"])
        bench.require(
            path in (root / "even.vortex", root / "odd.vortex"),
            "Unexpected source inventory",
        )
        bench.require(
            entry["version"] == f"sha256:{source['inputs'][path.name]['sha256']}",
            "Source identity mismatch",
        )
        with path.with_suffix(".csv").open(newline="") as stream:
            rows = [int(row["id"]) for row in csv.DictReader(stream)]
        bench.require(len(rows) == entry["row_count"], "Source row count mismatch")
        addresses.update(
            ((entry["id"], offset), row_id) for offset, row_id in enumerate(rows)
        )
    bench.require(
        len(addresses) == params["rows"]
        and len(set(mapping)) == len(mapping)
        and set(mapping) == set(addresses),
        "Native mapping must cover all source rows exactly",
    )
    expected = {}
    for engine in ("ann", "exact"):
        expected[engine] = []
        for query_id, query in enumerate(queries):
            hits = json.loads(
                (root / engine / f"round-0-query-{query_id}.result.json").read_text()
            )
            bench.validate_hits(hits, query, params["k"], params["rows"])
            expected[engine].append(hits)
    fixture = {
        "source": source,
        "root": root,
        "reference": reference,
        "manifest": manifest,
        "generation": generation,
        "queries": queries,
        "mapping": mapping,
        "addresses": addresses,
        "expected": expected,
        "reference_sha256": bench.fingerprint(root / "index.json"),
    }
    verify_fixture(fixture)
    return fixture


def parse_native(path, config):
    samples, lifecycle = {}, {}
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        bench.require(
            reader.fieldnames
            == [
                "event",
                "round",
                "query",
                "rank",
                "native_id",
                "distance",
                "elapsed_ms",
            ],
            "Invalid native CSV header",
        )
        for record in reader:
            elapsed = float(record["elapsed_ms"])
            bench.require(
                math.isfinite(elapsed) and elapsed >= 0, "Invalid native timing"
            )
            if record["event"] in ("open", "close"):
                bench.require(
                    record["event"] not in lifecycle, "Duplicate native lifecycle event"
                )
                lifecycle[record["event"]] = elapsed
                continue
            bench.require(record["event"] == "sample", "Unknown native event")
            round_id, query_id, rank = (
                int(record[field]) for field in ("round", "query", "rank")
            )
            bench.require(
                0 <= round_id < config["warmup_rounds"] + config["rounds"]
                and 0 <= query_id < len(config["queries"])
                and 0 <= rank < config["k"],
                "Invalid native sample identity",
            )
            key = round_id, query_id
            sample = samples.setdefault(
                key,
                {
                    "round": round_id,
                    "query": query_id,
                    "warmup": round_id < config["warmup_rounds"],
                    "latency_ms": elapsed,
                    "hits": {},
                },
            )
            bench.require(
                rank not in sample["hits"] and sample["latency_ms"] == elapsed,
                "Duplicate rank or inconsistent native timing",
            )
            sample["hits"][rank] = {
                "native_id": int(record["native_id"]),
                "distance": float(record["distance"]),
            }
    bench.require(
        set(lifecycle) == {"open", "close"}, "Missing native lifecycle events"
    )
    expected_count = (config["warmup_rounds"] + config["rounds"]) * len(
        config["queries"]
    )
    bench.require(len(samples) == expected_count, "Missing native samples")
    result = []
    for sample in samples.values():
        bench.require(
            set(sample["hits"]) == set(range(config["k"])), "Missing native ranks"
        )
        sample["hits"] = [sample["hits"][rank] for rank in range(config["k"])]
        result.append(sample)
    return {
        "open_ms": lifecycle["open"],
        "close_ms": lifecycle["close"],
        "samples": result,
    }


def compare_hits(hits, expected):
    bench.require(len(hits) == len(expected), "Search did not return k hits")
    bench.require(len({hit["id"] for hit in hits}) == len(hits), "Duplicate hit IDs")
    for actual, original in zip(hits, expected):
        bench.require(
            type(actual["id"]) is int and actual["id"] == original["id"],
            "Ranked ID mismatch",
        )
        bench.require(
            type(actual["distance"]) in (int, float)
            and math.isfinite(actual["distance"])
            and actual["distance"] >= 0
            and bench.f32(actual["distance"]) == bench.f32(original["distance"]),
            "Ranked distance mismatch",
        )


def validate_samples(report, fixture, config, native):
    seen = set()
    for sample in report["samples"]:
        round_id, query_id = sample["round"], sample["query"]
        bench.require(
            type(round_id) is int
            and type(query_id) is int
            and 0 <= round_id < config["warmup_rounds"] + config["rounds"]
            and 0 <= query_id < len(config["queries"]),
            "Invalid sample identity",
        )
        bench.require((round_id, query_id) not in seen, "Duplicate samples")
        seen.add((round_id, query_id))
        bench.require(
            type(sample["warmup"]) is bool
            and sample["warmup"] == (round_id < config["warmup_rounds"]),
            "Invalid warmup marker",
        )
        bench.require(
            type(sample["latency_ms"]) in (int, float)
            and math.isfinite(sample["latency_ms"])
            and sample["latency_ms"] > 0,
            "Invalid sample timing",
        )
        hits = []
        for hit in sample["hits"]:
            if native:
                native_id = hit["native_id"]
                bench.require(
                    type(native_id) is int and 0 <= native_id < len(fixture["mapping"]),
                    "Native ID out of bounds",
                )
                address = fixture["mapping"][native_id]
            else:
                address = hit["row"]["file_id"], hit["row"]["row_offset"]
            bench.require(address in fixture["addresses"], "Physical row out of bounds")
            hits.append(
                {"id": fixture["addresses"][address], "distance": hit["distance"]}
            )
        compare_hits(hits, fixture["expected"]["ann"][query_id])
        sample["ranked_ids"] = [hit["id"] for hit in hits]
    bench.require(
        len(seen)
        == (config["warmup_rounds"] + config["rounds"]) * len(config["queries"]),
        "Missing samples",
    )


def distribution(samples):
    measured = [sample for sample in samples if not sample["warmup"]]
    values = [sample["latency_ms"] for sample in measured]
    return {
        **bench.summarize(values),
        "serial_qps_from_query_latency": len(values) * 1000 / sum(values),
        "first_query_in_fresh_process_ms": next(
            sample["latency_ms"]
            for sample in samples
            if sample["round"] == 0 and sample["query"] == 0
        ),
        "round_p50_ms": [
            statistics.median(
                sample["latency_ms"]
                for sample in measured
                if sample["round"] == round_id
            )
            for round_id in sorted({sample["round"] for sample in measured})
        ],
    }


def qualify_queries(runner, queries, dimension):
    setup = [
        (
            "PREPARE qualify_query_input AS SELECT "
            f"list_transform($1::FLOAT[{dimension}]::FLOAT[], "
            "lambda x: x::DOUBLE) AS vector;"
        )
    ]
    statements = [
        (
            f"query-{query_id}",
            "EXECUTE qualify_query_input(["
            + ",".join(map(str, query))
            + f"]::FLOAT[{dimension}]);",
        )
        for query_id, query in enumerate(queries)
    ]
    directory = runner.run("sql-input-qualification", statements, setup=setup)
    effective = []
    for label, _ in statements:
        result = json.loads((directory / f"{label}.result.json").read_text())
        bench.require(
            len(result) == 1 and set(result[0]) == {"vector"},
            "Invalid SQL query-input result",
        )
        vector = result[0]["vector"]
        bench.require(
            len(vector) == dimension
            and all(
                type(value) in (int, float)
                and math.isfinite(value)
                and bench.f32(value) == value
                for value in vector
            ),
            "Invalid SQL Float32 query input",
        )
        effective.append(vector)
    return effective


def qualify_sql_artifacts(cli, fixture):
    source = fixture["source"]["artifacts"]
    duckdb_hash = bench.fingerprint(cli.duckdb)
    extension_hash = bench.fingerprint(cli.extension) if cli.extension else None
    changed = (
        duckdb_hash != source["duckdb_sha256"]
        or extension_hash != source["extension_sha256"]
    )
    bench.require(
        not changed or cli.qualify_sql_candidate,
        "SQL artifacts differ from the source qualification; explicitly use --qualify-sql-candidate to verify a candidate on the frozen fixture",
    )
    return duckdb_hash, extension_hash, changed


def run_binary(root, name, binary, arguments, time_binary, timeout):
    directory = root / name
    directory.mkdir()
    before = os.getloadavg()
    start = time.perf_counter()
    command = [
        str(time_binary),
        "-f",
        "%M",
        "-o",
        str(directory / "peak-rss-kib.txt"),
        str(binary),
        *map(str, arguments(directory)),
    ]
    with (directory / "stdout.log").open("w") as stdout, (
        directory / "stderr.log"
    ).open("w") as stderr:
        result = subprocess.run(
            command,
            check=False,
            stdout=stdout,
            stderr=stderr,
            env=dict(
                os.environ,
                OMP_NUM_THREADS="1",
                VORTEX_INDEX_TIMING="0",
                RUST_LOG="warn",
            ),
            timeout=timeout,
        )
    bench.require(
        result.returncode == 0, f"{name} failed; see {directory / 'stderr.log'}"
    )
    metadata = {
        "wall_seconds_including_startup": time.perf_counter() - start,
        "peak_rss_mib": int((directory / "peak-rss-kib.txt").read_text()) / 1024,
        "load_average_before": before,
        "load_average_after": os.getloadavg(),
    }
    return directory, metadata


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql-report", type=Path, required=True)
    parser.add_argument("--duckdb", type=Path, required=True)
    parser.add_argument("--extension", type=Path)
    parser.add_argument(
        "--validation-mode", choices=["strict", "snapshot"], default="strict"
    )
    parser.add_argument("--qualify-sql-candidate", action="store_true")
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--provider", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--time-binary", type=Path, default=Path("/usr/bin/time"))
    parser.add_argument("--timeout", type=float, default=1800)
    cli = parser.parse_args(argv)
    bench.require(math.isfinite(cli.timeout) and cli.timeout > 0, "Invalid timeout")
    for path in (cli.duckdb, cli.native, cli.provider, cli.time_binary):
        bench.require(path.is_file(), f"Missing executable: {path}")
    fixture = load_fixture(cli.sql_report)
    params = fixture["source"]["parameters"]
    duckdb_hash, extension_hash, candidate_changed = qualify_sql_artifacts(cli, fixture)
    root = cli.output_dir.resolve()
    root.mkdir(mode=0o700)
    args = SimpleNamespace(
        **{
            **params,
            "duckdb": cli.duckdb,
            "extension": cli.extension,
            "time_binary": cli.time_binary,
            "timeout": cli.timeout,
            "validation_mode": cli.validation_mode,
        }
    )
    runner = bench.Runner(args, root)
    effective_queries = qualify_queries(runner, fixture["queries"], params["dimension"])
    bench.require(
        runner.workers["sql-input-qualification"]["engine"]
        == fixture["source"]["workers"]["ann"]["engine"],
        "SQL candidate SDK identity differs from the source qualification",
    )
    config = {
        "format_version": 1,
        "reference": str(fixture["root"] / "index.json"),
        "queries": effective_queries,
        "k": params["k"],
        "max_check": 4096,
        "internal_results": max(64, params["k"]),
        "search_pages": params["posting_page_limit"],
        "warmup_rounds": params["warmup_rounds"],
        "rounds": params["rounds"],
    }
    config_path = root / "provider-config.json"
    bench.write_json(config_path, config)
    query_path = root / "queries.f32bin"
    with query_path.open("wb") as stream:
        stream.write(struct.pack("<II", len(config["queries"]), params["dimension"]))
        for query in config["queries"]:
            stream.write(struct.pack(f"<{len(query)}f", *query))
    modes, workers = {}, {}
    for mode in NATIVE_MODES:
        print(
            f"Running native {mode}: {params['queries']} queries, {params['warmup_rounds']} warmup + {params['rounds']} measured rounds, OMP_NUM_THREADS=1",
            flush=True,
        )

        def arguments(directory, mode=mode):
            return [
                mode,
                fixture["generation"] / "artifacts/spfresh/native",
                query_path,
                params["rows"],
                config["k"],
                config["max_check"],
                config["internal_results"],
                config["search_pages"],
                config["warmup_rounds"],
                config["rounds"],
                directory / "samples.csv",
            ]

        directory, workers[mode] = run_binary(
            root, mode, cli.native.resolve(), arguments, cli.time_binary, cli.timeout
        )
        native_report = parse_native(directory / "samples.csv", config)
        validate_samples(native_report, fixture, config, native=True)
        bench.write_json(directory / "samples.json", native_report)
        modes[mode] = {
            **distribution(native_report["samples"]),
            "open_ms": native_report["open_ms"],
            "close_ms": native_report["close_ms"],
        }
        print(
            f"Finished {mode}: {json.dumps(modes[mode])}; report {directory / 'samples.json'}",
            flush=True,
        )
    print("Running Rust Provider without SQL", flush=True)
    directory, workers["provider"] = run_binary(
        root,
        "provider",
        cli.provider.resolve(),
        lambda directory: [config_path, directory / "samples.json"],
        cli.time_binary,
        cli.timeout,
    )
    provider_report = json.loads((directory / "samples.json").read_text())
    bench.require(
        provider_report["format_version"] == 1
        and provider_report["revision"] == REVISION,
        "Provider format/revision mismatch",
    )
    validate_samples(provider_report, fixture, config, native=False)
    modes["provider"] = {
        **distribution(provider_report["samples"]),
        "open_ms": provider_report["open_ms"],
        "close_ms": provider_report["close_ms"],
    }
    print(
        f"Finished provider: {json.dumps(modes['provider'])}; report {directory / 'samples.json'}",
        flush=True,
    )
    inventory = (
        "["
        + ",".join(
            bench.quote(entry["uri"])
            for entry in fixture["reference"]["snapshot"]["files"]
        )
        + "]"
    )
    reference = fixture["root"] / "index.json"
    for engine in ("exact", "ann"):
        setup, statements, labels = bench.search_workload(
            engine,
            args,
            fixture["queries"],
            inventory,
            reference,
            config["warmup_rounds"] + config["rounds"],
        )
        directory = runner.run(f"sql-{engine}", statements, setup=setup)
        samples = []
        for index, (label, round_id, query_id) in enumerate(labels):
            profile = json.loads((directory / f"{label}.profile.json").read_text())
            bench.require(
                profile["query_name"].strip().rstrip(";")
                == statements[index][1].strip().rstrip(";"),
                "SQL profile/query mismatch",
            )
            hits = json.loads((directory / f"{label}.result.json").read_text())
            bench.validate_hits(
                hits, config["queries"][query_id], config["k"], params["rows"]
            )
            bench.require(
                hits == fixture["expected"][engine][query_id],
                "SQL ranked original rows changed",
            )
            samples.append(
                {
                    "round": round_id,
                    "query": query_id,
                    "warmup": round_id < config["warmup_rounds"],
                    "latency_ms": profile["latency"] * 1000,
                }
            )
        modes[f"sql-{engine}"] = distribution(samples)
        bench.write_json(directory / "samples.json", samples)
        print(
            f"Finished sql-{engine}: {json.dumps(modes[f'sql-{engine}'])}", flush=True
        )
    setup, statements, labels = bench.search_workload(
        "ann", args, fixture["queries"], inventory, reference, 2
    )
    directory = runner.run("sql-diagnostic", statements, timings=True, setup=setup)
    events = bench.parse_phases((directory / "stderr.log").read_text(), len(labels))
    for label, _, query_id in labels:
        hits = json.loads((directory / f"{label}.result.json").read_text())
        bench.require(
            hits == fixture["expected"]["ann"][query_id],
            "Diagnostics changed ranked rows",
        )
    bench.require(
        events[0].get("provider_cache_hit") is False
        and all(event.get("provider_cache_hit") is True for event in events[1:]),
        "Expected an initial cache miss followed by hits",
    )
    snapshot_status = bench.snapshot_cache_status(events, cli.validation_mode, True)
    bench.write_json(directory / "stages.json", events)
    verify_fixture(fixture)
    workers.update(runner.workers)
    per_query_recall = [
        len({hit["id"] for hit in ann} & {hit["id"] for hit in exact}) / config["k"]
        for ann, exact in zip(fixture["expected"]["ann"], fixture["expected"]["exact"])
    ]
    report = {
        "format_version": 1,
        "status": "ok",
        "source_report": str(cli.sql_report.resolve()),
        "reference_sha256": fixture["reference_sha256"],
        "parameters": {**config, "validation_mode": cli.validation_mode},
        "inputs": fixture["source"]["inputs"],
        "native_revision": REVISION,
        "artifacts": {
            "script_sha256": bench.fingerprint(Path(__file__)),
            "native_sha256": bench.fingerprint(cli.native),
            "provider_sha256": bench.fingerprint(cli.provider),
            "duckdb_sha256": duckdb_hash,
            "extension_sha256": extension_hash,
            "effective_queries_sha256": bench.fingerprint(query_path),
        },
        "sql_candidate_qualification": {
            "artifact_changed": candidate_changed,
            "source_artifacts": fixture["source"]["artifacts"],
            "matching_sdk_identity": True,
            "original_ranked_rows_equal": True,
        },
        "query_input_qualification": {
            "changed_float32_components": sum(
                original != actual
                for source_query, effective_query in zip(
                    fixture["queries"], effective_queries
                )
                for original, actual in zip(source_query, effective_query)
            ),
            "samples": str(root / "sql-input-qualification"),
        },
        "method": {
            "queries": "Native/Provider receive the exact Float32 vectors qualified through SQL's original prepared argument casts; SQL literals remain unchanged",
            "query_granularity": "One query per call; no batch average reported as single-query latency",
            "native_clock": "steady_clock; QueryResult construction/search/copy/destruction; no CSV output",
            "native_config": "Direct C++ config set once; bridge config set per call, as in production",
            "provider_clock": "Instant around public VectorIndex::search; validation, FFI and hit mapping included",
            "sql_clock": "DuckDB materialized SELECT JSON profile, as in the source benchmark",
            "validation": "Frozen fixture content verified before/after; native/Provider open once; SQL uses the explicitly recorded validation mode",
            "workspace": "cpp-reset clears at call boundaries; cpp-reuse is a direct single-handle/thread lower bound without bridge lifecycle/option guards; bridge/provider/SQL behavior depends on the recorded binaries",
            "qps": "Serial inverse mean query latency, not concurrent throughput",
            "cache": "OS page cache uncontrolled; fresh processes, not cold-disk measurements",
            "order": "cpp-reset, cpp-reuse, bridge, provider, sql-exact, sql-ann, separate diagnostics; sequential",
            "open": "Native loader / Provider store verification and materialization measured separately; SQL PREPARE excluded from query profiles",
        },
        "modes": modes,
        "workers": workers,
        "recall_at_k": statistics.mean(per_query_recall),
        "per_query_recall": per_query_recall,
        "repeat_results_equal": True,
        "ranked_ids_and_float32_distances_equal_across_layers": True,
        "sql_ranked_original_rows_equal": True,
        "fixture_content_unchanged": True,
        "stages_after_first_query": {
            phase: bench.summarize([event["phases"][phase] for event in events[1:]])
            for phase in bench.PHASES
        },
        "diagnostic_provider_cache": {
            "samples": len(events),
            "hits": len(events) - 1,
            "first_query_hit": False,
        },
        "diagnostic_snapshot_cache": snapshot_status,
    }
    bench.write_json(root / "summary.json", report)
    print(
        json.dumps(
            {
                "report": str(root / "summary.json"),
                "modes": modes,
                "recall_at_k": report["recall_at_k"],
            },
            indent=2,
        ),
        flush=True,
    )
    return report


if __name__ == "__main__":
    main()

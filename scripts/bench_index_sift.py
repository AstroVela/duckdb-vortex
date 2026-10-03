#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare SPFresh layers on the exact SIFT inputs qualified by OpenData."""

import argparse
import itertools
import json
import math
import os
import shutil
import struct
from pathlib import Path
from types import SimpleNamespace

import bench_index_layers as layers
import bench_index_sql as bench
import numpy as np

CHUNK_QUERIES = 256
DEFAULT_SQL_ARTIFACT_BUDGET = 256 * 1024 * 1024
SNAPSHOT_BUDGET_ERROR = (
    "Prepared snapshot exceeds the connection's retained handle or byte budget"
)


def read_records(path, dimension, kind):
    dtype = np.dtype([("dimension", "<u4"), ("values", kind, (dimension,))])
    bench.require(path.stat().st_size % dtype.itemsize == 0, "Truncated vector records")
    records = np.memmap(path, mode="r", dtype=dtype)
    bench.require(
        len(records) > 0 and np.all(records["dimension"] == dimension),
        "Record dimension mismatch",
    )
    return records["values"]


def load_inputs(manifest_path, name, query_count):
    entries = json.loads(manifest_path.read_text())
    matches = [entry for entry in entries if entry["name"] == name]
    bench.require(len(matches) == 1, "Dataset missing or duplicated in manifest")
    entry = matches[0]
    expected_rows = {"sift100k": 100_000, "sift1m": 1_000_000}[name]
    bench.require(
        entry["rows"] == expected_rows and entry["dimension"] == 128,
        "Unexpected SIFT shape",
    )
    for identity in entry["files"].values():
        path = Path(identity["path"])
        bench.require(
            path.is_file() and path.stat().st_size == identity["bytes"],
            "Input file size changed",
        )
        bench.require(
            bench.fingerprint(path) == identity["sha256"], "Input file checksum changed"
        )
    base = read_records(Path(entry["files"]["base"]["path"]), 128, "<f4")
    queries = read_records(Path(entry["files"]["queries"]["path"]), 128, "<f4")
    truth = read_records(Path(entry["files"]["groundtruth"]["path"]), 100, "<i4")
    bench.require(
        len(base) == expected_rows
        and len(queries) >= query_count
        and len(truth) >= query_count,
        "Dataset row count mismatch",
    )
    bench.require(
        np.isfinite(base).all() and np.isfinite(queries[:query_count]).all(),
        "Non-finite input",
    )
    bench.require(
        np.all((truth[:query_count] >= 0) & (truth[:query_count] < expected_rows)),
        "Ground-truth ID out of bounds",
    )
    return (
        entry,
        base,
        queries[:query_count].tolist(),
        truth[:query_count, :10].tolist(),
    )


def load_fixture(path, entry):
    reference_path = path / "index.json"
    reference = json.loads(reference_path.read_text())
    bench.require(reference["format_version"] == 1, "Unsupported reference")
    generation_name = reference["generation"]["generation"]
    bench.require(
        Path(generation_name).name == generation_name
        and generation_name not in ("", ".", ".."),
        "Invalid generation path",
    )
    generation = path / generation_name
    layers.checked_artifact(
        generation / "manifest.json", reference["generation"]["manifest"]
    )
    metadata = json.loads((generation / "manifest.json").read_text())
    bench.require(
        metadata["snapshot"] == reference["snapshot"]
        and metadata["backend"] == "spfresh.static",
        "Generation identity mismatch",
    )
    for artifact in metadata["artifacts"]:
        relative = Path(artifact["path"])
        bench.require(
            not relative.is_absolute() and ".." not in relative.parts,
            "Invalid artifact path",
        )
        layers.checked_artifact(generation / "artifacts" / relative, artifact)
    bundle = json.loads((generation / "artifacts/spfresh/bundle.json").read_text())
    bench.require(
        bundle["revision"] == layers.REVISION
        and bundle["metric"] == "squared_l2"
        and bundle["value_type"] == "float32"
        and bundle["snapshot"] == reference["snapshot"],
        "Unsupported native bundle",
    )
    bench.require(
        bundle["bundle"]["rows"] == entry["rows"]
        and bundle["bundle"]["dimension"] == 128,
        "Native dataset shape mismatch",
    )
    build = json.loads((path / "build.json").read_text())
    bench.require(
        build["rows"] == entry["rows"] and build["dimension"] == 128,
        "Build shape mismatch",
    )
    prefixes = {}
    start = 0
    files = reference["snapshot"]["files"]
    bench.require(len(files) == 2, "Expected the two ordered fixture shards")
    for index, file in enumerate(files):
        source_path = Path(file["uri"])
        bench.require(
            source_path == path / f"part-{index}.vortex" and file["id"] == index + 1,
            "Unexpected source file identity",
        )
        bench.require(
            file["version"] == f"sha256:{bench.fingerprint(source_path)}",
            "Source content changed",
        )
        prefixes[file["id"]] = (start, file["row_count"])
        start += file["row_count"]
    bench.require(start == entry["rows"], "Fixture does not cover the entire dataset")
    mapping_path = generation / "artifacts/spfresh/rows.bin"
    with mapping_path.open("rb") as stream:
        header = stream.read(16)
    bench.require(
        header[:8] == b"VXSFROW1"
        and struct.unpack("<Q", header[8:])[0] == start
        and mapping_path.stat().st_size == 16 + 16 * start,
        "Invalid native row map",
    )
    mapping = np.memmap(
        mapping_path,
        mode="r",
        offset=16,
        dtype=np.dtype([("file_id", "<u8"), ("row_offset", "<u8")]),
    )
    mapped_ids = np.empty(start, dtype=np.int64)
    known = np.zeros(start, dtype=np.bool_)
    for file_id, (prefix, count) in prefixes.items():
        mask = mapping["file_id"] == file_id
        bench.require(
            np.all(mapping["row_offset"][mask] < count), "Mapped row out of bounds"
        )
        mapped_ids[mask] = prefix + mapping["row_offset"][mask].astype(np.int64)
        known |= mask
    bench.require(
        known.all() and np.array_equal(np.sort(mapped_ids), np.arange(start)),
        "Native row map must cover all original IDs once",
    )
    return {
        "root": path,
        "reference": reference,
        "generation": generation,
        "prefixes": prefixes,
        "mapped_ids": mapped_ids,
        "build": build,
        "pages": bundle["bundle"]["posting_page_limit"],
        "artifact_bytes": sum(artifact["size"] for artifact in metadata["artifacts"]),
        "source_bytes": sum(Path(file["uri"]).stat().st_size for file in files),
        "reference_sha256": bench.fingerprint(reference_path),
    }


def statistics(samples):
    result = layers.distribution(samples)
    values = [sample["latency_ms"] for sample in samples if not sample["warmup"]]
    result["p90_ms"] = bench.percentile(values, 0.90)
    result["p99_ms"] = bench.percentile(values, 0.99)
    return result


def translate_samples(samples, fixture, base, queries, k, native):
    for sample in samples:
        hits = []
        for hit in sample["hits"]:
            if native:
                native_id = hit["native_id"]
                bench.require(
                    type(native_id) is int and 0 <= native_id < len(base),
                    "Native ID out of bounds",
                )
                row_id = int(fixture["mapped_ids"][native_id])
            else:
                address = hit["row"]
                prefix, count = fixture["prefixes"][address["file_id"]]
                bench.require(
                    0 <= address["row_offset"] < count, "Provider row out of bounds"
                )
                row_id = prefix + address["row_offset"]
            hits.append({"id": row_id, "distance": hit["distance"]})
        bench.require(
            len(hits) == k and len({hit["id"] for hit in hits}) == k,
            "Invalid ranked hit count",
        )
        bench.require(
            all(a["distance"] <= b["distance"] for a, b in itertools.pairwise(hits)),
            "Unordered distances",
        )
        query = np.asarray(queries[sample["query"]], dtype=np.float64)
        values = base[[hit["id"] for hit in hits]].astype(np.float64)
        distances = np.sum((values - query) ** 2, axis=1)
        bench.require(
            all(
                math.isfinite(hit["distance"])
                and math.isclose(
                    hit["distance"], float(distance), rel_tol=3e-5, abs_tol=1e-4
                )
                for hit, distance in zip(hits, distances)
            ),
            "Distance differs from the original fvecs row",
        )
        sample["ranked_hits"] = hits


def expected_results(samples, query_count):
    expected = {}
    for sample in samples:
        query = sample["query"]
        if query in expected:
            layers.compare_hits(sample["ranked_hits"], expected[query])
        else:
            expected[query] = sample["ranked_hits"]
    bench.require(set(expected) == set(range(query_count)), "Missing query results")
    return [expected[query] for query in range(query_count)]


def recall(expected, truth):
    bench.require(
        len(expected) == len(truth) and len(expected) > 0, "Recall query count mismatch"
    )
    values = [
        len({hit["id"] for hit in hits} & set(neighbors)) / len(neighbors)
        for hits, neighbors in zip(expected, truth)
    ]
    return sum(values) / len(values), values


def validate_sql_hits(hits, query, base, k):
    for hit in hits:
        value = hit["id"]
        if isinstance(value, str):
            bench.require(
                value.isascii() and value.isdecimal() and str(int(value)) == value,
                "Invalid JSON row ID",
            )
            hit["id"] = int(value)
        bench.require(type(hit["id"]) is int, "Invalid JSON row ID")
    bench.validate_hits(hits, query, k, len(base))
    bench.require(
        np.array_equal(
            np.asarray([hit["embedding"] for hit in hits], dtype=np.float64),
            base[[hit["id"] for hit in hits]].astype(np.float64),
        ),
        "SQL did not return the original SIFT vectors",
    )


def run_chunks(root, name, cli, fixture, queries, probes, warmup, rounds, native=True):
    directory = root / name
    directory.mkdir(mode=0o700)
    samples, workers, lifecycle = [], [], []
    for start in range(0, len(queries), CHUNK_QUERIES):
        chunk = queries[start : start + CHUNK_QUERIES]
        config = {
            "format_version": 1,
            "reference": str(fixture["root"] / "index.json"),
            "queries": chunk,
            "k": cli.k,
            "max_check": cli.max_check,
            "internal_results": probes,
            "search_pages": fixture["pages"],
            "warmup_rounds": warmup,
            "rounds": rounds,
        }
        config_path = directory / f"chunk-{start}.json"
        bench.write_json(config_path, config)
        if native:
            query_path = directory / f"chunk-{start}.f32bin"
            with query_path.open("xb") as stream:
                stream.write(struct.pack("<II", len(chunk), 128))
                for query in chunk:
                    stream.write(struct.pack("<128f", *query))

            def arguments(output, query_path=query_path):
                return [
                    "cpp-reuse" if name.startswith("tune-") else name,
                    fixture["generation"] / "artifacts/spfresh/native",
                    query_path,
                    cli.rows,
                    cli.k,
                    cli.max_check,
                    probes,
                    fixture["pages"],
                    warmup,
                    rounds,
                    output / "samples.csv",
                ]

            output, worker = layers.run_binary(
                directory,
                f"chunk-{start}",
                cli.native,
                arguments,
                cli.time_binary,
                cli.timeout,
            )
            report = layers.parse_native(output / "samples.csv", config)
        else:
            output, worker = layers.run_binary(
                directory,
                f"chunk-{start}",
                cli.provider,
                lambda output, config_path=config_path: [
                    config_path,
                    output / "samples.json",
                ],
                cli.time_binary,
                cli.timeout,
            )
            report = json.loads((output / "samples.json").read_text())
            bench.require(
                report["format_version"] == 1 and report["revision"] == layers.REVISION,
                "Provider revision mismatch",
            )
        seen = set()
        for sample in report["samples"]:
            key = sample["round"], sample["query"]
            bench.require(
                key not in seen
                and type(key[0]) is int
                and type(key[1]) is int
                and 0 <= key[0] < warmup + rounds
                and 0 <= key[1] < len(chunk)
                and type(sample["warmup"]) is bool
                and sample["warmup"] == (key[0] < warmup)
                and type(sample["latency_ms"]) in (int, float)
                and math.isfinite(sample["latency_ms"])
                and sample["latency_ms"] > 0,
                "Invalid or repeated sample",
            )
            seen.add(key)
            sample["query"] += start
        bench.require(
            len(seen) == len(chunk) * (warmup + rounds), "Missing chunk samples"
        )
        samples += report["samples"]
        workers.append(worker)
        lifecycle.append(
            {
                "query_start": start,
                "queries": len(chunk),
                "open_ms": report["open_ms"],
                "close_ms": report["close_ms"],
            }
        )
    return samples, workers, lifecycle


def sql_arguments(cli, fixture, mode, probes):
    candidate = mode == "snapshot" and cli.snapshot_duckdb is not None
    args = SimpleNamespace(
        **{
            **vars(cli),
            "threads": 1,
            "dimension": 128,
            "execution_mode": "prepared",
            "validation_mode": mode,
            "duckdb": cli.snapshot_duckdb if candidate else cli.duckdb,
            "extension": None if candidate else cli.extension,
            "backend_options": {
                "max_check": cli.max_check,
                "internal_results": probes,
                "search_pages": fixture["pages"],
            },
        }
    )
    budget = (
        cli.snapshot_cache_budget_mib * 1024 * 1024
        if candidate
        else DEFAULT_SQL_ARTIFACT_BUDGET
    )
    return args, budget


def run_sql(
    root, cli, fixture, queries, mode, probes, expected, base, diagnostic=False
):
    args, budget = sql_arguments(cli, fixture, mode, probes)
    runner = bench.Runner(args, root)
    inventory = (
        "["
        + ",".join(
            bench.quote(file["uri"])
            for file in fixture["reference"]["snapshot"]["files"]
        )
        + "]"
    )
    setup, statements, labels = bench.search_workload(
        "ann",
        args,
        queries,
        inventory,
        fixture["root"] / "index.json",
        2 if diagnostic else cli.warmup_rounds + cli.rounds,
    )
    name = f"sql-{mode}" + ("-diagnostic" if diagnostic else "")
    directory = runner.run(name, statements, timings=diagnostic, setup=setup)
    samples = []
    for index, (label, round_id, query_id) in enumerate(labels):
        profile = json.loads((directory / f"{label}.profile.json").read_text())
        bench.require(
            profile["query_name"].strip().rstrip(";")
            == statements[index][1].strip().rstrip(";"),
            "Profile query mismatch",
        )
        hits = json.loads((directory / f"{label}.result.json").read_text())
        validate_sql_hits(hits, queries[query_id], base, cli.k)
        layers.compare_hits(hits, expected[query_id])
        latency = profile["latency"] * 1000
        bench.require(math.isfinite(latency) and latency > 0, "Invalid SQL latency")
        samples.append(
            {
                "round": round_id,
                "query": query_id,
                "warmup": round_id < cli.warmup_rounds,
                "latency_ms": latency,
            }
        )
    bench.write_json(directory / "samples.json", samples)
    if diagnostic:
        events = bench.parse_phases((directory / "stderr.log").read_text(), len(labels))
        retained = fixture["artifact_bytes"] <= budget
        bench.require(
            [event.get("provider_cache_hit") for event in events]
            == [False] + [retained] * (len(events) - 1),
            "Provider-cache behavior differs from the stated artifact budget",
        )
        snapshot = bench.snapshot_cache_status(events, mode, True)
        bench.write_json(directory / "stages.json", events)
        return {
            "provider_cache": {
                "samples": len(events),
                "hits": sum(event["provider_cache_hit"] for event in events),
                "retained_artifact_budget_bytes": budget,
            },
            "snapshot_cache": snapshot,
            "phases_after_first_query": {
                phase: bench.summarize([event["phases"][phase] for event in events[1:]])
                for phase in bench.PHASES
            },
        }, runner.workers
    return statistics(samples), runner.workers


def snapshot_budget_control(root, cli, fixture, queries, probes, expected, base):
    bench.require(
        fixture["artifact_bytes"] > DEFAULT_SQL_ARTIFACT_BUDGET,
        "Default snapshot budget control requires an oversized index",
    )
    control_root = root / "default-snapshot-budget-control"
    control_root.mkdir(mode=0o700)
    default_cli = SimpleNamespace(**{**vars(cli), "snapshot_duckdb": None})
    try:
        run_sql(
            control_root,
            default_cli,
            fixture,
            queries[:1],
            "snapshot",
            probes,
            expected[:1],
            base,
        )
    except RuntimeError:
        stderr = (control_root / "sql-snapshot/stderr.log").read_text()
        bench.require(
            SNAPSHOT_BUDGET_ERROR in stderr, "Unexpected default snapshot failure"
        )
        rss = (control_root / "sql-snapshot/peak-rss-kib.txt").read_text().splitlines()
        bench.require(rss and rss[-1].isdecimal(), "Invalid failed-worker RSS")
        result = {
            "status": "rejected_as_expected",
            "error": SNAPSHOT_BUDGET_ERROR,
            "artifact_bytes": fixture["artifact_bytes"],
            "retained_artifact_budget_bytes": DEFAULT_SQL_ARTIFACT_BUDGET,
            "peak_rss_mib": int(rss[-1]) / 1024,
        }
        bench.write_json(control_root / "result.json", result)
        print(f"Default snapshot control: {result}", flush=True)
        return result
    raise RuntimeError("Default snapshot unexpectedly retained an oversized index")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("sift100k", "sift1m"), required=True)
    parser.add_argument("--input-manifest", type=Path, required=True)
    parser.add_argument("--duckdb", type=Path, required=True)
    parser.add_argument("--extension", type=Path)
    parser.add_argument("--snapshot-duckdb", type=Path)
    parser.add_argument("--snapshot-cache-budget-mib", type=int)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--provider", type=Path, required=True)
    parser.add_argument("--fixture-builder", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--strict-queries", type=int)
    parser.add_argument("--warmup-rounds", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--max-check", type=int, default=32768)
    parser.add_argument("--probes", type=int, nargs="+", default=[64, 128, 256, 512])
    parser.add_argument("--recall-target", type=float, default=0.99)
    parser.add_argument("--head-count", type=int)
    parser.add_argument("--posting-page-limit", type=int, default=128)
    parser.add_argument("--replicas", type=int)
    parser.add_argument("--timeout", type=float, default=7200)
    parser.add_argument("--time-binary", type=Path, default=Path("/usr/bin/time"))
    cli = parser.parse_args(argv)
    bench.require(
        (cli.snapshot_duckdb is None) == (cli.snapshot_cache_budget_mib is None)
        and (
            cli.snapshot_cache_budget_mib is None or cli.snapshot_cache_budget_mib > 0
        ),
        "A benchmark-only snapshot shell needs its explicit compiled cache budget",
    )
    bench.require(
        1 <= cli.queries <= 1000
        and cli.k == 10
        and 1 <= cli.rounds <= 100
        and 0 <= cli.warmup_rounds <= 100,
        "Invalid query count/k/rounds",
    )
    bench.require(
        cli.strict_queries is None or 1 <= cli.strict_queries <= cli.queries,
        "Invalid strict control count",
    )
    bench.require(
        math.isfinite(cli.recall_target)
        and 0 < cli.recall_target <= 1
        and math.isfinite(cli.timeout)
        and cli.timeout > 0,
        "Invalid recall target or timeout",
    )
    bench.require(
        1 <= cli.posting_page_limit <= 4096
        and all(
            10 <= probes <= 4096
            and probes <= cli.max_check <= 1048576
            and cli.posting_page_limit * probes * 4096
            <= bench.POSTING_BUFFER_LIMIT_BYTES
            for probes in cli.probes
        ),
        "Invalid query budget",
    )
    for binary in (
        cli.duckdb,
        cli.native,
        cli.provider,
        cli.fixture_builder,
        cli.time_binary,
    ):
        bench.require(binary.is_file(), f"Missing executable: {binary}")
    cli.duckdb, cli.native, cli.provider, cli.fixture_builder = [
        path.resolve()
        for path in (cli.duckdb, cli.native, cli.provider, cli.fixture_builder)
    ]
    if cli.snapshot_duckdb:
        bench.require(cli.snapshot_duckdb.is_file(), "Missing snapshot candidate")
        cli.snapshot_duckdb = cli.snapshot_duckdb.resolve()
        bench.require(
            bench.fingerprint(cli.snapshot_duckdb) != bench.fingerprint(cli.duckdb),
            "Snapshot candidate must be a separately compiled artifact",
        )
    root = cli.output_dir.resolve()
    root.mkdir(mode=0o700)
    shutil.copyfile(Path(__file__), root / "benchmark-script.py")
    entry, base, queries, truth = load_inputs(
        cli.input_manifest, cli.dataset, cli.queries
    )
    cli.rows = entry["rows"]
    bench.write_json(root / "inputs.json", entry)
    fixture_path = cli.fixture.resolve() if cli.fixture else root / "fixture"
    if not cli.fixture:
        config = {
            "base": entry["files"]["base"]["path"],
            "output_dir": str(fixture_path),
            "rows": cli.rows,
            "options": {
                "format_version": 1,
                "dimension": 128,
                "head_count": cli.head_count
                or (4096 if cli.dataset == "sift100k" else 16384),
                "posting_page_limit": cli.posting_page_limit,
                "replicas": cli.replicas or (4 if cli.dataset == "sift100k" else 1),
            },
        }
        bench.write_json(root / "fixture-config.json", config)
        print(f"Building {cli.dataset}: {config}", flush=True)
        layers.run_binary(
            root,
            "fixture-build",
            cli.fixture_builder,
            lambda _: [root / "fixture-config.json"],
            cli.time_binary,
            cli.timeout,
        )
        bench.write_json(fixture_path / "inputs.json", entry)
    bench.require(
        json.loads((fixture_path / "inputs.json").read_text()) == entry,
        "Fixture was built from other inputs",
    )
    fixture = load_fixture(fixture_path, entry)
    bench.require(
        fixture["pages"] == cli.posting_page_limit, "Fixture page limit mismatch"
    )
    qualification = bench.Runner(
        SimpleNamespace(
            duckdb=cli.duckdb,
            extension=cli.extension,
            threads=1,
            timeout=cli.timeout,
            time_binary=cli.time_binary,
        ),
        root,
    )
    effective = layers.qualify_queries(qualification, queries, 128)
    bench.require(effective == queries, "SQL casts changed the SIFT query vectors")
    if cli.snapshot_duckdb:
        candidate_root = root / "snapshot-candidate-qualification"
        candidate_root.mkdir(mode=0o700)
        args, _ = sql_arguments(cli, fixture, "snapshot", min(cli.probes))
        bench.require(
            layers.qualify_queries(bench.Runner(args, candidate_root), queries, 128)
            == queries,
            "Snapshot candidate casts changed the SIFT query vectors",
        )
    candidates, expected, selected = [], None, None
    for probes in sorted(set(cli.probes)):
        samples, _, _ = run_chunks(
            root, f"tune-{probes}", cli, fixture, queries, probes, 0, 1
        )
        translate_samples(samples, fixture, base, queries, cli.k, True)
        expected = expected_results(samples, len(queries))
        score, _ = recall(expected, truth)
        candidates.append(
            {
                "internal_results": probes,
                "max_check": cli.max_check,
                "recall_at_10": score,
            }
        )
        bench.write_json(root / "tuning.json", candidates)
        print(
            f"Recall qualification: internal_results={probes}, Recall@10={score:.4f}",
            flush=True,
        )
        if score >= cli.recall_target:
            selected = probes
            break
    bench.require(
        selected is not None,
        "No tested query budget reaches the recall target; timings are not an equal-quality comparison",
    )
    modes, workers = {}, {}
    for mode in (*layers.NATIVE_MODES, "provider"):
        print(
            f"Measuring {mode}: {cli.queries} queries, {cli.warmup_rounds} warmup + {cli.rounds} measured rounds",
            flush=True,
        )
        samples, process_resources, lifecycle = run_chunks(
            root,
            mode,
            cli,
            fixture,
            queries,
            selected,
            cli.warmup_rounds,
            cli.rounds,
            mode != "provider",
        )
        translate_samples(samples, fixture, base, queries, cli.k, mode != "provider")
        for sample in samples:
            layers.compare_hits(sample["ranked_hits"], expected[sample["query"]])
        modes[mode] = {**statistics(samples), "chunk_lifecycle": lifecycle}
        workers[mode] = process_resources
        bench.write_json(root / mode / "samples.json", samples)
        bench.write_json(root / "progress.json", {"modes": modes, "workers": workers})
        print(f"Finished {mode}: {modes[mode]}", flush=True)
    diagnostics = {}
    default_snapshot = None
    if fixture["artifact_bytes"] > DEFAULT_SQL_ARTIFACT_BUDGET:
        default_snapshot = snapshot_budget_control(
            root, cli, fixture, queries, selected, expected, base
        )
    strict_ids = np.linspace(
        0, len(queries) - 1, cli.strict_queries or len(queries), dtype=np.int64
    ).tolist()
    matched_strict_subset = {
        mode: statistics(
            [
                sample
                for sample in json.loads((root / mode / "samples.json").read_text())
                if sample["query"] in set(strict_ids)
            ]
        )
        for mode in (*layers.NATIVE_MODES, "provider")
    }
    for mode in ("strict", "snapshot"):
        if mode == "snapshot" and default_snapshot and not cli.snapshot_duckdb:
            modes["sql-snapshot"] = {
                "status": "unsupported_default_budget",
                "control": default_snapshot,
            }
            continue
        query_ids = strict_ids if mode == "strict" else list(range(len(queries)))
        mode_queries = [queries[index] for index in query_ids]
        mode_expected = [expected[index] for index in query_ids]
        modes[f"sql-{mode}"], resources = run_sql(
            root, cli, fixture, mode_queries, mode, selected, mode_expected, base
        )
        modes[f"sql-{mode}"]["query_ids"] = query_ids
        modes[f"sql-{mode}"]["recall_at_10"] = recall(
            mode_expected, [truth[index] for index in query_ids]
        )[0]
        modes[f"sql-{mode}"]["benchmark_only_budget_candidate"] = (
            mode == "snapshot" and cli.snapshot_duckdb is not None
        )
        if mode == "snapshot":
            matched_strict_subset["sql-snapshot"] = statistics(
                [
                    sample
                    for sample in json.loads(
                        (root / "sql-snapshot" / "samples.json").read_text()
                    )
                    if sample["query"] in set(strict_ids)
                ]
            )
        else:
            matched_strict_subset["sql-strict"] = modes["sql-strict"]
        workers.update(resources)
        bench.write_json(root / "progress.json", {"modes": modes, "workers": workers})
        print(f"Finished SQL {mode}: {modes[f'sql-{mode}']}", flush=True)
        diagnostics[mode], resources = run_sql(
            root,
            cli,
            fixture,
            queries[:8],
            mode,
            selected,
            expected[:8],
            base,
            diagnostic=True,
        )
        workers.update(resources)
    after = load_fixture(fixture_path, entry)
    bench.require(
        after["reference_sha256"] == fixture["reference_sha256"],
        "Reference changed during benchmark",
    )
    load_inputs(cli.input_manifest, cli.dataset, cli.queries)
    score, per_query = recall(expected, truth)
    report = {
        "format_version": 1,
        "status": (
            "ok"
            if "samples" in modes["sql-snapshot"]
            else "partial_unsupported_snapshot_budget"
        ),
        "dataset": cli.dataset,
        "inputs": entry,
        "fixture": str(fixture_path),
        "fixture_build": fixture["build"],
        "artifact_bytes": fixture["artifact_bytes"],
        "source_bytes": fixture["source_bytes"],
        "default_snapshot_budget_control": default_snapshot,
        "reference_sha256": fixture["reference_sha256"],
        "parameters": {
            "queries": cli.queries,
            "strict_control_queries": len(strict_ids),
            "k": cli.k,
            "warmup_rounds": cli.warmup_rounds,
            "rounds": cli.rounds,
            "max_check": cli.max_check,
            "internal_results": selected,
            "search_pages": fixture["pages"],
            "query_concurrency": 1,
            "default_sql_retained_artifact_budget_bytes": DEFAULT_SQL_ARTIFACT_BUDGET,
            "snapshot_candidate_retained_artifact_budget_mib": cli.snapshot_cache_budget_mib,
        },
        "tuning": candidates,
        "recall_at_10": score,
        "per_query_recall": per_query,
        "ranked_ids_and_distances_equal_across_layers": True,
        "measured_layers": [name for name, mode in modes.items() if "samples" in mode],
        "repeat_results_equal": True,
        "modes": modes,
        "matched_strict_subset": matched_strict_subset,
        "diagnostics": diagnostics,
        "workers": workers,
        "artifacts": {
            "native_revision": layers.REVISION,
            **{
                name: {"path": str(path), "sha256": bench.fingerprint(path)}
                for name, path in (
                    ("duckdb", cli.duckdb),
                    ("native", cli.native),
                    ("provider", cli.provider),
                    ("fixture_builder", cli.fixture_builder),
                    ("script", Path(__file__)),
                    ("snapshot_duckdb", cli.snapshot_duckdb),
                )
                if path is not None
            },
        },
        "method": {
            "inputs": "Original full fvecs, first query_count queries, supplied top-10 ground truth and original positional IDs; no renormalization or new ground truth",
            "native_provider": "Single query per call; <=256-query processes due to existing benchmark caps; all chunks have complete warmup rounds; opens/closes/CSV/JSON output excluded from individual query timers",
            "sql": "One prepared owner per mode; full original rows materialized; DuckDB JSON profile latency, profiler enabled; PREPARE excluded; strict remains default",
            "strict_control": "When fewer strict queries are configured, use evenly spaced IDs from the full query set and compare only their matched samples across layers; do not mix these tail percentiles with the full query distribution",
            "quality": "Select the first tested internal_results reaching the stated recall target on these benchmark queries, then freeze options across all layers",
            "qps": "Inverse mean measured search/profile latency, not whole-process or concurrent throughput",
            "cache": "OS page cache not dropped; frozen files, sequential processes; no SlateDB 1-GiB cache equivalence claimed",
            "limits": "Fixture builder input budget is explicitly 512 MiB; default SQL and Provider limits are unchanged. Any separately compiled snapshot shell and its declared retained-artifact budget are labeled as benchmark-only; oversized default snapshots are verified to reject",
            "workspace": "cpp-reuse is a direct single-handle/thread lower bound without bridge lifecycle/option guards; bridge/provider/SQL behavior depends on the recorded binaries",
            "environment": {
                key: os.environ.get(key)
                for key in (
                    "RAYON_NUM_THREADS",
                    "TOKIO_WORKER_THREADS",
                    "OMP_NUM_THREADS",
                )
            },
        },
    }
    bench.write_json(root / "summary.json", report)
    print(
        json.dumps(
            {
                "report": str(root / "summary.json"),
                "recall_at_10": score,
                "modes": modes,
            },
            indent=2,
        ),
        flush=True,
    )
    return report


if __name__ == "__main__":
    main()

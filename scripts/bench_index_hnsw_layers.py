#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Compare a frozen hnswlib graph through native, Provider and full-record SQL.

Requires a qualified native build/index record and a Vortex reference imported
from that same graph. SQL uses explicit prepared snapshot mode, not the strict
default. An isolated 1 GiB retention candidate is required for SIFT1M.
"""

import argparse
import csv
import json
import math
import os
import struct
import threading
import time
from pathlib import Path

import bench_index_layers as layers
import bench_index_sift as sift
import bench_index_sift_end_to_end as e2e
import bench_index_sql as bench

REVISION = "d9b3608c83d83b46c96e25088cb1d729b29dcfe9"


def identity(path):
    path = path.resolve(strict=True)
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": bench.fingerprint(path),
    }


def verify(record):
    bench.require(identity(Path(record["path"])) == record, "File identity changed")


def query_sql(reference, ef, mode="snapshot"):
    bench.require(
        mode in ("snapshot", "strict") and type(ef) is int and 10 <= ef <= 1048576,
        "Invalid SQL query options",
    )
    # Conflicting parameter casts force rebind; the index checks the dimension.
    return (
        'SELECT "row".id::UBIGINT AS id, "row".embedding::FLOAT[128] AS embedding, '
        "distance::FLOAT AS distance FROM vortex_index_search("
        f"{bench.quote(reference)}, $1::FLOAT[], 10, "
        f"validation_mode := {bench.quote(mode)}, backend_options := {bench.quote(json.dumps({'ef': ef}))}) "
        "ORDER BY rank;\n"
    )


def worker_arguments(
    layer, folder, args, entry, original, config, sql_path, query_path
):
    if layer == "native":
        return [
            "--mode",
            "measure",
            "--engine",
            "hnswlib",
            "--input",
            entry["files"]["queries"]["path"],
            "--index",
            original["index"]["path"],
            "--output",
            folder,
            "--rows",
            entry["rows"],
            "--dim",
            128,
            "--query-rows",
            entry["query_rows"],
            "--queries",
            args.queries,
            "--warmup",
            args.warmup,
            "--rounds",
            args.rounds,
            "--ef",
            args.ef,
            "--m",
            original["build_options"]["m"],
            "--threads",
            1,
        ]
    if layer == "provider":
        return [config, folder / "samples.json", "--barriers"]
    bench.require(layer == "sql", "Unknown worker layer")
    return [sql_path, query_path, folder / "samples.csv", args.warmup, args.rounds, 10]


def parse_native(path):
    current = None
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        bench.require(
            reader.fieldnames
            == ["round", "query", "rank", "native_id", "distance", "elapsed_ms"],
            "Invalid native header",
        )
        for item in reader:
            key = int(item["round"]), int(item["query"])
            if current is not None and key != (current["round"], current["query"]):
                yield current
                current = None
            latency = float(item["elapsed_ms"])
            if current is None:
                current = {
                    "round": key[0],
                    "query": key[1],
                    "latency_ms": latency,
                    "hits": [],
                }
            bench.require(
                int(item["rank"]) == len(current["hits"])
                and latency == current["latency_ms"],
                "Invalid native rank/timing",
            )
            current["hits"].append(
                {"id": int(item["native_id"]), "distance": float(item["distance"])}
            )
    if current is not None:
        yield current


def validate_provider_report(report):
    bench.require(
        type(report.get("format_version")) is int
        and report["format_version"] == 1
        and report.get("revision") == REVISION,
        "Provider report version or native revision differs",
    )
    for field in ("open_ms", "close_ms"):
        value = report.get(field)
        bench.require(
            type(value) in (int, float) and math.isfinite(value) and value >= 0,
            "Invalid Provider lifecycle timing",
        )


def validate_ann(
    records, base, queries, truth, warmup, rounds, prefixes=None, expected=None
):
    samples, first = [], {}
    for record in records:
        round_id, query_id = divmod(len(samples), len(queries))
        bench.require(
            type(record["round"]) is int
            and type(record["query"]) is int
            and (record["round"], record["query"]) == (round_id, query_id),
            "Missing, reordered or duplicate ANN sample",
        )
        if "warmup" in record:
            bench.require(
                type(record["warmup"]) is bool
                and record["warmup"] == (round_id < warmup),
                "Incorrect warmup marker",
            )
        latency = record["latency_ms"]
        bench.require(
            type(latency) in (int, float) and math.isfinite(latency) and latency > 0,
            "Invalid ANN latency",
        )
        hits = []
        for hit in record["hits"]:
            if prefixes is None:
                row_id = hit["id"]
            else:
                row = hit["row"]
                bench.require(
                    type(row["file_id"]) is int
                    and type(row["row_offset"]) is int
                    and row["file_id"] in prefixes,
                    "Invalid physical file identity",
                )
                prefix, count = prefixes[row["file_id"]]
                bench.require(
                    0 <= row["row_offset"] < count, "Physical row outside file"
                )
                row_id = prefix + row["row_offset"]
            hits.append(
                {
                    "id": row_id,
                    "distance": hit["distance"],
                    "embedding": (
                        base[row_id].tolist()
                        if type(row_id) is int and 0 <= row_id < len(base)
                        else []
                    ),
                }
            )
        e2e.validate_records(hits, queries[query_id], base)
        ranked = [{"id": hit["id"], "distance": hit["distance"]} for hit in hits]
        if query_id in first:
            layers.compare_hits(ranked, first[query_id])
        else:
            first[query_id] = ranked
        if expected is not None:
            layers.compare_hits(ranked, expected[query_id])
        samples.append(
            {
                "round": round_id,
                "query": query_id,
                "warmup": round_id < warmup,
                "latency_ms": latency,
            }
        )
    bench.require(
        len(samples) == len(queries) * (warmup + rounds), "Incomplete ANN rounds"
    )
    ranked = [first[i] for i in range(len(queries))]
    score, _ = sift.recall(ranked, truth)
    return (
        {
            **sift.statistics(samples),
            "recall_at_10": score,
            "repeat_parity": True,
            "verified_hits": len(samples) * 10,
        },
        samples,
        ranked,
    )


def host(cpu, sibling):
    ticks = {
        parts[0]: list(map(int, parts[1:]))
        for line in Path("/proc/stat").read_text().splitlines()
        if (parts := line.split()) and parts[0] in ("cpu", f"cpu{cpu}", f"cpu{sibling}")
    }
    compilers = []
    for path in Path("/proc").glob("[0-9]*/comm"):
        try:
            if path.read_text().strip() in ("cc1", "cc1plus", "rustc", "lto1"):
                compilers.append(int(path.parent.name))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
    return {
        "time": time.monotonic(),
        "ticks": ticks,
        "compiler_pids": compilers,
        "load": os.getloadavg(),
    }


def busy(first, last, key):
    delta = [b - a for a, b in zip(first["ticks"][key][:8], last["ticks"][key][:8])]
    bench.require(sum(delta) > 0 and min(delta) >= 0, "Invalid host CPU counters")
    return (sum(delta) - delta[3] - delta[4]) / sum(delta)


def pin_controller(cpu, sibling):
    controllers = sorted(os.sched_getaffinity(0) - {cpu, sibling})
    bench.require(controllers, "No separate controller CPU available")
    controller = 9 if 9 in controllers else controllers[0]
    os.sched_setaffinity(0, {controller})
    return controller


def quiet(output, cpu, sibling):
    snapshots = []
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        snapshots.append(host(cpu, sibling))
        bench.write_json(output / "preflight.json", snapshots)
        if len(snapshots) >= 12:
            window = snapshots[-12:]
            if (
                not any(item["compiler_pids"] for item in window)
                and busy(window[0], window[-1], "cpu") < 0.10
                and busy(window[0], window[-1], f"cpu{cpu}") < 0.10
                and busy(window[0], window[-1], f"cpu{sibling}") < 0.05
            ):
                return
        time.sleep(0.5)
    raise RuntimeError("Host did not become quiet within 15 minutes")


def host_qualification(runs, snapshots, sibling):
    compilers = sorted({pid for item in snapshots for pid in item["compiler_pids"]})
    windows = []
    for run in runs:
        phases = {
            item["phase"]: item["monotonic_seconds"]
            for item in run["resources"]["constraints"]["snapshots"]
        }
        first = max(
            (item for item in snapshots if item["time"] <= phases["ready"]),
            key=lambda item: item["time"],
        )
        last = min(
            (item for item in snapshots if item["time"] >= phases["after_close"]),
            key=lambda item: item["time"],
        )
        global_busy = busy(first, last, "cpu")
        sibling_busy = busy(first, last, f"cpu{sibling}")
        windows.append(
            {
                "layer": run["layer"],
                "repeat": run["repeat"],
                "global_busy_fraction": global_busy,
                "sibling_busy_fraction": sibling_busy,
                "qualified": global_busy < 0.10 and sibling_busy < 0.05,
            }
        )
    return {
        "qualified": not compilers and all(item["qualified"] for item in windows),
        "compiler_pids": compilers,
        "windows": windows,
    }


def pooled_statistics(samples):
    result = sift.statistics(samples)
    # Startup and individual rounds belong to their fresh process, not this pool.
    result.pop("first_query_in_fresh_process_ms")
    result.pop("round_p50_ms")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "input-manifest",
        "reference",
        "native-index-record",
        "native-build-provenance",
        "provider",
        "sql-build-provenance",
        "output-dir",
    ):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--dataset", choices=("sift100k", "sift1m"), required=True)
    parser.add_argument("--ef", type=int, required=True)
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--cpu", type=int, default=8)
    parser.add_argument("--sibling", type=int, default=26)
    parser.add_argument("--diagnostics", action="store_true")
    args = parser.parse_args()
    bench.require(
        not args.diagnostics or (args.warmup == 1 and args.rounds == 1),
        "Cache diagnostics require a separate one-warmup/one-measured-round run",
    )
    bench.require(
        1 <= args.queries <= 1000
        and 1 <= args.warmup <= 100
        and 1 <= args.rounds <= 100
        and 1 <= args.repeats <= 10,
        "Invalid repeat configuration",
    )
    # Input validation and preflight monitoring must not occupy the measured core.
    original_affinity = os.sched_getaffinity(0)
    controller = pin_controller(args.cpu, args.sibling)
    os.umask(0o077)
    os.environ.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    os.environ.setdefault(
        "DBUS_SESSION_BUS_ADDRESS", f"unix:path=/run/user/{os.getuid()}/bus"
    )
    output = args.output_dir.resolve()
    output.mkdir(mode=0o700)
    entry, base, queries, truth = sift.load_inputs(
        args.input_manifest, args.dataset, args.queries
    )
    original = json.loads(args.native_index_record.read_text())
    native_build = json.loads(args.native_build_provenance.read_text())
    sql_build = json.loads(args.sql_build_provenance.read_text())
    bench.require(
        original["status"] == "ok"
        and original["engine"] == "hnswlib"
        and original["inputs"] == entry,
        "Graph input identity differs",
    )
    bench.require(
        native_build["sources"]["hnswlib"]["revision"] == REVISION
        and original["binary"] == native_build["binary"],
        "Native build identity differs",
    )
    verify(original["index"])
    verify(native_build["binary"])
    bench.require(
        bench.fingerprint(Path(sql_build["binary"])) == sql_build["sha256"],
        "SQL binary identity changed",
    )
    reference = json.loads(args.reference.read_text())
    graph = (
        args.reference.parent
        / reference["generation"]["generation"]
        / "artifacts/hnswlib/index.bin"
    )
    bench.require(
        identity(graph)["sha256"] == original["index"]["sha256"],
        "Imported graph differs from native graph",
    )
    prefixes, count = {}, 0
    for file in reference["snapshot"]["files"]:
        bench.require(
            file["id"] not in prefixes
            and file["version"] == "sha256:" + bench.fingerprint(Path(file["uri"])),
            "Source file identity differs",
        )
        prefixes[file["id"]] = count, file["row_count"]
        count += file["row_count"]
    bench.require(count == entry["rows"], "Physical coverage differs")
    provider_identity = identity(args.provider)
    reference_identity = identity(args.reference)
    sql_identity = identity(Path(sql_build["binary"]))
    source_identities = [
        identity(Path(file["uri"])) for file in reference["snapshot"]["files"]
    ]
    query_path = output / "queries.f32bin"
    with query_path.open("xb") as stream:
        stream.write(struct.pack("<II", len(queries), 128))
        for query in queries:
            stream.write(struct.pack("<128f", *query))
    config = output / "provider.json"
    bench.write_json(
        config,
        {
            "format_version": 1,
            "reference": str(args.reference.resolve()),
            "queries": queries,
            "k": 10,
            "ef": args.ef,
            "warmup_rounds": args.warmup,
            "rounds": args.rounds,
        },
    )
    sql_path = output / "query.sql"
    sql_path.write_text(query_sql(args.reference.resolve(), args.ef))
    quiet(output, args.cpu, args.sibling)
    snapshots, done = [], threading.Event()

    def monitor():
        os.sched_setaffinity(0, {controller})
        while not done.is_set():
            snapshots.append(host(args.cpu, args.sibling))
            done.wait(0.1)

    thread = threading.Thread(target=monitor)
    thread.start()
    result = {
        "status": "in_progress",
        "dataset": args.dataset,
        "ef": args.ef,
        "query_count": len(queries),
        "warmup": args.warmup,
        "rounds": args.rounds,
        "process_repeats": args.repeats,
        "controller_cpu": controller,
        "validation_mode": "snapshot",
        "input_identity": entry,
        "graph_identity": original["index"],
        "reference_identity": reference_identity,
        "provider_identity": provider_identity,
        "sql_identity": sql_identity,
        "source_identities": source_identities,
        "driver_identity": identity(Path(__file__)),
        "native_build": native_build,
        "sql_build": sql_build,
        "runs": [],
    }
    expected, pooled = None, {"native": [], "provider": [], "sql": []}
    limits = {
        "cpu": args.cpu,
        "memory_mib": 2048,
        "warmup": args.warmup,
        "rounds": args.rounds,
    }
    try:
        for repeat in range(args.repeats):
            order = (
                ("native", "provider", "sql")
                if repeat % 2 == 0
                else ("sql", "provider", "native")
            )
            for layer in order:
                preflight = output / f"preflight-{repeat}-{layer}"
                preflight.mkdir(mode=0o700)
                quiet(preflight, args.cpu, args.sibling)
                binary = {
                    "native": Path(native_build["binary"]["path"]),
                    "provider": args.provider.resolve(),
                    "sql": Path(sql_build["binary"]),
                }[layer]

                def arguments(folder, layer=layer):
                    return worker_arguments(
                        layer,
                        folder,
                        args,
                        entry,
                        original,
                        config,
                        sql_path,
                        query_path,
                    )

                # The resource controller validates both CPUs before isolating
                # itself; its systemd worker receives its own explicit affinity.
                os.sched_setaffinity(0, original_affinity)
                try:
                    directory, metadata = e2e.run_worker(
                        output,
                        f"{repeat}-{layer}",
                        binary,
                        arguments,
                        Path.cwd(),
                        900,
                        limits,
                        1,
                        sql_timing=args.diagnostics and layer == "sql",
                        controller_cpu=controller,
                    )
                finally:
                    os.sched_setaffinity(0, {controller})
                bench.require(
                    metadata["constraints"]["verified"]
                    and not metadata["logged_errors"],
                    "Unqualified resource run",
                )
                if layer == "sql":
                    summary, samples = e2e.summarize_records(
                        e2e.parse_capi(directory / "samples.csv"),
                        base,
                        queries,
                        truth,
                        args.warmup,
                        args.rounds,
                        expected,
                    )
                    if args.diagnostics:
                        summary["cache"] = e2e.sql_cache_hits(
                            directory / "stderr.log", len(queries)
                        )
                else:
                    report = (
                        json.loads((directory / "samples.json").read_text())
                        if layer == "provider"
                        else None
                    )
                    if report is not None:
                        validate_provider_report(report)
                    records = (
                        report["samples"]
                        if report
                        else parse_native(directory / "samples.csv")
                    )
                    summary, samples, ranked = validate_ann(
                        records,
                        base,
                        queries,
                        truth,
                        args.warmup,
                        args.rounds,
                        prefixes if report else None,
                        expected,
                    )
                    if expected is None:
                        expected = ranked
                    summary["lifecycle"] = (
                        {key: report[key] for key in ("open_ms", "close_ms")}
                        if report
                        else json.loads((directory / "metrics.json").read_text())
                    )
                target = {"sift100k": 0.9977, "sift1m": 0.9940}[args.dataset]
                # Tiny smoke subsets have no official per-subset recall threshold.
                if len(queries) == 1000:
                    bench.require(
                        summary["recall_at_10"] + 1e-12 >= target,
                        "Recall floor not met",
                    )
                pooled[layer].extend(samples)
                result["runs"].append(
                    {
                        "repeat": repeat,
                        "layer": layer,
                        "directory": str(directory),
                        "summary": summary,
                        "resources": metadata,
                    }
                )
                bench.write_json(output / "report.json", result)
        verify(original["index"])
        verify(native_build["binary"])
        verify(provider_identity)
        verify(reference_identity)
        verify(sql_identity)
        bench.require(
            identity(graph)["sha256"] == original["index"]["sha256"],
            "Imported graph changed",
        )
        for source_identity in source_identities:
            verify(source_identity)
        sift.load_inputs(args.input_manifest, args.dataset, args.queries)
    except (Exception, KeyboardInterrupt) as error:
        result.update(
            status="interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
            error=str(error) or type(error).__name__,
        )
        bench.write_json(output / "report.json", result)
        raise
    finally:
        done.set()
        thread.join()
        snapshots.append(host(args.cpu, args.sibling))
        bench.write_json(output / "host.json", snapshots)
    qualification = host_qualification(result["runs"], snapshots, args.sibling)
    if not qualification["qualified"]:
        result.update(status="excluded", host_qualification=qualification)
        bench.write_json(output / "report.json", result)
        raise RuntimeError("Host interference invalidates matrix; see report.json")
    result.update(
        status="ok",
        host_qualification=qualification,
        host_windows=qualification["windows"],
        pooled={layer: pooled_statistics(samples) for layer, samples in pooled.items()},
        timing_boundaries={
            "native": "C++ ANN + ID/distance extraction/sort",
            "provider": "Rust public Provider ANN + FFI + row mapping/sort",
            "sql": "C API prepared execute + ANN + Vortex original-row take + full-vector extraction; snapshot validation",
        },
    )
    bench.write_json(output / "report.json", result)
    print(
        json.dumps(
            {
                "status": result["status"],
                "dataset": args.dataset,
                "pooled": result["pooled"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Qualify explicit static SPFresh index SQL across independent shell processes."""

import argparse
import csv
import json
import math
import subprocess
from pathlib import Path


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, required=True)
    parser.add_argument(
        "--extension",
        type=Path,
        help="Load an unsigned artifact instead of a built-in extension",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Fresh directory for sources, index, and report",
    )
    args = parser.parse_args()
    work = args.output_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    reference = work / "index.json"
    rows, dimension, k = 4096, 8, 10
    files = [work / "even.vortex", work / "odd.vortex"]
    options = json.dumps(
        {
            "format_version": 1,
            "dimension": dimension,
            "head_count": 64,
            "posting_page_limit": 12,
            "replicas": 4,
        }
    )
    prefix = f"LOAD {quote(args.extension.resolve())};\n" if args.extension else ""
    processes = 0

    def run(sql, expected_error=None):
        nonlocal processes
        processes += 1
        command = [str(args.duckdb.resolve()), "-batch", "-bail"]
        if args.extension:
            command.append("-unsigned")
        result = subprocess.run(
            command + [":memory:"],
            input=prefix + "SET threads=1;\n" + sql,
            text=True,
            capture_output=True,
            timeout=180,
        )
        (work / f"process-{processes}.log").write_text(result.stdout + result.stderr)
        if expected_error is None:
            require(result.returncode == 0, result.stdout + result.stderr)
        else:
            require(
                result.returncode != 0
                and expected_error.lower() in result.stderr.lower(),
                f"Expected {expected_error!r}: {result.stdout}{result.stderr}",
            )
        return result

    def build(ref=reference, build_files=files, build_options=options):
        return (
            f"SELECT * FROM vortex_index_build([{','.join(map(quote, build_files))}], "
            f"{quote(ref)}, 'embedding', 'spfresh.static', {quote(build_options)})"
        )

    def vector(row):
        return [float(row)] + [
            float((row * (axis + 3)) % (11 + axis)) for axis in range(1, dimension)
        ]

    def search(query, suffix="", ref=reference, topk=k):
        return (
            f"vortex_index_search({quote(ref)}, "
            f"[{','.join(str(value) + '::FLOAT' for value in query)}], {topk}{suffix})"
        )

    source_sql = []
    for file_id, path in enumerate(files):
        components = ["id::FLOAT"] + [
            f"((id * {axis + 3}) % {11 + axis})::FLOAT" for axis in range(1, dimension)
        ]
        source_sql.append(
            f"COPY (SELECT id::UBIGINT AS id, [{','.join(components)}]::FLOAT[{dimension}] AS embedding, 'row-' || id AS label FROM (SELECT i * 2 + {file_id} AS id FROM range({rows // 2}) t(i))) TO {quote(path)} (FORMAT vortex);"
        )
    run("\n".join(source_sql) + f"\nEXPLAIN {build()};")
    require(not reference.exists(), "EXPLAIN published an index reference")
    run(build() + ";")
    require(reference.is_file(), "Build did not publish the reference")

    queries = [7, 133, 1024, 2049, 4000]
    recalls = []
    for row in queries:
        query = vector(row)
        call = search(query)
        columns = (
            'rank, file_id, row_offset, distance, "row".id AS id, "row".label AS label'
        )
        output = work / f"query-{row}.csv"
        run(
            f"COPY (SELECT {columns} FROM {call} ORDER BY rank) TO {quote(output)} (HEADER true);"
        )
        with output.open() as stream:
            actual = list(csv.DictReader(stream))
        require(len(actual) == k, "Search did not return k rows")
        ids = [int(hit["id"]) for hit in actual]
        require(len(set(ids)) == k, "Search returned duplicate addresses")
        require(
            ids[0] == row and float(actual[0]["distance"]) == 0,
            "Self-neighbor was not recovered",
        )
        expected = sorted(
            range(rows),
            key=lambda i: (sum((a - b) ** 2 for a, b in zip(query, vector(i))), i),
        )[:k]
        recalls.append(len(set(ids).intersection(expected)) / k)
        order = []
        for rank, hit in enumerate(actual, 1):
            found = int(hit["id"])
            distance = float(hit["distance"])
            exact = sum((a - b) ** 2 for a, b in zip(query, vector(found)))
            require(int(hit["rank"]) == rank, "Rank was not preserved")
            require(
                found == int(hit["row_offset"]) * 2 + int(hit["file_id"]) - 1,
                "Physical address and original row differ",
            )
            require(
                hit["label"] == f"row-{found}"
                and math.isclose(distance, exact, rel_tol=1e-5, abs_tol=1e-5),
                "Original row or squared-L2 score differs",
            )
            order.append((distance, int(hit["file_id"]), int(hit["row_offset"])))
        require(order == sorted(order), "Results are not ordered by distance/address")
        explicit_call = search(
            query,
            suffix=", backend_options := "
            + quote('{"max_check":4096,"internal_results":64,"search_pages":12}'),
        )
        repeated = work / f"repeat-{row}.csv"
        run(
            f"CREATE TEMP TABLE result AS SELECT {columns} FROM {explicit_call} WHERE false;\nPREPARE nearest AS INSERT INTO result SELECT {columns} FROM {explicit_call};\nEXECUTE nearest;\nCOPY (SELECT * FROM result ORDER BY rank) TO {quote(repeated)} (HEADER true);"
        )
        require(
            output.read_bytes() == repeated.read_bytes(),
            "Cross-process prepared search differed",
        )

    query = vector(133)
    negatives = [
        (build() + ";", "already exists"),
        (f"SELECT * FROM {search(query, topk=0)};", "between 1"),
        (f"SELECT * FROM {search([0.0])};", "dimension"),
        (
            f"SELECT * FROM {search(query, suffix=', backend_options := ' + quote('{"unknown":1}'))};",
            "unknown field",
        ),
        (
            f"SELECT * FROM {search(query, suffix=', backend_options := ' + quote('{"max_check":4096,"internal_results":64,"search_pages":1}'))};",
            "query options",
        ),
        (
            f"SET enable_external_access=false; SELECT * FROM {search(query)};",
            "external access",
        ),
    ]
    for sql, message in negatives:
        run(sql, message)
    run(
        f"COPY (SELECT NULL::FLOAT[{dimension}] AS embedding FROM range(128)) TO {quote(work / 'null.vortex')} (FORMAT vortex);"
    )
    run(build(work / "null-index.json", [work / "null.vortex"]) + ";", "NULL")
    require(not (work / "null-index.json").exists(), "NULL build published a reference")

    before = reference.read_bytes()
    run(
        f"PREPARE nearest AS SELECT * FROM {search(query)}; COPY (SELECT 'changed') TO {quote(reference)} (FORMAT csv); EXECUTE nearest;",
        "changed after bind",
    )
    reference.write_bytes(before)
    run(f"SELECT * FROM {search(query)};")
    moved = files[0].with_suffix(".moved")
    files[0].rename(moved)
    try:
        run(f"SELECT * FROM {search(query)};", "No such file")
    finally:
        moved.rename(files[0])
    original = files[0].read_bytes()
    files[0].write_bytes(original + b"changed")
    try:
        run(f"SELECT * FROM {search(query)};", "version")
    finally:
        files[0].write_bytes(original)

    descriptor = json.loads(reference.read_text())
    generation = work / descriptor["generation"]["generation"]
    manifest = generation / "manifest.json"
    artifacts = json.loads(manifest.read_text())["artifacts"]
    for path in [manifest, generation / "artifacts" / artifacts[0]["path"]]:
        original = path.read_bytes()
        path.write_bytes(original + b"changed")
        try:
            run(f"SELECT * FROM {search(query)};", "mismatch")
        finally:
            path.write_bytes(original)
    run(f"SELECT * FROM {search(query)};")

    report = {
        "status": "ok",
        "duckdb": str(args.duckdb.resolve()),
        "rows": rows,
        "dimension": dimension,
        "k": k,
        "queries": len(queries),
        "processes": processes,
        "recall_at_k": sum(recalls) / len(recalls),
        "cross_process_reopen": True,
        "prepared_repeat_equal": True,
        "backend_options_parity": True,
        "ranked_original_rows": True,
        "negative_cases": len(negatives) + 6,
        "reference": str(reference),
    }
    (work / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

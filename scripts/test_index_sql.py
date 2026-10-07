#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""Qualify an explicit static backend across independent SQL shell processes."""

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
        "--backend",
        choices=("spfresh.static", "hnswlib.static"),
        default="spfresh.static",
    )
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
        if args.backend == "spfresh.static"
        else {
            "format_version": 1,
            "dimension": dimension,
            "m": 16,
            "ef_construction": 100,
            "seed": 100,
            "threads": 1,
        }
    )
    query_options = (
        '{"max_check":4096,"internal_results":64,"search_pages":12}'
        if args.backend == "spfresh.static"
        else '{"ef":64}'
    )
    invalid_query_options = (
        '{"max_check":4096,"internal_results":64,"search_pages":1}'
        if args.backend == "spfresh.static"
        else '{"ef":1}'
    )
    prefix = f"LOAD {quote(args.extension.resolve())};\n" if args.extension else ""
    processes = 0
    negative_cases = 0

    def run(sql, expected_error=None):
        nonlocal processes, negative_cases
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
            check=False,
        )
        (work / f"process-{processes}.log").write_text(result.stdout + result.stderr)
        if expected_error is None:
            require(result.returncode == 0, result.stdout + result.stderr)
        else:
            negative_cases += 1
            require(
                result.returncode > 0
                and expected_error.lower() in result.stderr.lower(),
                f"Expected {expected_error!r}: {result.stdout}{result.stderr}",
            )
        return result

    def build(ref=reference, build_files=files, build_options=options):
        return (
            f"SELECT * FROM vortex_index_build([{','.join(map(quote, build_files))}], "
            f"{quote(ref)}, 'embedding', {quote(args.backend)}, {quote(build_options)})"
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
            suffix=", backend_options := " + quote(query_options),
        )
        repeated = work / f"repeat-{row}.csv"
        query_sql = "[" + ",".join(str(value) + "::FLOAT" for value in query) + "]"
        search_options = quote(query_options)
        parameterized_call = (
            f"vortex_index_search({quote(reference)}, $1, $2, backend_options := $3)"
        )
        run(
            f"CREATE TEMP TABLE result AS SELECT {columns} FROM {explicit_call} WHERE false;\nPREPARE nearest AS INSERT INTO result SELECT {columns} FROM {parameterized_call};\nEXECUTE nearest({query_sql}, {k}, {search_options});\nCOPY (SELECT * FROM result ORDER BY rank) TO {quote(repeated)} (HEADER true);"
        )
        require(
            output.read_bytes() == repeated.read_bytes(),
            "Cross-process prepared search differed",
        )
        snapshot = work / f"snapshot-{row}.csv"
        run(
            f"CREATE TEMP TABLE result AS SELECT {columns} FROM {explicit_call} WHERE false;\n"
            f"PREPARE nearest AS INSERT INTO result SELECT {columns} FROM vortex_index_search({quote(reference)}, $1, $2, backend_options := $3, validation_mode := 'snapshot');\n"
            f"EXECUTE nearest({query_sql}, {k}, {search_options}); DELETE FROM result;\n"
            f"EXECUTE nearest({query_sql}, {k}, {search_options});\n"
            f"COPY (SELECT * FROM result ORDER BY rank) TO {quote(snapshot)} (HEADER true);"
        )
        require(
            output.read_bytes() == snapshot.read_bytes(),
            "Prepared snapshot differed from strict search",
        )

    query = vector(133)
    nul_reference = work / "nul-index.json"
    blocked_reference = work / "blocked-index.json"
    negatives = [
        (build() + ";", "already exists"),
        (f"SELECT * FROM {search(query, topk=0)};", "between 1"),
        (f"SELECT * FROM {search([0.0])};", "dimension"),
        (
            f"SELECT * FROM {search(query, suffix=', backend_options := ' + quote('{"unknown":1}'))};",
            "unknown field",
        ),
        (
            f"SELECT * FROM {search(query, suffix=', backend_options := ' + quote(invalid_query_options))};",
            "query options" if args.backend == "spfresh.static" else "ef",
        ),
        (
            f"SET enable_external_access=false; SELECT * FROM {search(query)};",
            "external access",
        ),
        (
            f"SELECT * FROM vortex_index_search({quote(reference)} || chr(0) || 'missing', [{','.join(str(value) + '::FLOAT' for value in query)}], {k});",
            "NUL",
        ),
        (
            f"SELECT * FROM {search(query, suffix=', backend_options := chr(0) || ' + quote('{"unknown":1}'))};",
            "NUL",
        ),
    ]
    build_inputs = [
        f"[{','.join(map(quote, files))}]",
        quote(nul_reference),
        "'embedding'",
        quote(args.backend),
        quote(options),
    ]
    for argument in range(len(build_inputs)):
        inputs = build_inputs.copy()
        inputs[argument] = (
            f"[{quote(files[0])} || chr(0) || 'missing', {quote(files[1])}]"
            if argument == 0
            else f"{inputs[argument]} || chr(0) || 'missing'"
        )
        negatives.append(
            (f"SELECT * FROM vortex_index_build({','.join(inputs)});", "NUL")
        )
    for operation in [
        f"SELECT * FROM {search(query)}",
        build(blocked_reference),
    ]:
        for prepared in [False, True]:
            sql = (
                f"PREPARE indexed AS {operation}; SET disabled_filesystems='LocalFileSystem'; EXECUTE indexed;"
                if prepared
                else f"SET disabled_filesystems='LocalFileSystem'; {operation};"
            )
            negatives.append((sql, "LocalFileSystem"))
    directories = {path.name for path in work.iterdir() if path.is_dir()}
    for sql, message in negatives:
        run(sql, message)
        require(
            not nul_reference.exists() and not blocked_reference.exists(),
            "Rejected arguments or filesystem policy published an index",
        )
        require(
            directories == {path.name for path in work.iterdir() if path.is_dir()},
            "Rejected operation left a generation or scratch directory",
        )
    run(f"SET disabled_filesystems='PipeFileSystem'; SELECT * FROM {search(query)};")
    if args.backend == "hnswlib.static":
        for threads in (0, 2, 4, 8):
            before = {path for path in work.iterdir() if path.is_dir()}
            build_options = json.dumps({**json.loads(options), "threads": threads})
            run(
                build(blocked_reference, build_options=build_options) + ";", "threads=1"
            )
            require(
                not blocked_reference.exists(),
                "Rejected thread count published a reference",
            )
            # SQL creates the store before invoking the backend; unsealed empty
            # generations follow the existing owner-managed cleanup contract.
            for path in {path for path in work.iterdir() if path.is_dir()} - before:
                require(
                    path.name.startswith("generation-")
                    and not any(entry.is_file() for entry in path.rglob("*")),
                    "Rejected thread count left scratch or published artifacts",
                )
    run(
        f"COPY (SELECT NULL::FLOAT[{dimension}] AS embedding FROM range(128)) TO {quote(work / 'null.vortex')} (FORMAT vortex);"
    )
    run(build(work / "null-index.json", [work / "null.vortex"]) + ";", "NULL")
    require(not (work / "null-index.json").exists(), "NULL build published a reference")

    directories = {path.name for path in work.iterdir() if path.is_dir()}
    invalid_vectors = [
        "[1::FLOAT, 2::FLOAT]",
        "1::FLOAT",
        "['a', 'b']::VARCHAR[2]",
        "[true, false]::BOOLEAN[2]",
        "[1::DOUBLE, 2::DOUBLE]::DOUBLE[2]",
    ]
    for case, expression in enumerate(invalid_vectors):
        invalid_file = work / f"invalid-{case}.vortex"
        invalid_reference = work / f"invalid-{case}.json"
        run(
            f"COPY (SELECT {expression} AS embedding FROM range(128)) TO {quote(invalid_file)} (FORMAT vortex);"
        )
        run(build(invalid_reference, [invalid_file]) + ";", "Float32")
        require(
            not invalid_reference.exists(), "Invalid vector type published an index"
        )
        require(
            directories == {path.name for path in work.iterdir() if path.is_dir()},
            "Invalid vector type left a generation or scratch directory",
        )

    replacement_file = work / "replacement.vortex"
    replacement_reference = work / "replacement.json"
    components = ["id::FLOAT"] + [
        f"((id * {axis + 3}) % {11 + axis})::FLOAT" for axis in range(1, dimension)
    ]
    run(
        f"COPY (SELECT id::UBIGINT AS id, [{','.join(components)}]::FLOAT[{dimension}] AS embedding, 'row-' || id AS label FROM range(5000, 5256) t(id)) TO {quote(replacement_file)} (FORMAT vortex);\n"
        + build(replacement_reference, [replacement_file])
        + ";"
    )
    parameterized_queries = []
    for mode in ("strict", "snapshot"):
        call = f"vortex_index_search({quote(reference)}, $1, {k}, validation_mode := {quote(mode)})"
        single = f"vortex_index_search({quote(reference)}, $1, 1, validation_mode := {quote(mode)})"
        parameterized_queries.extend(
            [
                f"SELECT * FROM {call}",
                f"WITH hits AS (SELECT * FROM {call}) SELECT * FROM hits",
                f'SELECT (SELECT "row".id FROM {single})',
                f'SELECT "row".id FROM {single} UNION ALL SELECT 999::UBIGINT WHERE false',
            ]
        )
    before = reference.read_bytes()
    query_sql = "[" + ",".join(str(value) + "::FLOAT" for value in query) + "]"
    for prepared_query in parameterized_queries:
        try:
            run(
                f"PREPARE nearest AS {prepared_query}; EXECUTE nearest({query_sql});\n"
                f"COPY (SELECT content FROM read_text({quote(replacement_reference)})) TO {quote(reference)} (FORMAT csv, HEADER false, QUOTE '', ESCAPE '');\n"
                f"EXECUTE nearest({query_sql});",
                "reference changed",
            )
            require(
                json.loads(reference.read_bytes())
                == json.loads(replacement_reference.read_bytes()),
                "Prepared regression must replace the reference with valid JSON",
            )
        finally:
            reference.write_bytes(before)

    constant_call = search(query, topk=1)
    initial_bind_queries = [
        f'SELECT "row".id, $1 AS parameter FROM {constant_call}',
        f'SELECT "row".id FROM {constant_call} WHERE rank >= $1',
        f'SELECT "row".id FROM {constant_call} LIMIT $1',
        f'WITH hits AS (SELECT * FROM {constant_call}) SELECT "row".id, $1 FROM hits',
        f'SELECT (SELECT "row".id FROM {constant_call}), $1',
        f'SELECT a."row".id FROM {constant_call} a CROSS JOIN vortex_index_search({quote(reference)}, [$1::FLOAT, {",".join(str(value) + "::FLOAT" for value in query[1:])}], 1) b',
    ]
    for prepared_query in initial_bind_queries:
        try:
            run(
                f"PREPARE nearest AS {prepared_query};\n"
                f"COPY (SELECT content FROM read_text({quote(replacement_reference)})) TO {quote(reference)} (FORMAT csv, HEADER false, QUOTE '', ESCAPE '');\n"
                "EXECUTE nearest(1);",
                "reference changed",
            )
            require(
                json.loads(reference.read_bytes())
                == json.loads(replacement_reference.read_bytes()),
                "First-execution regression must use a valid replacement reference",
            )
            refreshed = work / "refreshed.csv"
            run(
                f'PREPARE refreshed AS COPY (SELECT "row".id AS id FROM {constant_call} LIMIT $1) TO {quote(refreshed)} (HEADER true); EXECUTE refreshed(1);'
            )
            with refreshed.open() as stream:
                actual = list(csv.DictReader(stream))
            require(
                len(actual) == 1 and int(actual[0]["id"]) == 5000,
                "A newly prepared statement did not accept the replacement generation",
            )
            refreshed.unlink()
        finally:
            reference.write_bytes(before)

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

    snapshot_call = search(query, suffix=", validation_mode := 'snapshot'")
    for case, path in enumerate(
        [files[0], manifest, generation / "artifacts" / artifacts[0]["path"]]
    ):
        original = path.read_bytes()
        output = work / f"snapshot-retained-{case}.csv"
        try:
            run(
                f'CREATE TEMP TABLE result AS SELECT rank, "row".id AS id, "row".label AS label FROM {snapshot_call} WHERE false;\n'
                f'PREPARE nearest AS INSERT INTO result SELECT rank, "row".id, "row".label FROM {snapshot_call};\n'
                f"EXECUTE nearest; DELETE FROM result;\n"
                f"COPY (SELECT 'changed') TO {quote(path)} (FORMAT csv);\n"
                f"EXECUTE nearest;\n"
                f"COPY (SELECT * FROM result ORDER BY rank) TO {quote(output)} (HEADER true);"
            )
            with (work / "query-133.csv").open() as stream:
                expected = [
                    (hit["rank"], hit["id"], hit["label"])
                    for hit in csv.DictReader(stream)
                ]
            with output.open() as stream:
                actual = [
                    (hit["rank"], hit["id"], hit["label"])
                    for hit in csv.DictReader(stream)
                ]
            require(
                actual == expected, "Snapshot did not retain original rows and index"
            )
            run(
                f"SELECT * FROM {search(query)};",
                "version" if case == 0 else "mismatch",
            )
        finally:
            path.write_bytes(original)
    for setting, error in (
        ("enable_external_access=false", "external access"),
        ("disabled_filesystems='LocalFileSystem'", "LocalFileSystem"),
    ):
        run(
            f"PREPARE nearest AS SELECT * FROM {snapshot_call}; EXECUTE nearest; SET {setting}; EXECUTE nearest;",
            error,
        )

    report = {
        "status": "ok",
        "backend": args.backend,
        "duckdb": str(args.duckdb.resolve()),
        "rows": rows,
        "dimension": dimension,
        "k": k,
        "queries": len(queries),
        "processes": processes,
        "recall_at_k": sum(recalls) / len(recalls),
        "cross_process_reopen": True,
        "prepared_repeat_equal": True,
        "snapshot_repeat_equal": True,
        "snapshot_retains_verified_contents": True,
        "snapshot_checks_execution_access": True,
        "backend_options_parity": True,
        "ranked_original_rows": True,
        "nul_arguments_rejected": True,
        "disabled_local_filesystem_rejected": True,
        "invalid_vector_types_rejected": True,
        "parameterized_reference_pinned": True,
        "first_execution_reference_pinned": True,
        "negative_cases": negative_cases,
        "reference": str(reference),
    }
    (work / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

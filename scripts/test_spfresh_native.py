#!/usr/bin/env python3
"""Qualify SPFresh searches and Vortex IO in the same native Vane shell."""

import argparse
import csv
import json
import struct
import subprocess
import tempfile
from pathlib import Path


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def run(shell, sql):
    return subprocess.run(
        [str(shell), "-batch", "-bail", ":memory:"],
        input=sql,
        text=True,
        capture_output=True,
        timeout=180,
    )


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read_csv(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, required=True)
    parser.add_argument("--reference-summary", type=Path, required=True)
    parser.add_argument("--queries", type=Path, required=True, help="SpaceV Int8 vector file")
    parser.add_argument("--query-count", type=int, default=100)
    parser.add_argument("--output-json", type=Path, required=True)
    args = parser.parse_args()
    reference = json.loads(args.reference_summary.read_text())
    params = reference["params"]
    fixture = reference["fixture"]
    count = args.query_count
    require(0 < count <= fixture["query_count"], "Query count exceeds the reference")
    require(fixture["value_type"] == "Int8", "Expected a SpaceV Int8 reference")
    with args.queries.open("rb") as stream:
        rows, dim = struct.unpack("<ii", stream.read(8))
        require(count <= rows, "Query file has too few rows")
        require(dim == fixture["dim"], "Query dimension differs from the reference")
        values = struct.unpack(f"<{count * dim}b", stream.read(count * dim))
    queries = [values[i * dim : (i + 1) * dim] for i in range(count)]
    blob = struct.pack(f"<{len(values)}f", *values).hex()
    table = reference["table_path"]
    index = reference["index_name"]
    k = fixture["k"]
    options = (
        ", spfresh_backend := 'ssdserving_lib'"
        f", spfresh_max_check := {params['max_check']}"
        f", spfresh_internal_result_num := {params['internal_result_num']}"
        f", spfresh_posting_page_limit := {params['posting_page_limit']}"
    )

    def json_call(payload, topk=k, suffix=options, table_override=None):
        return (
            f"vortex_spfresh_search_batch({quote(table_override or table)}, {quote(index)}, "
            f"{quote(payload)}, {topk}{suffix})"
        )

    def blob_call(payload, dimension=dim):
        return (
            f"vortex_spfresh_search_batch_blob({quote(table)}, {quote(index)}, "
            f"{payload}, {count}, {dimension}, {k}{options})"
        )

    with tempfile.TemporaryDirectory(prefix="vane-spfresh-test-") as directory:
        work = Path(directory)
        payload = json.dumps(queries)
        json_search = json_call(payload)
        blob_search = blob_call(f"from_hex({quote(blob)})")
        sql = f"""
SET threads=1;
COPY (SELECT i::BIGINT AS id FROM range(100) t(i)) TO {quote(work / 'rows.vortex')} (FORMAT vortex);
COPY (SELECT count(*) AS n, sum(id) AS total FROM read_vortex({quote(work / 'rows.vortex')}))
TO {quote(work / 'vortex.csv')} (HEADER true);
CREATE TEMP TABLE json_results AS SELECT * FROM {json_search};
CREATE TEMP TABLE blob_results AS SELECT * FROM {blob_search};
CREATE TEMP TABLE repeated_results AS SELECT * FROM {json_search};
COPY (SELECT * FROM json_results ORDER BY query_id, distance, id) TO {quote(work / 'json.csv')} (HEADER true);
COPY (SELECT * FROM blob_results ORDER BY query_id, distance, id) TO {quote(work / 'blob.csv')} (HEADER true);
COPY (SELECT * FROM repeated_results ORDER BY query_id, distance, id) TO {quote(work / 'repeat.csv')} (HEADER true);
CREATE TEMP TABLE prepared_results AS SELECT * FROM json_results WHERE false;
PREPARE insert_blob AS INSERT INTO prepared_results SELECT * FROM {blob_call('$1')};
EXECUTE insert_blob(from_hex({quote(blob)}));
COPY (SELECT * FROM prepared_results ORDER BY query_id, distance, id)
TO {quote(work / 'prepared.csv')} (HEADER true);
"""
        result = run(args.duckdb, sql)
        require(result.returncode == 0, result.stdout + result.stderr)
        io_rows = read_csv(work / "vortex.csv")
        require(io_rows == [{"n": "100", "total": "4950"}], "Vortex IO round trip failed")
        json_rows = read_csv(work / "json.csv")
        blob_rows = read_csv(work / "blob.csv")
        repeat_rows = read_csv(work / "repeat.csv")
        prepared_rows = read_csv(work / "prepared.csv")
        require(json_rows == blob_rows == repeat_rows == prepared_rows, "JSON/BLOB/repeated/prepared results differ")
        require(len(json_rows) == count * k, "Unexpected number of search results")
        by_query = [[] for _ in range(count)]
        for row in json_rows:
            by_query[int(row["query_id"])].append((int(row["id"]), float(row["distance"])))
        direct = reference["direct_dynamic_ffi_search"]["search"]["topk_by_query"]
        truths = reference["ground_truth_by_query"]
        recalls = []
        for i, actual in enumerate(by_query):
            expected = sorted(
                zip(direct[i]["topk_ids_by_run"][-1], direct[i]["topk_distances_by_run"][-1]),
                key=lambda item: (item[1], item[0]),
            )
            require(actual == expected, f"Direct C ABI parity failed for query {i}")
            ids = {item[0] for item in actual}
            require(len(ids) == k, f"Duplicate IDs for query {i}")
            recalls.append(len(ids.intersection(truths[i]["ids"])) / k)

        negative_cases = [
            (json_call("[]"), "nonempty JSON array"),
            (json_call("[[0]]"), "dimension"),
            (json_call("not-json"), "Invalid SPFresh JSON"),
            (json_call(payload, topk=0), "positive uint32"),
            (json_call(payload, suffix=", spfresh_backend := 'managed_process'"), "ssdserving_lib"),
            (blob_call("from_hex('00')"), "BLOB length"),
            (blob_call(f"from_hex({quote(blob)})", dimension=dim + 1), "dimension"),
            (blob_call(f"from_hex({quote(struct.pack('<f', float('nan')).hex() + blob[8:])})"), "finite"),
        ]
        for call, error in negative_cases:
            failed = run(args.duckdb, f"SELECT * FROM {call};")
            require(failed.returncode != 0 and error in failed.stderr, failed.stdout + failed.stderr)
        denied = run(args.duckdb, f"SET enable_external_access=false; SELECT * FROM {json_search};")
        require(denied.returncode != 0 and "enable_external_access" in denied.stderr, "External access check failed")
        bad_table = work / "bad-table"
        (bad_table / "indexes").mkdir(parents=True)
        catalog = json.loads((Path(table) / "indexes" / "_index_catalog.json").read_text())
        entry = next(entry for entry in catalog if entry["name"] == index)
        original_manifest = Path(entry["external_manifest"])
        require(not original_manifest.is_absolute() and ".." not in original_manifest.parts, "Invalid manifest path")
        manifest = json.loads((Path(table) / original_manifest).read_text())
        destination = bad_table / original_manifest
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(manifest))
        entry["dim"] += 1
        (bad_table / "indexes" / "_index_catalog.json").write_text(json.dumps(catalog))
        invalid = run(args.duckdb, f"SELECT * FROM {json_call(payload, table_override=bad_table)};")
        require(invalid.returncode != 0 and "does not match the catalog" in invalid.stderr, invalid.stderr)
        report = {
            "status": "ok",
            "duckdb": str(args.duckdb.resolve()),
            "reference_summary": str(args.reference_summary.resolve()),
            "query_count": count,
            "k": k,
            "recall_at_k": sum(recalls) / count,
            "direct_ffi_parity_queries": count,
            "json_blob_repeat_equal": True,
            "vortex_io_round_trip": True,
            "prepared_blob_executed": True,
            "negative_cases_passed": len(negative_cases) + 2,
        }
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

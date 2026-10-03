# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import copy
import csv
import importlib
import json
import math
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def layers(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("bench_index_layers")


@pytest.fixture
def config():
    return {"queries": [[1.0, 2.0]], "k": 2, "warmup_rounds": 1, "rounds": 1}


@pytest.fixture
def native_csv():
    return (
        "event,round,query,rank,native_id,distance,elapsed_ms\n"
        "open,,,,,,3\n"
        "sample,0,0,0,0,1,100\n"
        "sample,0,0,1,1,2,100\n"
        "sample,1,0,0,0,1,10\n"
        "sample,1,0,1,1,2,10\n"
        "close,,,,,,2\n"
    )


def test_native_csv_rounds_ranks_and_lifecycle(layers, config, native_csv, tmp_path):
    path = tmp_path / "samples.csv"
    path.write_text(native_csv)
    report = layers.parse_native(path, config)
    assert report["open_ms"] == 3
    assert report["close_ms"] == 2
    assert [sample["warmup"] for sample in report["samples"]] == [True, False]
    assert report["samples"][1]["hits"] == [
        {"native_id": 0, "distance": 1},
        {"native_id": 1, "distance": 2},
    ]
    summary = layers.distribution(report["samples"])
    assert summary["samples"] == 1
    assert summary["p50_ms"] == 10
    assert summary["first_query_in_fresh_process_ms"] == 100
    assert summary["serial_qps_from_query_latency"] == 100


@pytest.mark.parametrize(
    "old,new,message",
    [
        ("event,round", "unknown,round", "header"),
        ("open,,,,,,3", "open,,,,,,nan", "timing"),
        ("open,,,,,,3", "open,,,,,,-1", "timing"),
        ("open,,,,,,3", "open,,,,,,3\nopen,,,,,,3", "Duplicate"),
        ("close,,,,,,2\n", "", "lifecycle"),
        ("sample,1,0,0", "sample,2,0,0", "identity"),
        ("sample,1,0,0", "sample,1,1,0", "identity"),
        ("sample,1,0,0", "sample,1,0,2", "identity"),
        ("sample,1,0,1,1,2,10", "sample,1,0,0,1,2,10", "Duplicate"),
        ("sample,1,0,1,1,2,10", "sample,1,0,1,1,2,11", "inconsistent"),
        ("sample,1,0,1,1,2,10\n", "", "ranks"),
        ("sample,1,0,0,0,1,10\nsample,1,0,1,1,2,10\n", "", "samples"),
        ("sample,1,0,1", "unknown,1,0,1", "Unknown"),
    ],
)
def test_invalid_native_reports_fail(
    layers, config, native_csv, tmp_path, old, new, message
):
    path = tmp_path / "samples.csv"
    path.write_text(native_csv.replace(old, new))
    with pytest.raises(RuntimeError, match=message):
        layers.parse_native(path, config)


def test_distances_compare_as_float32(layers):
    expected = [{"id": 7, "distance": 1.1}]
    layers.compare_hits([{"id": 7, "distance": layers.bench.f32(1.1)}], expected)


@pytest.mark.parametrize("distance", [True, "1", -1, math.nan, math.inf, 1.100001])
def test_invalid_or_changed_distances_fail(layers, distance):
    with pytest.raises(RuntimeError, match="distance"):
        layers.compare_hits(
            [{"id": 7, "distance": distance}], [{"id": 7, "distance": 1.1}]
        )


def test_duplicate_and_changed_ranked_ids_fail(layers):
    expected = [{"id": 7, "distance": 1}, {"id": 128, "distance": 2}]
    with pytest.raises(RuntimeError, match="Duplicate"):
        layers.compare_hits(
            [{"id": 7, "distance": 1}, {"id": 7, "distance": 2}], expected
        )
    with pytest.raises(RuntimeError, match="ID mismatch"):
        layers.compare_hits(list(reversed(expected)), expected)


@pytest.fixture
def query_runner(tmp_path):
    class Runner:
        def __init__(self):
            self.results = []
            self.calls = []

        def run(self, name, statements, setup=()):
            self.calls.append((name, statements, setup))
            directory = tmp_path / name
            directory.mkdir()
            for (label, _), result in zip(statements, self.results):
                (directory / f"{label}.result.json").write_text(json.dumps(result))
            return directory

    return Runner()


def test_sql_argument_rounding_is_qualified_without_weakening_parity(
    layers, query_runner
):
    original = -1.975404143333435
    actual = -1.9754042625427246
    query_runner.results = [[{"vector": [actual]}]]
    qualified = layers.qualify_queries(query_runner, [[original]], 1)
    assert qualified == [[actual]]
    _, statements, setup = query_runner.calls[0]
    args = SimpleNamespace(execution_mode="prepared", dimension=1, k=1)
    _, sql_statements, _ = layers.bench.search_workload(
        "ann", args, [[original]], "unused", "index.json", 1
    )
    assert statements[0][1] == sql_statements[0][1].replace(
        "measured_query", "qualify_query_input"
    )
    assert "x::DOUBLE" in setup[0]
    with pytest.raises(RuntimeError, match="distance"):
        layers.compare_hits(
            [{"id": 7, "distance": abs(original)}],
            [{"id": 7, "distance": abs(actual)}],
        )


@pytest.mark.parametrize(
    "result,message",
    [
        ([], "result"),
        ([{"other": [1.0]}], "result"),
        ([{"vector": [1.0]}, {"vector": [1.0]}], "result"),
        ([{"vector": []}], "Float32"),
        ([{"vector": [True]}], "Float32"),
        ([{"vector": [math.nan]}], "Float32"),
        ([{"vector": [1.1]}], "Float32"),
    ],
)
def test_invalid_qualified_sql_vectors_fail(layers, query_runner, result, message):
    query_runner.results = [result]
    with pytest.raises(RuntimeError, match=message):
        layers.qualify_queries(query_runner, [[1.0]], 1)


@pytest.mark.parametrize("changed", [False, True])
def test_candidate_sql_artifact_change_requires_explicit_qualification(
    layers, tmp_path, changed
):
    binary = tmp_path / "duckdb"
    binary.write_bytes(b"candidate")
    actual_hash = layers.bench.fingerprint(binary)
    fixture = {
        "source": {
            "artifacts": {
                "duckdb_sha256": "baseline" if changed else actual_hash,
                "extension_sha256": None,
            }
        }
    }
    cli = SimpleNamespace(duckdb=binary, extension=None, qualify_sql_candidate=False)
    if changed:
        with pytest.raises(RuntimeError, match="explicitly"):
            layers.qualify_sql_artifacts(cli, fixture)
    else:
        assert layers.qualify_sql_artifacts(cli, fixture) == (actual_hash, None, False)
    cli.qualify_sql_candidate = True
    assert layers.qualify_sql_artifacts(cli, fixture) == (actual_hash, None, changed)


@pytest.fixture
def mapped_fixture():
    return {
        "mapping": [(9, 1), (7, 0)],
        "addresses": {(9, 1): 128, (7, 0): 7},
        "expected": {"ann": [[{"id": 128, "distance": 1}, {"id": 7, "distance": 2}]]},
    }


@pytest.mark.parametrize("native", [True, False])
def test_native_ids_are_not_sql_ids(layers, config, mapped_fixture, native):
    hits = (
        [{"native_id": 0, "distance": 1}, {"native_id": 1, "distance": 2}]
        if native
        else [
            {"row": {"file_id": 9, "row_offset": 1}, "distance": 1},
            {"row": {"file_id": 7, "row_offset": 0}, "distance": 2},
        ]
    )
    samples = [
        {
            "round": round_id,
            "query": 0,
            "warmup": round_id == 0,
            "latency_ms": 10,
            "hits": hits,
        }
        for round_id in range(2)
    ]
    layers.validate_samples({"samples": samples}, mapped_fixture, config, native)
    assert all(sample["ranked_ids"] == [128, 7] for sample in samples)


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("round", True, "identity"),
        ("query", 1, "identity"),
        ("warmup", 1, "warmup"),
        ("latency_ms", 0, "timing"),
        ("latency_ms", math.inf, "timing"),
    ],
)
def test_invalid_sample_metadata_fail(
    layers, config, mapped_fixture, field, value, message
):
    sample = {
        "round": 0,
        "query": 0,
        "warmup": True,
        "latency_ms": 10,
        "hits": [{"native_id": 0, "distance": 1}, {"native_id": 1, "distance": 2}],
    }
    sample[field] = value
    with pytest.raises(RuntimeError, match=message):
        layers.validate_samples(
            {"samples": [sample]}, mapped_fixture, config, native=True
        )


def test_duplicate_missing_and_invalid_native_ids_fail(layers, config, mapped_fixture):
    sample = {
        "round": 0,
        "query": 0,
        "warmup": True,
        "latency_ms": 10,
        "hits": [{"native_id": 0, "distance": 1}, {"native_id": 1, "distance": 2}],
    }
    with pytest.raises(RuntimeError, match="Duplicate samples"):
        layers.validate_samples(
            {"samples": [sample, copy.deepcopy(sample)]},
            mapped_fixture,
            config,
            native=True,
        )
    with pytest.raises(RuntimeError, match="Missing samples"):
        layers.validate_samples(
            {"samples": [sample]}, mapped_fixture, config, native=True
        )
    sample["hits"][0]["native_id"] = 2
    with pytest.raises(RuntimeError, match="out of bounds"):
        layers.validate_samples(
            {"samples": [sample]}, mapped_fixture, config, native=True
        )


@pytest.fixture
def source_report(layers, tmp_path):
    bench = layers.bench
    _, queries = bench.generate(tmp_path, 64, 2, 1, 1234, 4)
    entries = []
    for file_id, name in ((5, "even"), (9, "odd")):
        path = tmp_path / f"{name}.vortex"
        path.write_bytes(b"fixture")
        entries.append(
            {
                "id": file_id,
                "uri": str(path),
                "version": f"sha256:{bench.fingerprint(path)}",
                "row_count": 32,
            }
        )
    snapshot = {
        "dataset_id": "benchmark",
        "version": "snapshot-1",
        "schema_fingerprint": "schema-1",
        "files": entries,
    }
    generation = tmp_path / "generation-1"
    artifacts = generation / "artifacts/spfresh"
    artifacts.mkdir(parents=True)
    bundle = {
        "revision": layers.REVISION,
        "metric": "squared_l2",
        "value_type": "float32",
        "snapshot": snapshot,
        "bundle": {"dimension": 2, "rows": 64, "posting_page_limit": 12},
    }
    bench.write_json(artifacts / "bundle.json", bundle)
    mapping = [(entry["id"], offset) for entry in entries for offset in range(32)]
    (artifacts / "rows.bin").write_bytes(
        b"VXSFROW1"
        + struct.pack("<Q", 64)
        + b"".join(struct.pack("<QQ", *row) for row in mapping)
    )

    def identity(path, name):
        return {
            "path": name,
            "size": path.stat().st_size,
            "checksum": f"sha256:{bench.fingerprint(path)}",
        }

    manifest = {
        "generation": "generation-1",
        "backend": "spfresh.static",
        "backend_version": 1,
        "snapshot": snapshot,
        "artifacts": [
            identity(artifacts / name, f"spfresh/{name}")
            for name in ("bundle.json", "rows.bin")
        ],
    }
    bench.write_json(generation / "manifest.json", manifest)
    reference = {
        "format_version": 1,
        "snapshot": snapshot,
        "generation": {
            "generation": "generation-1",
            "manifest": identity(generation / "manifest.json", "manifest.json"),
        },
    }
    bench.write_json(tmp_path / "index.json", reference)
    with (tmp_path / "even.csv").open() as stream:
        first = next(csv.DictReader(stream))
    vector = json.loads(first["embedding"])
    hits = [
        {
            "id": 0,
            "label": "row-0",
            "embedding": vector,
            "distance": math.fsum((a - b) ** 2 for a, b in zip(vector, queries[0])),
        }
    ]
    for engine in ("ann", "exact"):
        directory = tmp_path / engine
        directory.mkdir()
        bench.write_json(directory / "round-0-query-0.result.json", hits)
    report = {
        "status": "ok",
        "parameters": {
            "execution_mode": "prepared",
            "threads": 1,
            "warmup_rounds": 1,
            "rounds": 3,
            "queries": 1,
            "k": 1,
            "dimension": 2,
            "rows": 64,
            "posting_page_limit": 12,
        },
        "inputs": {
            name: {
                "bytes": (tmp_path / name).stat().st_size,
                "sha256": bench.fingerprint(tmp_path / name),
            }
            for name in (
                "even.csv",
                "odd.csv",
                "even.vortex",
                "odd.vortex",
                "queries.json",
            )
        },
    }
    path = tmp_path / "summary.json"
    bench.write_json(path, report)
    return path


def test_frozen_fixture_loads_and_rechecks(layers, source_report):
    fixture = layers.load_fixture(source_report)
    assert len(fixture["mapping"]) == 64
    assert fixture["addresses"][5, 1] == 2
    assert fixture["addresses"][9, 1] == 3
    layers.verify_fixture(fixture)


@pytest.mark.parametrize(
    "filename,message",
    [
        ("even.vortex", "Input content changed"),
        ("index.json", "reference changed"),
        ("generation-1/artifacts/spfresh/rows.bin", "Artifact content changed"),
    ],
)
def test_equal_length_mutations_fail(layers, source_report, filename, message):
    fixture = layers.load_fixture(source_report)
    path = source_report.parent / filename
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    with pytest.raises(RuntimeError, match=message):
        layers.verify_fixture(fixture)


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("execution_mode", "ad-hoc", "prepared"),
        ("threads", 2, "one thread"),
        ("rounds", 0, "round counts"),
        ("queries", 257, "query count"),
    ],
)
def test_incompatible_source_configuration_fails(
    layers, source_report, field, value, message
):
    report = json.loads(source_report.read_text())
    report["parameters"][field] = value
    layers.bench.write_json(source_report, report)
    with pytest.raises(RuntimeError, match=message):
        layers.load_fixture(source_report)

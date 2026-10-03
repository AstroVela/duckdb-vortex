# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import importlib
import json
import struct
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def sift(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("bench_index_sift")


def test_vector_record_values_are_preserved(sift, tmp_path):
    path = tmp_path / "vectors.fvecs"
    path.write_bytes(struct.pack("<IffIff", 2, 1.25, -2.5, 2, 3, 4))
    assert sift.read_records(path, 2, "<f4").tolist() == [[1.25, -2.5], [3, 4]]


@pytest.mark.parametrize(
    "data,message",
    [
        (struct.pack("<Iff", 3, 1, 2), "dimension"),
        (struct.pack("<Iff", 2, 1, 2) + b"x", "Truncated"),
        (b"", "empty"),
    ],
)
def test_invalid_vector_records_fail(sift, tmp_path, data, message):
    path = tmp_path / "vectors.fvecs"
    path.write_bytes(data)
    with pytest.raises((RuntimeError, ValueError), match=message):
        sift.read_records(path, 2, "<f4")


def test_original_groundtruth_not_index_parity_is_used_for_recall(sift):
    expected = [[{"id": 7}, {"id": 8}], [{"id": 9}, {"id": 10}]]
    score, per_query = sift.recall(expected, [[7, 20], [9, 10]])
    assert score == 0.75
    assert per_query == [0.5, 1.0]
    with pytest.raises(RuntimeError, match="count"):
        sift.recall(expected, [[7, 20]])


@pytest.mark.parametrize("native", [True, False])
def test_row_mapping_is_not_assumed_to_equal_native_id(sift, native):
    base = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    fixture = {"mapped_ids": [1, 0], "prefixes": {9: (0, 1), 7: (1, 1)}}
    hits = (
        [{"native_id": 0, "distance": 0}, {"native_id": 1, "distance": 8}]
        if native
        else [
            {"row": {"file_id": 7, "row_offset": 0}, "distance": 0},
            {"row": {"file_id": 9, "row_offset": 0}, "distance": 8},
        ]
    )
    samples = [{"query": 0, "hits": hits}]
    sift.translate_samples(samples, fixture, base, [[3, 4]], 2, native)
    assert samples[0]["ranked_hits"] == [
        {"id": 1, "distance": 0},
        {"id": 0, "distance": 8},
    ]


def test_distances_are_checked_against_original_vectors(sift):
    samples = [{"query": 0, "hits": [{"native_id": 0, "distance": 100}]}]
    with pytest.raises(RuntimeError, match="original"):
        sift.translate_samples(
            samples,
            {"mapped_ids": [0]},
            np.array([[1, 2]], dtype=np.float32),
            [[1, 2]],
            1,
            True,
        )


def test_missing_or_changing_repeat_results_fail(sift):
    samples = [
        {"query": 0, "ranked_hits": [{"id": 7, "distance": 1}]},
        {"query": 0, "ranked_hits": [{"id": 8, "distance": 1}]},
    ]
    with pytest.raises(RuntimeError, match="ID mismatch"):
        sift.expected_results(samples, 1)
    with pytest.raises(RuntimeError, match="Missing"):
        sift.expected_results(samples[:1], 2)


@pytest.mark.parametrize("row_id", [0, "0"])
def test_sql_unsigned_json_ids_are_checked_against_original_rows(sift, row_id):
    hits = [{"id": row_id, "label": "row-0", "embedding": [1.0, 2.0], "distance": 0.0}]
    sift.validate_sql_hits(hits, [1.0, 2.0], np.array([[1, 2]], dtype=np.float32), 1)
    assert hits[0]["id"] == 0


@pytest.mark.parametrize("row_id", ["+0", "00", "0 ", "-1", "0.0", True])
def test_invalid_sql_json_ids_fail(sift, row_id):
    with pytest.raises(RuntimeError, match="JSON row ID"):
        sift.validate_sql_hits(
            [{"id": row_id}], [1.0, 2.0], np.array([[1, 2]], dtype=np.float32), 1
        )


def test_sql_returned_vectors_must_match_original_even_if_distance_matches(sift):
    hits = [{"id": 0, "label": "row-0", "embedding": [0.0, 0.0], "distance": 5.0}]
    with pytest.raises(RuntimeError, match="original SIFT vectors"):
        sift.validate_sql_hits(
            hits, [1.0, 2.0], np.array([[1, 2]], dtype=np.float32), 1
        )


@pytest.mark.parametrize("mode", ["strict", "snapshot"])
def test_sql_options_match_native_and_provider(sift, mode):
    options = {"max_check": 32768, "internal_results": 128, "search_pages": 128}
    args = SimpleNamespace(
        validation_mode=mode,
        execution_mode="prepared",
        dimension=128,
        k=10,
        backend_options=options,
    )
    setup, statements, _ = sift.bench.search_workload(
        "ann", args, [[0.0] * 128], "inventory", "index.json", 1
    )
    assert "backend_options := '" + json.dumps(options) + "'" in setup[0]
    assert ("validation_mode := 'snapshot'" in setup[0]) == (mode == "snapshot")
    assert statements[0][1].startswith("EXECUTE measured_query(")


def test_changed_input_checksum_fails_before_conversion(sift, tmp_path):
    path = tmp_path / "base.fvecs"
    path.write_bytes(b"wrong")
    entry = {
        "name": "sift100k",
        "rows": 100_000,
        "dimension": 128,
        "files": {"base": {"path": str(path), "bytes": 5, "sha256": "old"}},
    }
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([entry]))
    with pytest.raises(RuntimeError, match="checksum"):
        sift.load_inputs(manifest, "sift100k", 1000)


@pytest.mark.parametrize("mode", ["strict", "snapshot"])
def test_benchmark_cache_candidate_never_changes_default_strict_shell(sift, mode):
    cli = SimpleNamespace(
        snapshot_duckdb=Path("candidate"),
        snapshot_cache_budget_mib=1024,
        duckdb=Path("default"),
        extension=Path("default-extension"),
        max_check=32768,
    )
    args, budget = sift.sql_arguments(cli, {"pages": 128}, mode, 64)
    assert args.duckdb == Path("candidate" if mode == "snapshot" else "default")
    assert args.extension == (None if mode == "snapshot" else cli.extension)
    assert budget == (1024 if mode == "snapshot" else 256) * 1024 * 1024


def test_snapshot_candidate_requires_explicit_budget(sift):
    with pytest.raises(RuntimeError, match="explicit compiled cache budget"):
        sift.main(
            [
                "--dataset",
                "sift1m",
                "--input-manifest",
                "inputs.json",
                "--duckdb",
                "default",
                "--native",
                "native",
                "--provider",
                "provider",
                "--fixture-builder",
                "builder",
                "--output-dir",
                "output",
                "--snapshot-duckdb",
                "candidate",
            ]
        )


@pytest.mark.parametrize("expected_failure", [True, False])
def test_default_snapshot_budget_control_requires_the_actual_budget_error(
    sift, tmp_path, monkeypatch, expected_failure
):
    def fail(root, cli, fixture, queries, mode, probes, expected, base):
        assert cli.snapshot_duckdb is None
        assert mode == "snapshot" and len(queries) == len(expected) == 1
        directory = root / "sql-snapshot"
        directory.mkdir()
        (directory / "stderr.log").write_text(
            sift.SNAPSHOT_BUDGET_ERROR if expected_failure else "unrelated error"
        )
        (directory / "peak-rss-kib.txt").write_text(
            "Command exited with non-zero status 1\n1024\n"
        )
        raise RuntimeError("SQL failed")

    monkeypatch.setattr(sift, "run_sql", fail)
    arguments = (
        tmp_path,
        SimpleNamespace(snapshot_duckdb=Path("candidate")),
        {"artifact_bytes": sift.DEFAULT_SQL_ARTIFACT_BUDGET + 1},
        [[1], [2]],
        64,
        [[{"id": 0}], [{"id": 1}]],
        np.array([[1], [2]]),
    )
    if expected_failure:
        result = sift.snapshot_budget_control(*arguments)
        assert result["status"] == "rejected_as_expected"
        assert result["peak_rss_mib"] == 1
        assert result["retained_artifact_budget_bytes"] == 256 * 1024 * 1024
    else:
        with pytest.raises(RuntimeError, match="Unexpected default snapshot failure"):
            sift.snapshot_budget_control(*arguments)

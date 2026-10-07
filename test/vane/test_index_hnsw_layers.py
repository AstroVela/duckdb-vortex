# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import copy
import importlib
import math
import sys
from pathlib import Path

import numpy as np
import pytest


@pytest.fixture
def hnsw(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts"))
    return importlib.import_module("bench_index_hnsw_layers")


@pytest.fixture
def ann():
    base = np.zeros((10, 128), dtype=np.float32)
    base[:, 0] = np.arange(10)
    queries = np.zeros((2, 128), dtype=np.float32)
    truth = np.tile(np.arange(10), (2, 1))
    samples = [
        {
            "round": round_id,
            "query": query_id,
            "warmup": round_id == 0,
            "latency_ms": 1.0,
            "hits": [{"id": row, "distance": float(row * row)} for row in range(10)],
        }
        for round_id in range(2)
        for query_id in range(2)
    ]
    return samples, base, queries, truth


def test_ann_complete_two_query_subset_and_parity(hnsw, ann):
    records, base, queries, truth = ann
    summary, samples, ranked = hnsw.validate_ann(records, base, queries, truth, 1, 1)
    assert summary["recall_at_10"] == 1
    assert len(samples) == 4
    assert len(ranked) == 2
    translated = copy.deepcopy(records)
    for record in translated:
        for hit in record["hits"]:
            hit["row"] = {
                "file_id": 11 if hit["id"] < 5 else 29,
                "row_offset": hit.pop("id") % 5,
            }
    assert (
        hnsw.validate_ann(
            translated, base, queries, truth, 1, 1, {11: (0, 5), 29: (5, 5)}, ranked
        )[2]
        == ranked
    )


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "reorder",
        "duplicate",
        "nan",
        "distance",
        "out-of-bounds",
        "duplicate-hit",
        "warmup",
    ],
)
def test_invalid_ann_results_rejected(hnsw, ann, damage):
    records, base, queries, truth = ann
    records = copy.deepcopy(records)
    if damage == "missing":
        records.pop()
    elif damage == "reorder":
        records[0], records[1] = records[1], records[0]
    elif damage == "duplicate":
        records[1] = copy.deepcopy(records[0])
    elif damage == "nan":
        records[0]["latency_ms"] = math.nan
    elif damage == "distance":
        records[0]["hits"][1]["distance"] = 0
    elif damage == "out-of-bounds":
        records[0]["hits"][1]["id"] = -1
    elif damage == "duplicate-hit":
        records[0]["hits"][1] = copy.deepcopy(records[0]["hits"][0])
    else:
        records[0]["warmup"] = False
    with pytest.raises(RuntimeError):
        hnsw.validate_ann(records, base, queries, truth, 1, 1)


@pytest.mark.parametrize(
    "ef,mode",
    [(9, "snapshot"), (1048577, "snapshot"), (64, "ignored"), (True, "snapshot")],
)
def test_sql_options_rejected(hnsw, ef, mode):
    with pytest.raises(RuntimeError, match="Invalid SQL"):
        hnsw.query_sql("/tmp/ref.json", ef, mode)


def test_sql_boundary_is_full_record_explicit_snapshot(hnsw):
    sql = hnsw.query_sql("/tmp/ref'quote.json", 136)
    assert "ref''quote" in sql
    assert "validation_mode := 'snapshot'" in sql
    assert '"ef": 136' in sql
    assert '"row".embedding::FLOAT[128]' in sql
    assert "$1::FLOAT[], 10" in sql
    assert "$1::FLOAT[128]" not in sql


def test_native_csv_requires_contiguous_ranks_and_equal_timing(hnsw, tmp_path):
    path = tmp_path / "samples.csv"
    path.write_text(
        "round,query,rank,native_id,distance,elapsed_ms\n0,0,0,1,0,1\n0,0,2,2,1,1\n"
    )
    with pytest.raises(RuntimeError, match="native rank"):
        list(hnsw.parse_native(path))


@pytest.mark.parametrize(
    "field,value",
    [
        ("format_version", True),
        ("format_version", 2),
        ("revision", "unqualified"),
        ("open_ms", -1),
        ("open_ms", math.inf),
        ("close_ms", math.nan),
    ],
)
def test_provider_report_requires_pinned_revision_and_finite_timing(hnsw, field, value):
    report = {
        "format_version": 1,
        "revision": hnsw.REVISION,
        "open_ms": 10.0,
        "close_ms": 0.0,
    }
    hnsw.validate_provider_report(report)
    report[field] = value
    with pytest.raises(RuntimeError, match="Provider"):
        hnsw.validate_provider_report(report)


@pytest.mark.parametrize("available,expected", [({8, 9, 26, 27}, 9), ({8, 10, 26}, 10)])
def test_controller_is_pinned_away_from_worker_before_preflight(
    hnsw, monkeypatch, available, expected
):
    calls = []
    monkeypatch.setattr(hnsw.os, "sched_getaffinity", lambda pid: available)
    monkeypatch.setattr(
        hnsw.os, "sched_setaffinity", lambda pid, cpus: calls.append((pid, cpus))
    )
    assert hnsw.pin_controller(8, 26) == expected
    assert calls == [(0, {expected})]


def test_controller_requires_a_separate_cpu(hnsw, monkeypatch):
    monkeypatch.setattr(hnsw.os, "sched_getaffinity", lambda pid: {8, 26})
    with pytest.raises(RuntimeError, match="No separate controller CPU"):
        hnsw.pin_controller(8, 26)


def test_main_isolates_controller_before_input_work(hnsw, monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(hnsw.os, "sched_getaffinity", lambda pid: {8, 9, 26})
    monkeypatch.setattr(
        hnsw.os, "sched_setaffinity", lambda pid, cpus: calls.append((pid, cpus))
    )
    monkeypatch.setattr(hnsw.os, "umask", lambda mask: None)

    def inputs(*args):
        assert calls == [(0, {9})]
        raise RuntimeError("reached input loading on isolated controller")

    monkeypatch.setattr(hnsw.sift, "load_inputs", inputs)
    arguments = ["benchmark", "--dataset", "sift100k", "--ef", "96"]
    for option in (
        "input-manifest",
        "reference",
        "native-index-record",
        "native-build-provenance",
        "provider",
        "sql-build-provenance",
        "output-dir",
    ):
        arguments.extend([f"--{option}", str(tmp_path / option)])
    monkeypatch.setattr(sys, "argv", arguments)
    with pytest.raises(RuntimeError, match="reached input loading"):
        hnsw.main()


@pytest.mark.parametrize(
    "compiler,global_busy,sibling_busy,expected",
    [
        (False, 5, 1, True),
        (True, 5, 1, False),
        (False, 10, 1, False),
        (False, 5, 5, False),
    ],
)
def test_host_qualification_records_excluded_windows(
    hnsw, compiler, global_busy, sibling_busy, expected
):
    runs = [
        {
            "layer": "sql",
            "repeat": 0,
            "resources": {
                "constraints": {
                    "snapshots": [
                        {"phase": "ready", "monotonic_seconds": 1},
                        {"phase": "after_close", "monotonic_seconds": 2},
                    ]
                }
            },
        }
    ]
    snapshots = [
        {
            "time": 1,
            "compiler_pids": [42] if compiler else [],
            "ticks": {"cpu": [0, 0, 0, 0, 0], "cpu26": [0, 0, 0, 0, 0]},
        },
        {
            "time": 2,
            "compiler_pids": [],
            "ticks": {
                "cpu": [global_busy, 0, 0, 100 - global_busy, 0],
                "cpu26": [sibling_busy, 0, 0, 100 - sibling_busy, 0],
            },
        },
    ]
    report = hnsw.host_qualification(runs, snapshots, 26)
    assert report["qualified"] is expected
    assert len(report["windows"]) == 1
    assert report["compiler_pids"] == ([42] if compiler else [])


def test_pooled_statistics_does_not_label_one_process_startup(hnsw, ann):
    records, base, queries, truth = ann
    _, samples, _ = hnsw.validate_ann(records, base, queries, truth, 1, 1)
    summary = hnsw.pooled_statistics(samples * 3)
    assert summary["samples"] == 6
    assert "first_query_in_fresh_process_ms" not in summary
    assert "round_p50_ms" not in summary

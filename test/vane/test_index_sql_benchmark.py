# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import csv
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "bench_index_sql", ROOT / "scripts" / "bench_index_sql.py"
)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def test_dataset_is_reproducible_and_queries_are_independent(tmp_path):
    roots = [tmp_path / str(i) for i in range(3)]
    for root in roots:
        root.mkdir()
    outputs = [
        bench.generate(root, 64, 8, 5, seed, 4)
        for root, seed in zip(roots, [1234, 1234, 5678])
    ]
    for index in range(2):
        assert outputs[0][0][index].read_bytes() == outputs[1][0][index].read_bytes()
    assert outputs[0][1] == outputs[1][1]
    assert outputs[0][1] != outputs[2][1]
    vectors = []
    ids = []
    for path in outputs[0][0]:
        with path.open() as stream:
            for row in csv.DictReader(stream):
                vectors.append(json.loads(row["embedding"]))
                ids.append(int(row["id"]))
    assert sorted(ids) == list(range(64))
    assert all(query not in vectors for query in outputs[0][1])
    assert all(value == bench.f32(value) for vector in vectors for value in vector)


def test_invalid_ranked_rows_and_distances_fail():
    hit = {"id": 0, "label": "row-0", "embedding": [2, 1], "distance": 1}
    assert bench.validate_hits([hit], [1, 1], 1, 64) == [(0, 1)]
    with pytest.raises(RuntimeError, match="duplicate"):
        bench.validate_hits([hit, hit], [1, 1], 2, 64)
    with pytest.raises(RuntimeError, match="distance"):
        bench.validate_hits([{**hit, "distance": 0}], [1, 1], 1, 64)
    with pytest.raises(RuntimeError, match="identity"):
        bench.validate_hits([{**hit, "label": "row-1"}], [1, 1], 1, 64)


def test_phase_events_require_complete_correlated_samples():
    event = {
        "event": "vortex_index_search_timing",
        "format_version": 1,
        "total_ms": 7,
        "phases": dict.fromkeys(bench.PHASES, 1),
    }
    stderr = "native log\n" + json.dumps(event)
    assert bench.parse_phases(stderr, 1) == [event]
    with pytest.raises(RuntimeError, match="one timing event"):
        bench.parse_phases(stderr, 2)
    with pytest.raises(RuntimeError, match="sum"):
        bench.parse_phases(json.dumps({**event, "total_ms": 8}), 1)
    with pytest.raises(RuntimeError, match="Incomplete"):
        bench.parse_phases(json.dumps({**event, "phases": {}}), 1)


@pytest.mark.parametrize(
    "arguments,message",
    [
        (["--rows", "1000000"], "256 MiB"),
        (["--posting-page-limit", "4096"], "posting buffer"),
    ],
)
def test_unsupported_budgets_fail_before_creating_output(tmp_path, arguments, message):
    with pytest.raises(RuntimeError, match=message):
        bench.main(
            [
                "--duckdb",
                "/missing/duckdb",
                "--output-dir",
                str(tmp_path / "output"),
                *arguments,
            ]
        )
    assert not (tmp_path / "output").exists()


def test_statistics_use_interpolated_percentiles_and_reject_nonfinite_values():
    summary = bench.summarize([1, 2, 3, 4])
    assert summary["samples"] == 4
    assert summary["p50_ms"] == 2.5
    assert summary["p95_ms"] == pytest.approx(3.85)
    with pytest.raises(RuntimeError, match="Invalid"):
        bench.summarize([float("nan")])

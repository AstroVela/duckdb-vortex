# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import csv
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "bench_index_sql.py"
assert SCRIPT.is_file(), f"Missing benchmark script: {SCRIPT}"
spec = importlib.util.spec_from_file_location("bench_index_sql", SCRIPT)
assert spec is not None and spec.loader is not None, f"Cannot load benchmark: {SCRIPT}"
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


@pytest.fixture
def timing_event():
    return {
        "event": "vortex_index_search_timing",
        "format_version": 1,
        "total_ms": 7,
        "phases": dict.fromkeys(bench.PHASES, 1),
    }


def test_phase_events_require_complete_correlated_samples(timing_event):
    event = timing_event
    stderr = "native log\n" + json.dumps(event)
    assert bench.parse_phases(stderr, 1) == [event]
    with pytest.raises(RuntimeError, match="one timing event"):
        bench.parse_phases(stderr, 2)
    with pytest.raises(RuntimeError, match="sum"):
        bench.parse_phases(json.dumps({**event, "total_ms": 8}), 1)
    with pytest.raises(RuntimeError, match="Incomplete"):
        bench.parse_phases(json.dumps({**event, "phases": {}}), 1)


@pytest.mark.parametrize(
    "mode,hit", [("strict", False), ("snapshot", False), ("snapshot", True)]
)
def test_snapshot_timing_metadata_is_preserved(timing_event, mode, hit):
    event = {**timing_event, "validation_mode": mode, "snapshot_cache_hit": hit}
    assert bench.parse_phases(json.dumps(event), 1) == [event]


@pytest.mark.parametrize(
    "fields,message",
    [
        ({"validation_mode": "unknown"}, "validation mode"),
        ({"validation_mode": None}, "validation mode"),
        ({"snapshot_cache_hit": True}, "snapshot cache"),
        ({"validation_mode": "strict", "snapshot_cache_hit": True}, "snapshot cache"),
        ({"validation_mode": "snapshot", "snapshot_cache_hit": 1}, "snapshot cache"),
    ],
)
def test_invalid_snapshot_timing_metadata_fails(timing_event, fields, message):
    with pytest.raises(RuntimeError, match=message):
        bench.parse_phases(json.dumps({**timing_event, **fields}), 1)


@pytest.mark.parametrize("mode", ["strict", "snapshot"])
@pytest.mark.parametrize("prepared", [False, True])
def test_snapshot_diagnostics_confirm_owner_scope(timing_event, mode, prepared):
    hit = mode == "snapshot" and prepared
    events = [
        {**timing_event, "validation_mode": mode, "snapshot_cache_hit": value}
        for value in [False, hit, hit]
    ]
    assert bench.snapshot_cache_status(events, mode, prepared) == {
        "samples": 3,
        "hits": 2 if hit else 0,
        "first_query_hit": False,
    }


@pytest.mark.parametrize(
    "modes,hits,prepared",
    [
        (["snapshot", "snapshot"], [True, True], True),
        (["snapshot", "snapshot"], [False, False], True),
        (["snapshot", "snapshot"], [False, True], False),
        (["strict", "snapshot"], [False, True], True),
        ([None, None], [False, True], True),
        (["snapshot", "snapshot"], [None, None], True),
    ],
)
def test_inconsistent_snapshot_diagnostics_fail(timing_event, modes, hits, prepared):
    events = [
        {**timing_event, "validation_mode": mode, "snapshot_cache_hit": hit}
        for mode, hit in zip(modes, hits)
    ]
    with pytest.raises(RuntimeError, match="snapshot"):
        bench.snapshot_cache_status(events, "snapshot", prepared)


def test_snapshot_metadata_remains_optional_only_for_old_strict_events(timing_event):
    assert bench.snapshot_cache_status([timing_event], "strict", True) is None
    with pytest.raises(RuntimeError, match="snapshot"):
        bench.snapshot_cache_status([timing_event], "snapshot", True)


def test_strict_diagnostics_reject_snapshot_events(timing_event):
    event = {
        **timing_event,
        "validation_mode": "snapshot",
        "snapshot_cache_hit": False,
    }
    with pytest.raises(RuntimeError, match="Strict"):
        bench.snapshot_cache_status([event], "strict", True)


@pytest.mark.parametrize("engine", ["ann", "exact"])
def test_snapshot_option_is_explicit_and_only_changes_ann(engine):
    args = SimpleNamespace(dimension=2, k=1, execution_mode="prepared")
    strict = bench.search_workload(engine, args, [[1.0, 2.0]], "files", "index.json", 2)
    args.validation_mode = "strict"
    assert (
        bench.search_workload(engine, args, [[1.0, 2.0]], "files", "index.json", 2)
        == strict
    )
    args.validation_mode = "snapshot"
    snapshot = bench.search_workload(
        engine, args, [[1.0, 2.0]], "files", "index.json", 2
    )
    assert snapshot[1:] == strict[1:]
    if engine == "ann":
        assert "validation_mode := 'snapshot'" in snapshot[0][0]
        assert snapshot[0] != strict[0]
    else:
        assert snapshot == strict


@pytest.mark.parametrize(
    "field,message",
    [
        ("phases", "phases must be an object"),
        ("total_ms", "Invalid total timing"),
    ],
)
def test_phase_events_reject_missing_fields(timing_event, field, message):
    del timing_event[field]
    with pytest.raises(RuntimeError, match=message):
        bench.parse_phases(json.dumps(timing_event), 1)


@pytest.mark.parametrize(
    "phases",
    [None, [], list(bench.PHASES), "phases", 7],
    ids=["null", "empty-array", "names-array", "string", "integer"],
)
def test_phase_events_require_a_phase_object(timing_event, phases):
    timing_event["phases"] = phases
    with pytest.raises(RuntimeError, match="phases must be an object"):
        bench.parse_phases(json.dumps(timing_event), 1)


@pytest.mark.parametrize(
    "field,message",
    [("phases", "Invalid phase timings"), ("total_ms", "Invalid total timing")],
)
@pytest.mark.parametrize(
    "value",
    [None, True, "1", [], {}, float("nan"), float("inf"), -1, 10**400],
    ids=[
        "null",
        "boolean",
        "string",
        "array",
        "object",
        "nan",
        "infinity",
        "negative",
        "overflowing-integer",
    ],
)
def test_phase_events_reject_invalid_timings(timing_event, field, value, message):
    if field == "phases":
        timing_event[field][bench.PHASES[0]] = value
    else:
        timing_event[field] = value
    with pytest.raises(RuntimeError, match=message):
        bench.parse_phases(json.dumps(timing_event), 1)


@pytest.mark.parametrize("version", [None, True, 1.0, "1", 2])
def test_phase_events_require_version_one_integer(timing_event, version):
    timing_event["format_version"] = version
    with pytest.raises(RuntimeError, match="Unsupported timing event format"):
        bench.parse_phases(json.dumps(timing_event), 1)


def test_phase_events_report_sum_overflow(timing_event):
    timing_event["phases"] = dict.fromkeys(bench.PHASES, 10**308)
    timing_event["total_ms"] = 10**308
    with pytest.raises(RuntimeError, match="sum"):
        bench.parse_phases(json.dumps(timing_event), 1)


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


@pytest.mark.parametrize(
    "pages,k,message",
    [
        (1024, 10, "Missing executable"),
        (1025, 10, "posting buffer"),
        (1008, 65, "Missing executable"),
        (1009, 65, "posting buffer"),
    ],
)
def test_posting_buffer_budget_boundaries(tmp_path, pages, k, message):
    with pytest.raises(RuntimeError, match=message):
        bench.main(
            [
                "--duckdb",
                "/missing/duckdb",
                "--output-dir",
                str(tmp_path / "output"),
                "--posting-page-limit",
                str(pages),
                "--k",
                str(k),
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


@pytest.mark.parametrize("engine", ["exact", "ann"])
@pytest.mark.parametrize("mode", ["ad-hoc", "prepared"])
def test_workload_prepares_one_owner_and_preserves_query_order(engine, mode):
    args = SimpleNamespace(dimension=2, k=1, execution_mode=mode)
    queries = [[1.0, 2.0], [3.0, 4.0]]
    setup, statements, labels = bench.search_workload(
        engine, args, queries, "['/data.vortex']", Path("/index.json"), 3
    )
    assert len(statements) == 6
    assert labels == [
        (f"round-{r}-query-{q}", r, q) for r in range(3) for q in range(2)
    ]
    assert [label for label, _ in statements] == [label for label, _, _ in labels]
    if mode == "prepared":
        assert len(setup) == 1
        assert setup[0].startswith("PREPARE measured_query AS SELECT")
        assert "$1::FLOAT[2]" in setup[0]
        assert all(sql.startswith("EXECUTE measured_query(") for _, sql in statements)
        assert statements[0][1] == statements[2][1] == statements[4][1]
    else:
        assert setup == []
        assert all(sql.startswith("SELECT") for _, sql in statements)
    assert "[1.0,2.0]::FLOAT[2]" in statements[0][1]
    assert "[3.0,4.0]::FLOAT[2]" in statements[1][1]


@pytest.mark.parametrize("value", [None, 0, "true", []])
def test_phase_events_reject_invalid_cache_status(timing_event, value):
    timing_event["provider_cache_hit"] = value
    with pytest.raises(RuntimeError, match="cache status"):
        bench.parse_phases(json.dumps(timing_event), 1)


@pytest.mark.parametrize("value", [False, True])
def test_phase_events_accept_boolean_cache_status(timing_event, value):
    timing_event["provider_cache_hit"] = value
    assert bench.parse_phases(json.dumps(timing_event), 1) == [timing_event]

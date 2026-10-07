# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import copy
import csv
import importlib
import json
import os
import struct
import subprocess
import tomllib
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def e2e(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("bench_index_sift_end_to_end")


@pytest.mark.parametrize("archive_path", ["absolute", "relative", "relative-decoy"])
def test_capi_build_hashes_the_archive_used_by_the_linker(
    e2e, tmp_path, monkeypatch, archive_path
):
    sdk = tmp_path / "sdk"
    archive = sdk / "lib/libvortex_duckdb.a"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"linked archive")
    caller = tmp_path / "caller"
    caller.mkdir()
    monkeypatch.chdir(caller)
    if archive_path == "relative-decoy":
        decoy = caller / "lib/libvortex_duckdb.a"
        decoy.parent.mkdir()
        decoy.write_bytes(b"unrelated archive")
    library = str(archive if archive_path == "absolute" else archive.relative_to(sdk))
    manifest = tmp_path / "link.json"
    manifest.write_text(
        json.dumps(["c++", "tools/shell/shell.o", library, "-o", "tools/shell/duckdb"])
    )
    commands = []

    def compile_or_link(command, *, check, cwd=None):
        assert check
        if cwd is not None:
            assert cwd.resolve() == sdk
            assert (cwd / library).read_bytes() == b"linked archive"
        Path(command[command.index("-o") + 1]).write_bytes(b"compiled output")
        commands.append(command)

    monkeypatch.setattr(e2e.subprocess, "run", compile_or_link)
    result = e2e.build_capi(
        manifest, Path("../sdk"), sdk / "include", tmp_path / "build", 256
    )
    assert len(commands) == 2
    assert result["archive_sha256"] == e2e.bench.fingerprint(archive)
    assert json.loads((tmp_path / "build/build.json").read_text()) == result


def records():
    base = np.arange(20, dtype=np.float32).reshape(10, 2)
    query = base[0].tolist()
    hits = [
        {
            "id": i,
            "embedding": row.tolist(),
            "distance": float(np.sum((row - base[0]) ** 2)),
        }
        for i, row in enumerate(base)
    ]
    samples = [
        {
            "round": i,
            "query": 0,
            "warmup": i == 0,
            "latency_ms": float(i + 1),
            "hits": copy.deepcopy(hits),
        }
        for i in range(4)
    ]
    samples.append({"type": "lifecycle", "open_ms": 100, "close_ms": 10})
    return base, [query], [list(range(10))], samples


def test_full_rounds_exclude_warmup_but_verify_every_record(e2e):
    base, queries, truth, samples = records()
    result, timings = e2e.summarize_records(iter(samples), base, queries, truth, 1, 3)
    assert result["p50_ms"] == 3
    assert result["round_p50_ms"] == [2, 3, 4]
    assert result["recall_at_10"] == 1
    assert result["full_records_verified"] == 40
    assert len(timings) == 4
    assert all("hits" not in sample for sample in timings)


@pytest.mark.parametrize(
    "mutation,message",
    [
        (lambda s: s.pop(1), "sample"),
        (lambda s: s.insert(1, copy.deepcopy(s[0])), "sample"),
        (lambda s: s.pop(), "Incomplete"),
        (lambda s: s[1].update(warmup=True), "sample"),
        (lambda s: s[1].update(latency_ms=float("nan")), "latency"),
        (lambda s: s[1]["hits"][0].update(embedding=[1, 2]), "vectors differ"),
        (lambda s: s[1]["hits"][0].update(distance=9), "distances"),
        (lambda s: s[1]["hits"][0].update(id=True), "IDs/count"),
        (lambda s: s[1]["hits"].pop(), "IDs/count"),
    ],
)
def test_invalid_or_incomplete_measurements_fail(e2e, mutation, message):
    base, queries, truth, samples = records()
    mutation(samples)
    with pytest.raises(RuntimeError, match=message):
        e2e.summarize_records(iter(samples), base, queries, truth, 1, 3)


def test_repeat_parity_detects_changed_valid_ranking(e2e):
    base, queries, truth, samples = records()
    base = np.vstack([base, base[-1] + 2])
    samples[1]["hits"][-1] = {
        "id": 10,
        "embedding": base[10].tolist(),
        "distance": float(np.sum((base[10] - base[0]) ** 2)),
    }
    with pytest.raises(RuntimeError, match="ID mismatch"):
        e2e.summarize_records(iter(samples), base, queries, truth, 1, 3)


def test_sql_projection_matches_opendata_without_fixture_label(e2e):
    sql = e2e.query_sql("index'quoted.json", 512, 128)
    assert '"row".id::UBIGINT' in sql
    assert '"row".embedding::FLOAT[128]' in sql
    assert "label" not in sql
    assert "$1::FLOAT[128]::FLOAT[]" in sql
    assert "'index''quoted.json'" in sql
    assert "validation_mode := 'snapshot'" in sql
    assert '"internal_results": 512' in sql
    assert "validation_mode := 'strict'" in e2e.query_sql(
        "index.json", 64, 128, "strict"
    )


@pytest.mark.parametrize("explicit_nprobe", [True, False])
def test_configuration_reuses_exact_existing_store_and_original_inputs(
    e2e, tmp_path, explicit_nprobe
):
    original = tmp_path / "baseline.toml"
    original.write_text(
        '[data.storage]\ntype="SlateDb"\npath="sift1m"\n'
        '[data.storage.object_store]\ntype="Local"\npath="/existing/store"\n'
        '[[params.recall]]\ndataset="sift1m"\nnprobe="256"\n'
        'query_concurrency="1"\nblock_cache_bytes="1073741824"\n'
    )
    if not explicit_nprobe:
        original.write_text(original.read_text().replace('nprobe="256"\n', ""))
    entry = {
        "name": "sift1m",
        "files": {
            kind: {"path": f"/input/{kind}"}
            for kind in ("base", "queries", "groundtruth")
        },
    }
    generated, nprobe = e2e.opendata_config(original, entry, 1000)
    config = tomllib.loads(generated)
    assert nprobe == (256 if explicit_nprobe else None)
    assert ("nprobe" in config["params"]["recall"][0]) == explicit_nprobe
    assert config["data"]["storage"]["path"] == "sift1m/0"
    assert config["data"]["storage"]["object_store"]["path"] == "/existing/store"
    assert config["params"]["recall"][0]["query_file"] == "/input/queries"
    assert config["params"]["recall"][0]["phases"] == "WARM"
    assert "INGEST" not in generated
    assert tomllib.loads(original.read_text())["data"]["storage"]["path"] == "sift1m"
    control, _ = e2e.opendata_config(
        original, entry, 1000, tmp_path / "no-maintenance.json"
    )
    assert tomllib.loads(control)["data"]["storage"]["settings_path"] == str(
        tmp_path / "no-maintenance.json"
    )
    assert "settings_path" not in tomllib.loads(original.read_text())["data"]["storage"]
    calibrated, calibrated_nprobe = e2e.opendata_config(
        original,
        entry,
        1000,
        nprobe_override=384,
        store_path=tmp_path / "private-store",
    )
    assert calibrated_nprobe == 384
    assert tomllib.loads(calibrated)["data"]["storage"]["object_store"]["path"] == str(
        tmp_path / "private-store"
    )
    assert (
        int(tomllib.loads(calibrated)["params"]["recall"][0]["block_cache_bytes"])
        == 1024**3
    )


def test_capi_parser_checks_header_rank_and_lifecycle(e2e, tmp_path):
    path = tmp_path / "samples.csv"
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "event",
                "round",
                "query",
                "warmup",
                "latency_ms",
                "rank",
                "id",
                "distance",
                "v0",
                "v1",
            ]
        )
        writer.writerow(["open", "", "", "", "10", "", "", "", "", ""])
        writer.writerow(["prepare", "", "", "", "20", "", "", "", "", ""])
        writer.writerow(["sample", "0", "0", "1", "3", "0", "7", "8", "1", "2"])
        writer.writerow(["close", "", "", "", "5", "", "", "", "", ""])
    samples = list(e2e.parse_capi(path, 2))
    assert samples[0]["hits"] == [{"id": 7, "distance": 8, "embedding": [1, 2]}]
    assert samples[1] == {
        "type": "lifecycle",
        "open_ms": 10,
        "prepare_ms": 20,
        "close_ms": 5,
    }
    path.write_text(path.read_text().replace("sample,0,0,1,3,0,", "sample,0,0,1,3,1,"))
    with pytest.raises(RuntimeError, match="rank"):
        list(e2e.parse_capi(path, 2))


def test_recall_requires_close_quality_not_just_a_common_floor(e2e):
    modes = {"sql": {"recall_at_10": 0.994}, "opendata": {"recall_at_10": 0.991}}
    with pytest.raises(RuntimeError, match="Recall gap"):
        e2e.qualify_recall(modes, 0.99, 0.002)
    modes["opendata"]["recall_at_10"] = 0.992
    assert e2e.qualify_recall(modes, 0.99, 0.002)["qualified"]
    with pytest.raises(RuntimeError, match="quality floor"):
        e2e.qualify_recall(modes, 0.995, 0.002)


def test_cache_metrics_preserve_hit_miss_labels_and_exclude_warmup(e2e, tmp_path):
    path = tmp_path / "metrics.jsonl"
    hit = json.dumps(
        {
            "name": "slatedb.db_cache.access_count",
            "labels": {"entry_kind": "data_block", "result": "hit"},
        }
    )
    miss = json.dumps(
        {
            "name": "slatedb.db_cache.access_count",
            "labels": {"entry_kind": "data_block", "result": "miss"},
        }
    )
    observations = []
    for phase in e2e.resources.phases(1, 1):
        hits, misses = (
            (90, 10)
            if phase == "before_round_1"
            else (99, 11) if phase in ("after_round_1", "after_close") else (0, 0)
        )
        observations.append(
            {
                "type": "counter_snapshot",
                "format_version": 1,
                "phase": phase,
                "counters": {hit: hits, miss: misses},
            }
        )
    path.write_text("\n".join(map(json.dumps, observations)))
    result = e2e.cache_counters(path, 1, 1)
    assert result["measured_cache_access"]["data_block"] == {
        "hit": 9,
        "miss": 1,
        "hit_fraction": 0.9,
    }
    path.write_text(path.read_text().replace('"before_round_1"', '"wrong"'))
    with pytest.raises(RuntimeError, match="phases"):
        e2e.cache_counters(path, 1, 1)


@pytest.mark.parametrize(
    "phase",
    [
        "ready",
        "before_round_0",
        "before_first_search",
        "after_first_search",
        "after_round_0",
        "before_round_1",
        "after_round_1",
        "after_close",
    ],
)
@pytest.mark.parametrize("error", ["disappeared", "decreased"])
def test_cache_metrics_reject_lost_or_reset_counters(e2e, tmp_path, phase, error):
    keys = {
        outcome: json.dumps(
            {
                "name": "slatedb.db_cache.access_count",
                "labels": {"entry_kind": "data_block", "result": outcome},
            }
        )
        for outcome in ("hit", "miss")
    }
    snapshots = [
        {
            "type": "counter_snapshot",
            "format_version": 1,
            "phase": phase,
            "counters": {key: index + 1 for key in keys.values()},
        }
        for index, phase in enumerate(e2e.resources.phases(1, 1))
    ]
    for snapshot in snapshots:
        if snapshot["phase"] == phase:
            if error == "disappeared":
                del snapshot["counters"][keys["miss"]]
            else:
                snapshot["counters"][keys["miss"]] = 0
    path = tmp_path / "metrics.jsonl"
    path.write_text("\n".join(map(json.dumps, snapshots)))
    with pytest.raises(RuntimeError, match=f"counter.*{error}"):
        e2e.cache_counters(path, 1, 1)


@pytest.mark.parametrize("rounds", [1, 3])
@pytest.mark.parametrize("error", ["disappeared", "decreased"])
def test_cache_metrics_reject_counter_loss_between_rounds(e2e, tmp_path, rounds, error):
    keys = {
        outcome: json.dumps(
            {
                "name": "slatedb.db_cache.access_count",
                "labels": {"entry_kind": "data_block", "result": outcome},
            }
        )
        for outcome in ("hit", "miss")
    }
    phases = e2e.resources.phases(1, rounds)
    boundary = phases.index("before_round_1")
    snapshots = []
    for index, phase in enumerate(phases):
        counters = {keys["hit"]: index * 9, keys["miss"]: index}
        if index >= boundary:
            if error == "disappeared":
                del counters[keys["miss"]]
            else:
                counters[keys["miss"]] -= boundary
        snapshots.append(
            {
                "type": "counter_snapshot",
                "format_version": 1,
                "recording": True,
                "phase": phase,
                "counters": counters,
            }
        )
    path = tmp_path / "metrics.jsonl"
    path.write_text("\n".join(map(json.dumps, snapshots)))
    with pytest.raises(RuntimeError, match=f"counter.*{error}"):
        e2e.cache_counters(path, 1, rounds)


def test_cache_metrics_allow_counters_registered_during_a_round(e2e, tmp_path):
    key = json.dumps(
        {
            "name": "slatedb.db_cache.access_count",
            "labels": {"entry_kind": "data_block", "result": "hit"},
        }
    )
    snapshots = [
        {
            "type": "counter_snapshot",
            "format_version": 1,
            "phase": phase,
            "counters": {key: 5} if phase in ("after_round_1", "after_close") else {},
        }
        for phase in e2e.resources.phases(1, 1)
    ]
    path = tmp_path / "metrics.jsonl"
    path.write_text("\n".join(map(json.dumps, snapshots)))
    result = e2e.cache_counters(path, 1, 1)
    assert result["measured_cache_access"]["data_block"] == {
        "hit": 5,
        "miss": 0,
        "hit_fraction": 1,
    }


def write_sql_cache_events(path, hits):
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "event": "vortex_index_search_timing",
                    "validation_mode": "snapshot",
                    "provider_cache_hit": hit,
                    "snapshot_cache_hit": hit,
                },
                separators=(",", ":"),
            )
            for hit in hits
        )
    )


def test_sql_cache_diagnostics_are_not_block_cache_hit_rates(e2e, tmp_path):
    path = tmp_path / "stderr.log"
    write_sql_cache_events(path, [False, True, True, True])
    result = e2e.sql_cache_hits(path, 2)
    assert result["provider_cache_hits"] == result["snapshot_cache_hits"] == 2
    assert result["initial_provider_cache_hit"] is False
    path.write_text(path.read_text().splitlines()[0])
    with pytest.raises(RuntimeError, match="diagnostics"):
        e2e.sql_cache_hits(path, 2)


@pytest.mark.parametrize("field", ["provider_cache_hit", "snapshot_cache_hit"])
@pytest.mark.parametrize(
    "hits",
    [
        [False, False, False, False],
        [True, True, True, True],
        [False, False, True, True],
        [False, True, False, True],
        [False, True, True, False],
        [0, True, True, True],
        [False, True, True, 1],
        [False, True, True, None],
    ],
)
def test_sql_cache_diagnostics_require_an_initial_miss_then_hits(
    e2e, tmp_path, field, hits
):
    path = tmp_path / "stderr.log"
    write_sql_cache_events(path, [False, True, True, True])
    events = [json.loads(line) for line in path.read_text().splitlines()]
    for event, hit in zip(events, hits):
        event[field] = hit
    path.write_text(
        "\n".join(json.dumps(event, separators=(",", ":")) for event in events)
    )
    with pytest.raises(RuntimeError, match="cache.*miss.*hits"):
        e2e.sql_cache_hits(path, 2)


@pytest.fixture
def resource_run(e2e, tmp_path, monkeypatch):
    entry = {
        "name": "sift100k",
        "files": {
            kind: {"path": f"/input/{kind}"}
            for kind in ("base", "queries", "groundtruth")
        },
    }
    original = tmp_path / "baseline.toml"
    original.write_text(
        '[data.storage]\ntype="SlateDb"\npath="sift100k"\n'
        '[data.storage.object_store]\ntype="Local"\npath="/existing/store"\n'
        '[[params.recall]]\ndataset="sift100k"\n'
        'query_concurrency="1"\nblock_cache_bytes="1073741824"\n'
    )
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    (fixture / "inputs.json").write_text(json.dumps(entry))
    capi = tmp_path / "capi"
    capi.mkdir()
    binary = capi / "worker"
    binary.write_bytes(b"test worker")
    monkeypatch.setattr(e2e.bench, "fingerprint", lambda path: "test-fingerprint")
    (capi / "build.json").write_text(
        json.dumps(
            {
                "sha256": e2e.bench.fingerprint(binary),
                "retained_artifact_budget_bytes": 1,
            }
        )
    )
    parity = tmp_path / "parity.json"
    parity.write_text(
        json.dumps(
            [
                {"query": i, "ranked_hits": [{"id": 7, "distance": 0}]}
                for i in range(1000)
            ]
        )
    )
    monkeypatch.setattr(
        e2e.sift,
        "load_inputs",
        lambda *args: (entry, np.zeros((1, 128)), [[0.0] * 128] * 2, [[], []]),
    )
    monkeypatch.setattr(
        e2e.sift,
        "load_fixture",
        lambda *args: {
            "reference": {"snapshot": {"files": ["source"]}},
            "artifact_bytes": 1,
            "pages": 12,
        },
    )
    calls = []

    def worker(root, name, binary, arguments, cwd, timeout, limits, threads, **flags):
        directory = root / name
        directory.mkdir()
        identity = None
        if name.startswith("opendata"):
            config = tomllib.loads(arguments(directory)[0].read_text())
            params = config["params"]["recall"][0]
            identity = {
                "nprobe": int(params.get("nprobe", 100)),
                "block_cache_bytes": 1024**3,
                "queries": 2,
                "warmup_rounds": limits["warmup"],
                "rounds": limits["rounds"],
                "rayon_threads": threads,
                "tokio_workers": threads,
                "storage_counters_enabled": flags.get("opendata_counters", False),
            }
        result = {"identity": identity, "recall_at_10": 1.0}
        suffix = "jsonl" if identity else "csv"
        (directory / f"samples.{suffix}").write_text(json.dumps(result))
        if flags.get("sql_timing"):
            write_sql_cache_events(directory / "stderr.log", [False, True, True, True])
        calls.append((name, limits, flags))
        return directory, {}

    monkeypatch.setattr(e2e, "run_worker", worker)
    monkeypatch.setattr(e2e, "parse_capi", lambda path: json.loads(path.read_text()))
    monkeypatch.setattr(
        e2e, "parse_opendata", lambda path: json.loads(path.read_text())
    )

    def summarize(records, base, queries, truth, warmup, rounds, expected=None):
        if expected is not None:
            assert expected == [[{"id": 7, "distance": 0}]] * len(queries)
        return records, []

    monkeypatch.setattr(e2e, "summarize_records", summarize)
    monkeypatch.setattr(e2e, "cache_counters", lambda *args: {})
    monkeypatch.setattr(e2e.subprocess, "check_output", lambda *args, **kw: "revision")
    opendata_root = tmp_path / "opendata"
    opendata_root.mkdir()
    cli = e2e.argparse.Namespace(
        command="run",
        queries=2,
        warmup_rounds=1,
        rounds=3,
        timeout=10,
        cpu=8,
        memory_mib=2048,
        minimum_recall=0.99,
        recall_tolerance=0.002,
        output_dir=tmp_path / "output",
        input_manifest=tmp_path / "inputs.json",
        dataset="sift100k",
        fixture=fixture,
        capi=binary,
        parity_samples=parity,
        opendata_maintenance="off",
        opendata_config=original,
        opendata_nprobe=None,
        probes=64,
        opendata=binary,
        opendata_root=opendata_root,
        order=["sql-snapshot-capi", "opendata"],
    )
    return cli, calls


@pytest.mark.parametrize("configured_nprobe", [None, 320])
@pytest.mark.parametrize("opendata_first", [False, True])
def test_resource_run_freezes_effective_nprobe_and_separates_recorders(
    e2e, resource_run, configured_nprobe, opendata_first
):
    cli, calls = resource_run
    if configured_nprobe is not None:
        cli.opendata_config.write_text(
            cli.opendata_config.read_text() + f'nprobe="{configured_nprobe}"\n'
        )
    if opendata_first:
        cli.order.reverse()
    report = e2e.run(cli)
    effective_nprobe = configured_nprobe or 100
    assert report["status"] == "ok"
    assert report["configuration"]["nprobe"] == effective_nprobe
    sources = report["binaries"]["opendata"]["benchmark_sources"]
    assert {"bencher/src/metrics.rs", "bencher/Cargo.toml"} <= sources.keys()
    assert [name for name, _, _ in calls] == cli.order + [
        "sql-cache-diagnostic",
        "opendata-cache-diagnostic",
    ]
    assert all(not flags.get("sql_timing") for _, _, flags in calls[:2])
    assert all(not flags.get("opendata_counters") for _, _, flags in calls[:2])
    assert calls[2][2]["sql_timing"]
    assert calls[3][2]["opendata_counters"]
    assert all(limits["rounds"] == 1 for _, limits, _ in calls[2:])
    diagnostic = tomllib.loads(
        (cli.output_dir / "opendata-diagnostic.toml").read_text()
    )
    assert int(diagnostic["params"]["recall"][0]["nprobe"]) == effective_nprobe


@pytest.mark.parametrize("parity_count", [2, 1000])
def test_run_accepts_matching_or_larger_parity_samples(e2e, resource_run, parity_count):
    cli, _ = resource_run
    samples = json.loads(cli.parity_samples.read_text())[:parity_count]
    cli.parity_samples.write_text(json.dumps(samples + samples))
    report = e2e.run(cli)
    assert report["status"] == "ok"
    assert report["queries"] == 2


@pytest.mark.parametrize("posting_view", [None, "0", "1"])
def test_report_records_posting_reader_mode(
    e2e, resource_run, monkeypatch, posting_view
):
    if posting_view is None:
        monkeypatch.delenv("VORTEX_SPFRESH_POSTING_VIEW", raising=False)
    else:
        monkeypatch.setenv("VORTEX_SPFRESH_POSTING_VIEW", posting_view)
    cli, _ = resource_run
    report = e2e.run(cli)
    assert report["configuration"]["spfresh_posting_view"] == (posting_view or "1")


@pytest.mark.parametrize("posting_view", ["", "true", "2"])
def test_invalid_posting_view_mode_fails_before_starting_workers(
    e2e, resource_run, monkeypatch, posting_view
):
    monkeypatch.setenv("VORTEX_SPFRESH_POSTING_VIEW", posting_view)
    cli, calls = resource_run
    with pytest.raises(RuntimeError, match="posting-view mode"):
        e2e.run(cli)
    assert not calls
    assert not cli.output_dir.exists()


@pytest.mark.parametrize("query_ids", [[], [0], [0, 2]])
def test_run_rejects_missing_parity_queries(e2e, resource_run, query_ids):
    cli, calls = resource_run
    samples = json.loads(cli.parity_samples.read_text())
    cli.parity_samples.write_text(json.dumps([samples[i] for i in query_ids]))
    with pytest.raises(RuntimeError, match="Missing query results"):
        e2e.run(cli)
    assert not calls


def test_run_rejects_changed_parity_repeats(e2e, resource_run):
    cli, calls = resource_run
    samples = json.loads(cli.parity_samples.read_text())[:2]
    samples.append({"query": 0, "ranked_hits": [{"id": 8, "distance": 0}]})
    cli.parity_samples.write_text(json.dumps(samples))
    with pytest.raises(RuntimeError, match="ID mismatch"):
        e2e.run(cli)
    assert not calls


@pytest.mark.parametrize("store_path", ["absolute", "relative", "relative-decoy"])
def test_resource_run_resolves_private_store_from_worker_directory(
    e2e, resource_run, tmp_path, monkeypatch, store_path
):
    cli, calls = resource_run
    store = cli.opendata_root / "data"
    store.mkdir()
    (store / "index").write_bytes(b"original store")
    caller = tmp_path / "caller"
    caller.mkdir()
    monkeypatch.chdir(caller)
    cli.opendata_root = Path("../opendata")
    if store_path == "relative-decoy":
        (caller / "data").mkdir()
        (caller / "data/index").write_bytes(b"unrelated store")
    configured = str(store) if store_path == "absolute" else "data"
    cli.opendata_config.write_text(
        cli.opendata_config.read_text().replace("/existing/store", configured)
    )
    report = e2e.run(cli)
    assert report["status"] == "ok"
    for name, _, flags in calls:
        if name.startswith("opendata"):
            source, target = flags["private_store"]
            assert source == store
            assert source.is_absolute()
            assert source.joinpath("index").read_bytes() == b"original store"
            assert target == cli.output_dir / name / "private-store"
    assert configured in cli.opendata_config.read_text()


@pytest.mark.parametrize("field", ["provider_cache_hit", "snapshot_cache_hit"])
def test_failed_sql_cache_diagnostic_marks_report_failed(
    e2e, resource_run, monkeypatch, field
):
    cli, _ = resource_run
    worker = e2e.run_worker

    def cold_worker(*args, **kwargs):
        directory, metadata = worker(*args, **kwargs)
        if kwargs.get("sql_timing"):
            path = directory / "stderr.log"
            path.write_text(
                path.read_text().replace(f'"{field}":true', f'"{field}":false')
            )
        return directory, metadata

    monkeypatch.setattr(e2e, "run_worker", cold_worker)
    monkeypatch.setattr(e2e.argparse.ArgumentParser, "parse_args", lambda *args: cli)
    with pytest.raises(RuntimeError, match="cache.*miss.*hits"):
        e2e.main([])
    assert (
        json.loads((cli.output_dir / "summary.json").read_text())["status"] == "failed"
    )


@pytest.fixture
def capi():
    path = os.environ.get("VORTEX_SIFT_CAPI")
    if not path:
        pytest.skip("Set VORTEX_SIFT_CAPI to run compiled C API protocol tests")
    return path


def run_capi(capi, tmp_path, sql, binary=None):
    query = tmp_path / "query.sql"
    query.write_text(sql)
    values = tmp_path / "queries.f32bin"
    values.write_bytes(
        binary if binary is not None else struct.pack("<II4f", 2, 2, 1, 2, 3, 4)
    )
    output = tmp_path / "samples.csv"
    return (
        subprocess.run(
            [capi, str(query), str(values), str(output), "1", "3", "1"],
            capture_output=True,
            text=True,
        ),
        output,
    )


def test_capi_float_array_parameter_and_owned_records(capi, e2e, tmp_path):
    process, output = run_capi(
        capi, tmp_path, "SELECT 0::UBIGINT, $1::FLOAT[2], 0::FLOAT"
    )
    assert process.returncode == 0, process.stderr
    samples = list(e2e.parse_capi(output, 2))
    assert len(samples) == 9
    for index, sample in enumerate(samples[:-1]):
        assert sample["query"] == index % 2
        assert sample["round"] == index // 2
        assert sample["hits"][0]["embedding"] == [[1, 2], [3, 4]][index % 2]


@pytest.mark.parametrize(
    "sql,binary,message",
    [
        ("SELECT 0::BIGINT, $1::FLOAT[2], 0::FLOAT", None, "Expected UBIGINT"),
        (
            "SELECT 0::UBIGINT, $1::FLOAT[2], 0::FLOAT WHERE false",
            None,
            "Missing returned",
        ),
        ("SELECT 0::UBIGINT, $1::FLOAT[2], NULL::FLOAT", None, "Null returned"),
        ("SELECT 0::UBIGINT, $1::FLOAT[2], 0::FLOAT", b"x", "Truncated"),
        (
            "SELECT 0::UBIGINT, $1::FLOAT[2], 0::FLOAT",
            struct.pack("<II2f", 1, 2, float("nan"), 2),
            "Nonfinite",
        ),
        (
            "SELECT 0::UBIGINT, $1::FLOAT[2], 0::FLOAT",
            struct.pack("<II2f", 1, 2, 1, 2) + b"x",
            "Trailing",
        ),
    ],
)
def test_capi_invalid_records_and_inputs_fail(capi, tmp_path, sql, binary, message):
    process, _ = run_capi(capi, tmp_path, sql, binary)
    assert process.returncode != 0
    assert message in process.stderr

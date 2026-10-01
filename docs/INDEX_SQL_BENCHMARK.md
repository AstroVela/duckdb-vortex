# Static index SQL benchmark

Build the index-enabled shell or matching loadable artifact as described in
[Static Index SQL](STATIC_INDEX_SQL.md). The benchmark uses only Python 3.11+'s
standard library and GNU `time`; run measurements sequentially on Linux x86_64.

```bash
python3 scripts/bench_index_sql.py \
  --duckdb build/index-sql-native/duckdb \
  --output-dir build/index-bench-100k-128
```

The defaults are 100,000 rows, 128 dimensions, 30 independent queries, k=10,
one complete warmup round and three measured rounds, with one DuckDB thread.
`OMP_NUM_THREADS=1` is set for each worker. `--extension /absolute/path/to/vortex.duckdb_extension`
uses a matching shell without built-in Vortex, with automatic installation and
loading disabled. Each output directory must be new.

Data consists of Float32 Gaussian mixtures with 64 shared centers and independent
random streams for centers, rows and queries. The seed defaults to 20261001.
Queries are independently generated, not self-neighbor lookups. Two interleaved
Vortex files contain the same vectors used by both engines. Exact search uses
DuckDB's `array_distance` and squared L2 with distance/ID ordering. Both paths
return IDs, labels, embeddings and distances. Results are checked for ranked
row identity, squared-L2 consistency, duplicate IDs and equality across repeats.
Recall is measured against exact top-k; a low recall is reported, not concealed
by an automatic parameter change.

`--head-count`, `--posting-page-limit` and `--replicas` configure static build
options. Defaults are 512, 256 and 4. The reference remains frozen throughout a
run, and search uses the backend's default query options. The existing 256 MiB
builder vector budget permits 100,000 x 128 but not 1,000,000 x 128. The latter
requires a separately reviewed factory-budget change; the script rejects it
before preparing data. Lower-dimensional larger runs can be requested explicitly.

## Measurements

Preparation, build, exact scan, ANN and diagnostics run in separate processes.
Within each search process, all queries and rounds use one connection. Exact
search runs before ANN; no benchmark workers run concurrently. The first query
in each fresh process is recorded separately. OS page caches are not evicted,
so this is not a cold-disk measurement.

Per-query latency comes from DuckDB JSON profiling of a materialized SELECT.
It excludes process startup and shell JSON rendering; profiling remains enabled
for both engines. Warmup rounds are excluded from p50, p95 and serial QPS.
QPS is the inverse mean query latency and is not concurrent/client throughput.
GNU `time` records peak process RSS, including engine startup, separately for
each worker. DuckDB buffer-memory profiling is not used as a substitute for RSS.
Build time comes from the profiled build statement. Index size includes the
published generation and reference, excluding benchmark logs and scratch files.

## Phase Diagnostics

The companion Vortex SQL adapter emits versioned JSON events on stderr when
`VORTEX_INDEX_TIMING=1` is set before process startup. The benchmark enables it
only in a separate diagnostic process and verifies one event per query plus
identical ranked results. Normal latency measurements run with it disabled.

| Field | Measured work |
| --- | --- |
| `reference_check_ms` | Execution-time reference identity check |
| `source_validation_ms` | Paths, source snapshot and sealed-store validation |
| `provider_open_ms` | Scratch/factory setup, artifact materialization and provider open |
| `native_search_ms` | Provider search, including native search and hit mapping |
| `hit_validation_ms` | Hit bounds, coverage, ordering and uniqueness |
| `take_ms` | Fetch and validate ranked original rows |
| `result_materialization_ms` | Result construction, handle cleanup and bounded FFI conversion |

These phases sum to the timed Rust execution. SQL binding, optimizer work,
DuckDB result consumption and shell output are outside that phase total.
`--skip-stage-timings` explicitly allows benchmarking an older artifact without
diagnostics; the report then records `stages: null`.

## Reports

`summary.json` records parameters, engine identity, binary/input hashes,
per-query recall, latency distributions, per-round medians, process RSS, build
time, index size and phase distributions. `samples.json` retains every latency,
including warmups and the first query. `stages.json` retains diagnostic events.
Each worker directory retains its SQL, raw profiling/result JSON and logs.
Keep these artifacts with comparisons so the data, options and executable can
be matched across runs.

## Initial Local Baseline

On 2026-10-01, the default 100,000 x 128 workload completed on Linux x86_64
with one DuckDB thread and 90 measured samples per engine. The local SDK shell
reported DuckDB v1.5.0, source ID `d8a9d61d59`, with Vortex revision
`c91c18b50a05452bea5ac8910b6d58633fbaddd7`. This is a synthetic local SDK
qualification, not a production-wheel or cold-cache result.

| Engine | p50 (ms) | p95 (ms) | Serial QPS | Peak process RSS (MiB) |
| --- | ---: | ---: | ---: | ---: |
| Exact Vortex scan | 54.88 | 59.17 | 18.00 | 137.42 |
| Static SPFresh SQL | 3137.24 | 3272.62 | 0.322 | 167.73 |

Recall@10 was 1.0 for all 30 independent queries, with identical results across
all rounds. Build time was 24.89 seconds, index size was 168.84 MiB, and peak
build RSS was 178.77 MiB. In the separate 30-query diagnostic run, source
validation and provider open together accounted for 98.55% of summed Rust
execution time; their individual medians were 1500.00 and 1587.49 ms. Provider
search, including native search and hit mapping, had a median of 25.19 ms.
These measurements identify the per-query validation/open path as the main
cost in this configuration; this benchmark does not change or cache that path.

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

`--execution-mode ad-hoc` is the default and preserves the original workload.
To measure repeated execution through one prepared owner per search worker, use:

```bash
python3 scripts/bench_index_sql.py \
  --duckdb /absolute/path/to/candidate/duckdb \
  --execution-mode prepared \
  --output-dir build/index-bench-prepared-100k-128
```

Both exact and ANN prepare one parameterized statement and execute the complete
query set through that owner. Vectors, query order, warmups, measured rounds,
and backend options are unchanged. PREPARE setup is not profiled as query latency.
An older artifact can run this mode without provider reuse, making it suitable
for before/after comparisons; use matching release builds and SDK/shell versions.

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
| `provider_open_ms` | Provider-cache lookup, or scratch/factory setup, artifact materialization and open on a miss |
| `native_search_ms` | Provider search, including native search and hit mapping |
| `hit_validation_ms` | Hit bounds, coverage, ordering and uniqueness |
| `take_ms` | Fetch and validate ranked original rows |
| `result_materialization_ms` | Result construction, handle cleanup and bounded FFI conversion |

These phases sum to the timed Rust execution. SQL binding, optimizer work,
DuckDB result consumption and shell output are outside that phase total.
`--skip-stage-timings` explicitly allows benchmarking an older artifact without
diagnostics; the report then records `stages: null`.

Prepared mode runs two complete diagnostic query-set rounds in one owner. A
reuse-capable artifact also emits boolean `provider_cache_hit`; a first miss
followed by hits demonstrates provider retention, not skipped file validation.
The report separates the first diagnostic query from the remaining queries and
summarizes hit counts. Older version-1 events without this optional field remain
accepted and report `diagnostic_provider_cache: null`.

## Reports

`summary.json` records parameters, engine identity, binary/input hashes,
per-query recall, latency distributions, per-round medians, process RSS, build
time, index size and phase distributions. `samples.json` retains every latency,
including warmups and the first query. `stages.json` retains diagnostic events;
`stage-samples.json` associates them with query/round identities and the first
query marker. `stages_first_query` and `stages_after_first_query` separate cold
provider open from later diagnostic calls. `diagnostic_provider_cache` records
observed hits, not an inferred speedup.
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
cost in that original ad-hoc configuration. They predate prepared-provider reuse;
the historical baseline above is unchanged.

## Prepared Reuse Comparison

On 2026-10-02, the same default 100,000 x 128 workload was run sequentially
before and after prepared-provider reuse, with 90 measured samples per engine
and 60 separate diagnostic queries per run. Both builds used Rust release mode,
the same DuckDB v1.5.0 SDK (`d8a9d61d59`) and shell objects, the same qualified
SPFresh native cache, and matching compiler flags. The baseline Vortex revision
was `7446bed09971b71b851387512999657860d15d1b`, tree-equivalent to the initial
baseline above; the candidate was the source tree committed as
`6438f93778192b8bc3e887efcc17737dbcb79349` on `feat/vortex-index-prepared-reuse`.
Input hashes, including the encoded Vortex files, and the benchmark
script hash matched. This remains a synthetic local SDK qualification.

| Mode | p50 (ms) | p95 (ms) | Serial QPS | Peak process RSS (MiB) |
| --- | ---: | ---: | ---: | ---: |
| Exact prepared scan, before | 55.65 | 57.84 | 18.15 | 136.88 |
| Exact prepared scan, candidate | 56.91 | 59.35 | 17.51 | 136.65 |
| ANN prepared, before | 3169.51 | 3252.45 | 0.318 | 168.48 |
| ANN prepared, candidate | 1415.93 | 1434.63 | 0.706 | 168.65 |

ANN median latency decreased by 55.33% (2.24x), with Recall@10 of 1.0 for all
30 independent queries and identical repeated results in both versions. The
candidate diagnostic observed one initial miss followed by 59 hits. Excluding
the first query, median provider-open/lookup time decreased from 1544.84 ms to
0.0134 ms. Source/store validation still ran on every call and remained dominant
at 1374.73 ms, versus 1461.47 ms before. Native-search medians were 25.39 and
24.18 ms. This change does not make ANN faster than the exact scan on this workload.

The first query in each fresh process took 2848.95 ms before and 3001.85 ms in
the candidate. Each is a single observation with uncontrolled OS page caches;
no first-call or cold-disk improvement is claimed. Candidate build time was
25.84 seconds and the index size was 168.84 MiB, with unchanged build/search
budgets. The retained artifact-byte limit is not an RSS bound.

Reports and raw samples are retained locally under
`build/index-bench-prepared-before-100k-128-20261002/` and
`build/index-bench-prepared-after-100k-128-20261002/`. Small candidate ad-hoc
qualification also observed zero cache hits, preserving the uncached path.

A separate native probe prepared two independent owners for this 168.84 MiB
generation. The second owner fell back to uncached opens while the first was
retained, then cached successfully after the first was deallocated. Ranked
results stayed equal and scratch was cleaned, exercising the 256 MiB aggregate
artifact-byte limit with real SPFresh handles.

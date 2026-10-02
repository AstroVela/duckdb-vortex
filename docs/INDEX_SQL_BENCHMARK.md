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

`--validation-mode strict` is the default. To measure the explicit immutable
snapshot contract on a snapshot-capable artifact, add `--validation-mode snapshot`
with `--execution-mode prepared`. The first execution still fully validates and
opens the generation; later executions retain the verified source and index.
This does not relax the default or measure live-file freshness. Ad-hoc statements
do not share a retained snapshot across independent queries. See
[Static Index SQL](STATIC_INDEX_SQL.md#explicit-snapshot-mode) for the contract.

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
| `source_validation_ms` | Paths, source snapshot and sealed-store validation in strict mode or on the first snapshot call; retained-source lookup on a snapshot hit |
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

Snapshot-capable events also include `validation_mode` and boolean
`snapshot_cache_hit`. Both benchmark drivers require an initial snapshot miss
followed by hits for prepared snapshot mode, and report them separately as
`diagnostic_snapshot_cache`. Ad-hoc snapshots must report only misses. These
fields remain optional for older strict artifacts.

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

## Checksum Backend Comparison

On 2026-10-02, a second comparison retained prepared-provider reuse and changed
only the complete-content SHA-256 implementation in `vortex-index` from
RustCrypto `sha2` to `ring`. The baseline was
`6438f93778192b8bc3e887efcc17737dbcb79349`; the candidate was that revision
plus local, uncommitted checksum changes, not a new published Vortex revision.
Both were release builds against the same DuckDB v1.5.0 SDK (`d8a9d61d59`),
shell objects, SPFresh native cache and compiler flags. Candidate qualification
used a temporary local-path manifest; the tracked outer dependency pins were
not changed. Registry package versions and checksums were unchanged.

This Linux x86_64 host has an Intel Xeon E5-2686 v4 with AVX2 but no SHA CPU
extension. Both adapters still validate complete file/artifact contents on every
search before provider-cache lookup. The digest format, schema fingerprints,
serialized references, generation formats, IO checks and memory budgets remain
unchanged. There is no timestamp-only validation or skipped checksum fast path.

The fresh default 100,000 x 128 runs used 30 independent queries, k=10, one
complete warmup and three measured rounds: 90 measured samples per engine and
60 separate diagnostic queries per version. Input hashes, including encoded
Vortex files, benchmark script hashes, options and SDK identities matched.

| Mode | p50 (ms) | p95 (ms) | Serial QPS | Peak process RSS (MiB) |
| --- | ---: | ---: | ---: | ---: |
| Exact prepared scan, fresh baseline | 57.66 | 59.74 | 17.131 | 140.46 |
| Exact prepared scan, checksum candidate | 56.91 | 84.32 | 16.821 | 137.74 |
| ANN prepared, fresh baseline | 1515.52 | 1591.25 | 0.654 | 167.89 |
| ANN prepared, checksum candidate | 847.36 | 894.47 | 1.171 | 169.38 |

ANN median latency decreased by 44.09% (1.79x). Its three candidate round
medians were 846.63, 849.65 and 847.14 ms. Recall@10 was 1.0 for all queries,
with identical repeated results. All 300 result files across exact, ANN and
diagnostic workers were also compared between versions, including IDs, labels,
returned embeddings and distances. Both diagnostic workers observed one
initial provider-cache miss followed by 59 hits.

Excluding the first diagnostic query, median source/store validation decreased
from 1540.48 to 814.72 ms. Native-search medians were 26.17 and 25.39 ms;
provider-cache lookup medians were 0.0146 and 0.0144 ms. The improvement is in
validation, not a change to the SPFresh search algorithm. ANN remains about
15x slower than the exact scan on this workload because complete validation
still scales with the total source and artifact bytes.

Host load differed between the full runs and the exact-scan p95 varied. A
separate alternating check used the same baseline-built generation and the
first five independent queries, in baseline/candidate/candidate/baseline order.
Each worker had one warmup and two measured rounds, yielding 20 measured
samples per version. Baseline and candidate medians were 1524.71 and 843.97 ms
(1.81x); all ranked rows matched the full baseline. This corroborates the
improvement without treating the environment as controlled or claiming a
portable CPU-independent speedup.

The candidate also reopened an old generation with identical ranked results,
and a small ad-hoc run observed zero provider-cache hits. Verification passed
119 index tests (including 54 new digest compatibility cases) and two index
doctests, 329 SQL adapter tests, default/file-only/local-store checks, formatting
and strict targeted Clippy. Built-in and loadable native qualification each
passed 66 processes, including 38 negative cases. No ASAN, production-wheel,
Ray or new remote-CI qualification was performed for this change.

Reports and raw samples are retained locally under
`build/index-bench-checksum-before-100k-128-20261002/`,
`build/index-bench-checksum-after-100k-128-20261002/` and
`build/index-bench-checksum-alternating-20261002/`, with a comparison record at
`build/index-bench-checksum-comparison-20261002.json`. These measurements are
synthetic local SDK qualification with uncontrolled OS page caches, not
cold-disk results. The historical prepared-reuse table above is unchanged.

## Native, Provider And SQL Comparison

`scripts/bench_index_layers.py` reuses the files and published generation from
a successful prepared-mode `bench_index_sql.py` report. It does not rebuild
the index separately for each layer. Build the opt-in `spfresh_benchmark` CMake
target and `bench_provider` Rust example in the matching Vortex checkout, as
described in `vortex-index-spfresh/README.md`. Use the same patched SPFresh
revision, registry dependency versions, Rust toolchain, release profile and
compiler flags as the SQL artifact. In particular, the Vortex workspace's
default release profile and frame-pointer flags differ from the outer extension's.

```bash
python3 scripts/bench_index_layers.py \
  --sql-report build/index-bench-prepared-100k-128/summary.json \
  --duckdb /absolute/path/to/matching/duckdb \
  --native /absolute/path/to/spfresh_benchmark \
  --provider /absolute/path/to/bench_provider \
  --output-dir build/index-bench-layers-100k-128
```

For a loadable source qualification, also supply its matching `--extension`.
The driver rejects a different shell/extension hash, non-prepared source runs,
multiple threads, changed inputs/references/manifests/artifacts, incompatible
native shapes or an invalid physical-row mapping. To explicitly qualify a new
SQL candidate on an existing frozen fixture, use `--qualify-sql-candidate`.
This permits the recorded SQL artifact hash to differ, but still requires the
same SDK identity, unchanged fixture hashes and complete original-row equality
against the source qualification. Without that flag, the artifact hash guard
remains unchanged. The report records both old and new artifact identities.
Add `--validation-mode snapshot` only for a snapshot-capable SQL candidate;
native and Provider workloads are unaffected by that choice.

Stop all writers for the
entire run. Each worker uses a fresh process and `OMP_NUM_THREADS=1`; workers run
sequentially, with the source report's query order, warmups and measured rounds.

| Mode | Per-query timed work | Open/validation policy |
| --- | --- | --- |
| `cpp-reset` | Direct SPANN search, including QueryResult allocation/copy/destruction and both workspace resets | Native open once; options set once |
| `cpp-reuse` | Direct SPANN search retaining workspace on one fixed handle/thread | Native open once; experimental lower bound |
| `bridge` | Current C ABI search, including per-call options and workspace resets | Native open once |
| `provider` | Public `VectorIndex::search`, including validation, FFI and physical-row mapping | Verify/materialize artifacts once at open |
| `sql-ann` | Prepared SQL, selected validation mode, Provider search, ranked original-row fetch and result consumption | Strict by default; snapshot explicitly retains fully verified source/index after the first execution |
| `sql-exact` | Prepared exact Vortex scan returning the same original-row columns | Control, not an ANN layer |

Native queries use C++ `steady_clock`; Provider queries use Rust `Instant`;
SQL uses the existing DuckDB materialized-SELECT profile. Query-result output
serialization is outside the timers. All calls contain one query: no batch
average is presented as single-query latency. Warmups are excluded from
percentiles/QPS; first search and open/close are reported separately. Peak RSS
comes from GNU `time` for each entire worker, not a per-handle memory bound.

The driver first qualifies the original SQL arguments through the same prepared
Float32 casts, extracting components as DOUBLE so their exact values survive
JSON. DuckDB's numeric-literal casts can differ by one Float32 ULP from the
Python input. Native and Provider receive the qualified values, while SQL keeps
the original literals; thus all layers search identical effective vectors.
The report records changed component counts and the packed-query hash rather
than relaxing distance comparisons. Dense native IDs are translated through
`rows.bin` and the source CSV inventories, not treated as SQL IDs.

Every native/Provider query must match the source ANN's ranked IDs and Float32
distances, and all SQL repeats must match the complete original-row results.
Recall is measured against the source exact top-k. A separate two-round SQL
diagnostic worker requires an initial Provider-cache miss followed by hits.
Full fixture/reference/artifact hashes are checked again at the end.
`summary.json` retains modes, clocks, options, binary/input hashes, per-round
medians, RSS, recall and diagnostics; worker CSV/JSON contains all raw samples.

This is a frozen-fixture lower-bound comparison, not an assertion that native
and SQL do equal work. Native/Provider do not repeat strict SQL's full-content
checks or fetch original rows. Their difference from SQL must not be labelled solely
as DuckDB engine overhead. The `cpp-reuse` experiment does not enable production
workspace reuse, whose multi-handle/thread lifetime contract remains unchanged.

### Local Layered Results

On 2026-10-02, the checksum candidate's existing 100,000 x 128 generation was
reused for this matrix: 30 independent queries, k=10, 512 heads, posting-page
limit 256, four replicas, max_check 4096, internal_results 64, one complete
warmup and three measured rounds. Each mode has 90 measured samples. The SQL
artifact remains `6438f93778192b8bc3e887efcc17737dbcb79349` plus the uncommitted
checksum changes above, built against DuckDB v1.5.0 (`d8a9d61d59`). The new
benchmark-only native accessor is absent from the production bridge.

Native measurements use the same qualified patched revision
`5893eb61ee3b18610b6b00f1939be7dae1af8904` and core/distance archives as SQL.
The benchmark executable was linked with GCC `-O3 -DNDEBUG`, OpenMP, and the
same zstd library. Provider and SQL release qualification used Rust 1.97.1,
codegen-units 16, lto=false and matching compiler flags. Provider qualification
used the temporary local-path manifest described above; registry versions and
checksums match the tracked outer lockfile, whose dependency pins are unchanged.
The direct C++ comparison is the patched static SPANN path, not vanilla/latest
upstream SPFresh, online updates or SPDK.

| Mode | p50 (ms) | p95 (ms) | Serial QPS | Peak process RSS (MiB) |
| --- | ---: | ---: | ---: | ---: |
| Direct C++, workspace reset | 22.15 | 24.82 | 45.72 | 49.57 |
| Direct C++, workspace reuse | 7.88 | 9.03 | 127.23 | 76.41 |
| Current C bridge | 21.71 | 25.23 | 46.05 | 49.62 |
| Rust Provider, no SQL | 21.82 | 24.80 | 45.90 | 51.98 |
| Prepared SQL ANN | 765.14 | 770.64 | 1.307 | 169.61 |
| Prepared SQL exact scan | 52.43 | 53.79 | 19.05 | 141.25 |

Provider open, including store verification and artifact materialization, took
1725.07 ms; its first search took 24.62 ms. Native open took about 2.0 ms but
does not do the Provider's store verification/copying. First searches with and
without workspace reuse took 26.87 and 32.35 ms; the SQL first query took
1801.15 ms. These are separate single observations, not cold-disk comparisons.
The three round medians were 7.99/7.85/7.81 ms for direct reuse,
21.95/21.69/21.87 ms for Provider, and 764.81/765.14/766.10 ms for SQL ANN.

Recall@10 was 1.0 for all queries. Ranked IDs and exact Float32 distances
matched across all five ANN modes, and complete SQL original-row results
matched the source qualification across all repeats and diagnostics. Input
qualification observed 160 changed Float32 components out of 3840; all layers
used the same effective SQL values. Source, reference, manifest and artifact
content hashes matched before and after the run.

The separate diagnostic worker observed one cache miss followed by 59 hits.
Excluding the first query, median source/store validation was 732.06 ms,
Provider lookup 0.0131 ms, Provider/native search 23.88 ms, ranked-row take
0.89 ms and result materialization 4.68 ms. Validation remains the dominant SQL
cost. Provider and bridge latency distributions are close; this does not support
attributing the large SQL difference to the Rust FFI wrapper. Direct workspace
reuse had a 2.81x lower median than direct reset but increased process RSS and
still needs a separate multi-handle/thread ownership design before production use.

Reports and raw samples are retained locally under
`build/index-bench-layers-smoke-qualified-20261002/` and
`build/index-bench-layers-100k-128-20261002/`. A fresh full CMake target build
also passed the layered smoke test, recorded at
`build/index-bench-layers-smoke-cmake-20261002/`. Narrow verification passed 18
SPFresh crate tests, two Provider-example tests, 97 Python benchmark tests,
strict all-target/all-feature SPFresh Clippy on the repository's Rust 1.91.0
toolchain, Rust/C++ formatting and Python Black/Ruff checks. No ASAN,
production-wheel, Ray, concurrent-throughput or new remote-CI qualification was
performed. This is local synthetic qualification with uncontrolled page caches
and sequential measurement order; the changed host conditions also mean the
765.14 ms SQL result is not a new code-improvement claim over the earlier
847.36 ms run. All historical comparison tables remain unchanged.

## Explicit Snapshot Comparison

On 2026-10-02, the same frozen 100,000 x 128 checksum-candidate generation
was reused to compare the new SQL binary in strict and explicit snapshot modes.
No index was rebuilt. Both complete layered matrices used identical binary,
effective-query and fixture hashes, 30 independent queries, k=10, 512 heads,
posting-page limit 256, four replicas, max_check 4096, internal_results 64,
one warmup and three measured rounds: 90 measured queries per mode. Measurements
were sequential, with one DuckDB/OpenMP thread and uncontrolled OS page caches.

The measured core source is now committed as
`8d5f08e5bc85663b6272fadbd81067a8028f6fd3`, containing the checksum,
layered-benchmark and explicit-snapshot changes. Both tracked outer manifests
and lockfiles pin this revision. Measurements used the same DuckDB v1.5.0 SDK
(`d8a9d61d59`) and native `5893eb61ee3b18610b6b00f1939be7dae1af8904`
static-only patch as above. Release Rust code and the Provider example were
rebuilt with the matched SDK through the temporary local-path manifest, not
through a production-wheel build. The shell reused the same SDK objects and
compiler flags. Its final
link stripped debug sections to conserve local disk, without changing release
code generation. The new SQL artifact was explicitly qualified against the
old source report using `--qualify-sql-candidate`.

| Mode | p50 (ms) | p95 (ms) | Serial QPS | Peak process RSS (MiB) |
| --- | ---: | ---: | ---: | ---: |
| Prepared SQL ANN, strict default | 841.89 | 888.58 | 1.181 | 167.01 |
| Prepared SQL ANN, explicit snapshot | 30.33 | 33.99 | 32.78 | 169.85 |
| Rust Provider, snapshot matrix (no SQL) | 22.96 | 25.72 | 43.54 | 51.73 |
| Direct C++, reset (snapshot matrix) | 23.46 | 26.29 | 42.71 | 49.58 |
| Direct C++, reuse experiment (snapshot matrix) | 8.32 | 9.53 | 120.32 | 76.42 |
| Exact prepared scan, strict matrix | 57.28 | 58.57 | 17.46 | 134.07 |
| Exact prepared scan, snapshot matrix | 56.77 | 58.41 | 17.62 | 127.24 |

Snapshot ANN median latency was 27.76x lower than the strict control, with
round medians of 30.37, 30.27 and 30.51 ms. All 30 queries had Recall@10 of
1.0. Ranked IDs and exact Float32 distances matched across ANN layers, and
all SQL original rows matched the source qualification and every repeat.
The 160 Float32 component corrections out of 3840 matched between matrices;
source, reference, manifest and artifact hashes remained unchanged.

Each separate diagnostic run observed one provider miss followed by 59 hits.
Snapshot mode additionally observed one snapshot miss and 59 hits; strict
mode reported zero snapshot hits. Excluding the first diagnostic query,
source/store validation had a median of 812.41 ms in strict mode, versus
0.0166 ms for retained-source lookup in snapshot mode. Snapshot Provider/native
search was 25.78 ms, original-row take 0.92 ms and Rust result materialization
0.73 ms. This is an explicit contract change avoiding repeated validation,
not a faster checksum or a claim that the default now costs 30 ms. Native
workspace lifetime rules were not changed; `cpp-reuse` remains experimental.

The snapshot process's first SQL query took 1905.04 ms, versus 2030.18 ms in
strict mode. These single observations include complete initial verification
and provider open, not OS-cold reads. Provider-only open took 1754.05 ms in
the snapshot matrix and its first search took 25.55 ms. First-call cost is
not removed. Snapshot owners continue reading their pinned rows after original
source/artifact/manifest mutation; changed references and disabled external or
local filesystem access are still rejected. Retained source/artifact budgets
are not RSS limits, and snapshot budget exhaustion errors instead of falling
back to rereading files.

An initial qualification before the final request-layout and diagnostic-check
cleanup measured 29.01 ms snapshot and 839.32 ms strict medians. Its samples
remain retained separately; the table above is the final-code rerun, not a
selection of the fastest observations.

The strict result lies within the earlier checksum candidate's local latency
range; exact-control timing and host load varied between matrices. These are
synthetic local SDK results, not portable speedup guarantees or cold-disk,
concurrent-throughput, Ray or production-wheel qualification. No sanitizer
work was added, and the new loadable extension has not been qualified.

Verification passed 119 all-feature index library tests, two index doctests,
349 SQL adapter library tests (including 19 snapshot cases), and 121 Python
benchmark tests. Strict all-target/all-feature Clippy for `vortex-index` and
`vortex-duckdb` passed on the repository's Rust 1.91.0 toolchain, along with
Rust/C++ formatting and Python Black/Ruff. Core SQL unit tests use the default
precompiled DuckDB v1.5.5; the separately rebuilt native SDK shell above is
v1.5.0. Its original qualification passed 66 processes and 38 negative cases,
with Recall@10 of 1.0, unchanged-reference pinning and original-row equality.

A separate real-SPFresh probe rewrote a source, sealed manifest and native
`vectors.bin` after two prepared snapshot queries, then forced catalog rebind.
Both queries still returned identical complete ranked rows from the retained
view, with three snapshot hits after the first miss. The default strict query
rejected the changed source, restoring the original bytes allowed strict
search again, and deallocation released scratch. Its SQL, logs, ranked CSV and
summary are retained at `build/index-snapshot-native-probe-final-20261002/`.
The original native qualification is retained at
`build/index-sql-snapshot-final-native-qualification-20261002/`.

Reports, raw samples and diagnostic events are retained under
`build/index-bench-snapshot-smoke-20261002/`,
`build/index-bench-snapshot-100k-128-20261002/` and
`build/index-bench-snapshot-strict-100k-128-20261002/` for initial qualification,
and `build/index-bench-snapshot-final-100k-128-20261002/` plus
`build/index-bench-snapshot-final-strict-100k-128-20261002/` for final code.

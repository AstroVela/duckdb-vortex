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

## Same-Input SIFT Comparison

`scripts/bench_index_sift.py` compares native C++, the native bridge, the Rust
Provider, and prepared strict/snapshot SQL on the exact complete SIFT100K or
SIFT1M inputs already recorded by an OpenData benchmark. This companion needs
NumPy, a qualified native `spfresh_benchmark`, a qualified `bench_provider`, and
Vortex's native-only `bench_sift_fixture` example. It verifies file sizes and
SHA-256 values in the supplied input manifest before conversion and again after
measurement. The manifest is a list of dataset entries with `name`, `rows`,
`dimension`, and `files.base/queries/groundtruth` identities, each containing
absolute `path`, `bytes`, and `sha256`.

```bash
uv run --no-project --with numpy python scripts/bench_index_sift.py \
  --dataset sift100k \
  --input-manifest /absolute/path/opendata-groundtruth-check.json \
  --duckdb /absolute/path/duckdb \
  --native /absolute/path/spfresh_benchmark \
  --provider /absolute/path/bench_provider \
  --fixture-builder /absolute/path/bench_sift_fixture \
  --queries 1000 --strict-queries 16 --warmup-rounds 1 --rounds 1 \
  --output-dir build/index-sift100k
```

Use `--fixture /absolute/path/existing-fixture` to reuse an already sealed fixture
whose input identities match. IDs and embeddings retain their original fvecs
order. Recall@10 uses the supplied original ground truth, not agreement among
ANN layers. Query qualification checks Float32 casts; every returned distance
is checked against its original base row, and SQL must return the exact original
embedding. Ranked IDs/distances must agree across layers and warmup/repeat rounds.

The first tested `internal_results` among `--probes 64 128 256 512` reaching
`--recall-target 0.99` is frozen across layers with `--max-check 32768` and the
fixture's posting-page limit. Tuning uses the same benchmark queries, not a
held-out quality evaluation. Failure to reach the target is an error, not a
lower-quality timing presented as an equal-quality comparison. The native and
Provider tools retain their 256-query cap, so 1,000 queries run in four sequential
processes, each with full warmup and separate open/close times. SQL uses one
prepared owner per mode. `cpp-reuse` is a direct same-handle/thread lower bound
without bridge lifecycle/option guards. Current bridge builds retain one workspace
pair per handle and lend it to the calling thread only during serialized searches;
parameter changes and search errors discard it. Interpret older reset-based
bridge reports using their recorded binary hashes, not current source behavior.

Strict SQL verifies files on every call. `--strict-queries 16` uses evenly spaced
query IDs and reports their matched subsets separately across all other layers;
do not compare 16-sample tail percentiles to the full 1,000-query distribution.
Diagnostics run in separate processes and verify actual provider/snapshot cache
hits or misses. `progress.json` preserves completed layers if a later step fails.

SIFT1M's raw vector input needs 488.28 MiB, above the production builder's
256 MiB default; the fixture tool alone opts into a 512 MiB input budget. Its
index also exceeds SQL's default 256 MiB retained-artifact budget. The driver
checks that default snapshot SQL rejects this case and measures default strict
SQL without retained handles. It must not silently fall back to a snapshot.
To additionally measure warm snapshots with a **separately compiled,
benchmark-only** shell, explicitly provide both:

```bash
--snapshot-duckdb /absolute/path/benchmark-only-cache1g/duckdb \
--snapshot-cache-budget-mib 1024
```

The declared budget must match the compiled artifact. These flags do not change
runtime budgets or production defaults. The candidate's binary hash is recorded
and its cache hits are verified. Without a candidate, an oversized default
snapshot is reported as unsupported, with no invented timing. OS page caches
are not dropped; SPFresh's limits are not equivalent to OpenData's 1 GiB SlateDB
cache. Serial QPS is inverse mean search/profile latency, not concurrent or
whole-client throughput. Retain the input manifest, fixture/build metadata,
binary hashes, raw samples, profiles, diagnostics and resource logs together.

### Local SIFT Results, 2026-10-02

On the same Xeon E5-2686 v4 host, both full corpora completed with the original
first 1,000 queries, k=10, one warmup round and one measured round. SPFresh's
measured Recall@10 was 99.77% for SIFT100K and 99.40% for SIFT1M. Every measured
layer returned identical ranked IDs/distances, and SQL returned original rows.
This is an initial comparison, not a multi-run stability claim.

| Path | SIFT100K p50 / p99 ms | SIFT1M p50 / p99 ms |
| --- | ---: | ---: |
| Native C++, workspace reset | 14.95 / 16.96 | 37.58 / 44.88 |
| Native C++, experimental workspace reuse | 2.49 / 2.96 | 10.35 / 13.61 |
| Native bridge | 15.10 / 16.51 | 37.54 / 44.75 |
| Rust Provider | 15.12 / 16.66 | 33.72 / 40.75 |
| Prepared snapshot SQL | 19.23 / 21.82 | 28.43 / 35.03 |

The SIFT1M snapshot row uses the separately compiled **benchmark-only 1 GiB
retained-artifact budget**. Default snapshot SQL correctly rejects its
509.94 MiB index; the production builder's 256 MiB raw-input default also remains
unchanged. SIFT100K fits default budgets. Build configurations were 4,096 heads /
4 replicas and 16,384 heads / 1 replica, respectively. Both use 128 posting pages
and max_check=32,768; internal_results=64 and 512 reached the recall target.
The initial SIFT1M tuning points at 64/128/256 returned only 87.52%/94.35%/98.14%
recall; these are not the performance points reported above.

Default strict SQL, on the 16 evenly spaced control queries, had p50 719.22 ms
and 5,374.42 ms, respectively. The 1M diagnostic had no provider cache hits:
after the first query, median source validation was 2,316 ms and provider open
was 2,968 ms. The snapshot diagnostic had 15/15 later provider/snapshot hits.
Its 1M native-search phase was about 26 ms, below the independent Provider
process's median. Different executable environments, allocation history and
timing boundaries can affect these processes; do not subtract them to claim a negative
SQL overhead or attribute the difference to a particular allocator without
a controlled follow-up. The same-binary C++ reset/reuse contrast is the clearer
evidence for prioritizing workspace lifecycle optimization.

The previous OpenData native Rust/SlateDB runs on these exact input hashes
reached 99.59% and 99.10% recall with warm p50 8.60 ms and 14.28 ms. Those used
a 1 GiB SlateDB cache and different storage/runtime and warmup behavior, not
SPFresh's budgets; they are quality-qualified reference points, not identical
execution conditions or a unified engine ranking.

Local raw reports are `build/index-sift100k-final-20261002/summary.json` and
`build/index-sift1m-final-20261002/summary.json`, with corresponding resource
directories. The initial 1M run retains the full tuning curve and build logs in
`build/index-sift1m-20261002`; the final run reuses that exact sealed fixture and
reconfirms the selected 512-candidate point. The experimental shell's source
delta, link argv and hashes are retained in
`build/index-sql-sift-cache1g-20261002`. The earlier OpenData report is
`/home/kaka/opendata/bench-runs/sift-baseline-20261002/summary.md` on this host.

### Handle-Owned Workspace Results

The subsequent local candidate retains one SPANN/BKT workspace pair per native
handle, lends it to the calling thread for a serialized synchronous search, and
detaches it from TLS on return. Capacity-option changes and failures after
borrowing discard the pair. Closing on another thread releases it; no pointer-keyed
global cache or per-worker copies remain. Index format, page-limit constraints,
search budgets, source/reference validation and production defaults are unchanged.

The exact same sealed SIFT generations and 1,000 queries were repeated with
one full warmup and **three measured rounds**, 3,000 samples per full-data layer.
This is repeated execution within each worker, not three independent SQL process
restarts. The original initial reports above used one measured round. Recall
remains 99.77% / 99.40%; all ranked IDs/distances, original SQL rows and repeat
results agree, and source/fixture hashes remain unchanged.

| Path | SIFT100K p50 / p99 ms | SIFT1M p50 / p99 ms |
| --- | ---: | ---: |
| Native C++, workspace reset control | 14.95 / 16.87 | 38.96 / 48.76 |
| Native C++, direct workspace reuse | 2.52 / 2.97 | 11.48 / 15.38 |
| Native bridge, handle-owned reuse | 2.51 / 2.94 | 11.51 / 15.20 |
| Rust Provider, handle-owned reuse | 2.45 / 2.91 | 11.01 / 14.49 |
| Prepared snapshot SQL | 6.49 / 7.21 | 14.70 / 17.92 |

Compared with the original initial reports, Provider p50 decreased from
15.12 to 2.45 ms and 33.72 to 11.01 ms; snapshot SQL decreased from 19.23 to
6.49 ms and 28.43 to 14.70 ms. The same-binary reset/bridge contrast is the
controlled workspace comparison. Independent executable medians should not be
subtracted to infer wrapper overhead. Provider's three round medians were
2.447/2.454/2.455 ms and 11.080/10.979/10.986 ms; snapshot SQL's were
6.504/6.484/6.487 ms and 14.636/14.679/14.803 ms.

The 1M snapshot still uses a **benchmark-only 1 GiB retained-artifact budget**.
Default snapshot again rejects its 509.94 MiB index. Default strict SQL on the
16 evenly spaced controls, 48 measured samples, remains 716.32 ms / 5,379.55 ms.
Its 1M diagnostic has zero provider hits, with median source validation 2,341 ms
and provider open 2,997 ms. Snapshot diagnostics observe 15/15 later hits in
both caches, with 1M native search 11.13 ms, original-row take 1.02 ms and
result materialization 0.63 ms. First verification/open costs are not removed.

Retention trades memory for latency. Provider peak RSS increased from about
44 to 52 MiB on 100K and 93 to 164 MiB on 1M. Current snapshot SQL peaks are
140/390 MiB; strict SQL peaks are 138/484 MiB. The older runs used fewer rounds,
and allocator history differs, so these peaks do not isolate an exact workspace
memory delta. Posting-buffer budgets are not whole-process or total-handle RSS
budgets. The same current native binary peaks at 77 MiB in reset mode versus
148 MiB in handle-owned bridge mode on 1M.

Against the earlier OpenData quality-qualified reference, Provider is now faster
on both datasets. Snapshot SQL is faster on 100K (6.49 vs 8.60 ms), while 1M
is close but slightly slower (14.70 vs 14.28 ms). This is not a controlled
algorithm ranking: SlateDB cache/runtime, warmup and timing boundaries differ.

Raw reports are `build/index-sift100k-workspace-20261002/summary.json` and
`build/index-sift1m-workspace-20261002/summary.json`. Candidate binary archives,
link commands and provenance are under `build/index-workspace-sql-20261002`;
native qualification, new Provider and the preserved old Provider are under
`build/index-workspace-native-20261002`. These are local source qualifications,
not a dependency-revision bump in the outer extension. Native lifecycle tests,
locked all-feature Rust tests, all-target Clippy and Python benchmark tests pass;
no ASAN work is included.

### Committed-Source Repeat, 2026-10-03

The measured companion integration pinned Vortex
`b7e903342be08984f55976420a6441e8da41b515`, based on merged #15
`dd15b32254ca9649ee1fd1925aadc9fd53b8bfc6`, in both tracked adapters. The default
release SQL archive was rebuilt through the tracked Vane Cargo manifest and
lock, without a local source override. Provider/fixture executables compile the
example sources from that same Git checkout through a run-local harness whose
Vortex revisions and registry package versions/checksums match the adapter lock.
Native SPFresh remains pinned at
`5893eb61ee3b18610b6b00f1939be7dae1af8904` with the unchanged static-only patch.
Rust is 1.97.1; these local shell measurements use the qualified DuckDB v1.5.0
SDK (`d8a9d61d59`), not a production wheel or the Vane release runtime. Only the
Rust archive and output path were replaced in the preserved SDK link arguments.

After Vortex #16 merged, the current adapters and release/CI assertions pin its
formal commit `c6f7e497f99205937a6f4e73b4ab9ac3b74593b3`. The merged and measured
commits have the identical Git tree
`c2ee9a4d8ff62dc1f10f3e00120026399fbbba5f`. Both locked adapter builds and pin
contracts are revalidated for that formal revision; these measurements retain
their actual source/binary hashes rather than being relabeled as a new run.

Both original sealed generations and the first 1,000 official queries were
reused, with k=10, max_check=32,768, 128 posting pages and internal_results=64/512.
Each full-data path has one warmup plus three measured rounds, 3,000 measured
samples. Native/Provider still use four sequential capped workers; SQL keeps one
prepared owner. SQL timings remain profiler-enabled materialized-query latency,
not the unprofiled C API timing boundary of the full-record comparison below.

For this repeat, the entire driver and all sequential child workers share one
temporary systemd cgroup: CPU affinity 8, CPUQuota=100%, MemoryMax=2 GiB and
MemorySwapMax=0, with OMP_NUM_THREADS=1. This includes the driver's input mapping
and validation memory; it is not the per-engine phase-accounting method below.
The host remains shared and OS caches are not evicted. No competing builds or
benchmarks from this qualification ran during measurement.

| Path | SIFT100K p50 / p99 ms | SIFT1M p50 / p99 ms |
| --- | ---: | ---: |
| Native C++, workspace reset control | 15.25 / 17.06 | 40.91 / 48.92 |
| Native C++, direct workspace reuse | 2.45 / 2.86 | 10.98 / 14.69 |
| Native bridge, handle-owned reuse | 2.45 / 2.86 | 10.81 / 14.60 |
| Rust Provider, handle-owned reuse | 2.49 / 2.89 | 11.41 / 15.18 |
| Prepared snapshot SQL | 6.85 / 7.53 | 20.45 / 34.23 |

Recall@10 remains 99.77% / 99.40%. Ranked IDs and Float32 distances agree across
all layers and warmup/measured repeats; all returned SQL embeddings match their
original base rows. Input and fixture identities are unchanged, and report
binary hashes match the preserved executables.

The SIFT1M snapshot row still requires a separately compiled **benchmark-only
1 GiB retained-artifact budget**. Its core copy differs from the pinned commit
only in that one budget constant. The production 256 MiB default correctly
rejects the 509.94 MiB index; neither production cache budgets nor the builder's
raw-input default change. Snapshot diagnostics observe one initial miss followed
by 15/15 provider and snapshot hits on both corpora. Strict diagnostics have no
snapshot hits; the 1M provider also has no hits under the default artifact budget.

Default strict SQL uses only four evenly spaced controls (IDs 0, 333, 666, 999),
12 measured samples, with p50 716.40 / 6,074.53 ms. These controls are not the
full-query tail distribution and remain slow because repeated validation and,
for 1M, provider reopen are not bypassed. First snapshot queries take
1,923.88 / 6,008.77 ms; warm timings do not remove verification/open costs.

Provider round medians are 2.497/2.487/2.487 ms and
11.432/11.336/11.482 ms. Snapshot SQL round medians are
6.845/6.860/6.853 ms and 18.139/20.743/23.200 ms. During the 1M snapshot worker,
the host's one-minute load average rises from 4.84 to 13.19. Preserve this timing
variation rather than presenting the earlier 14.70 ms local median as a stable
guarantee. The same-binary reset/reuse contrast still demonstrates the workspace
benefit; independent executable medians are not an isolated wrapper-cost
measurement or a new controlled OpenData ranking.

Raw samples, profiles, diagnostics and worker resource logs are retained at
`build/committed-sift100k-20261003/summary.json` and
`build/committed-sift1m-20261003/summary.json`.
`build/COMMITTED_SIFT_PROVENANCE.md` records the exact source, build and cgroup
qualification; preserved archives and structured link commands are under
`build/committed-default` and `build/committed-cache1g`. Historical reports above
remain separate. This follow-up adds no ASAN work, posting-reader redesign or
paid-service calls.

#### Low-Load Recheck, 2026-10-03

After the host became quieter, both corpora were rerun sequentially without
rebuilding binaries, changing the sealed fixtures or retuning. Binary/script
hashes, input/reference identities, build metadata and all benchmark parameters
are identical to the committed-source repeat above, including the whole-driver
single-core/2 GiB cgroup. Each full-data layer again has 3,000 measured samples
after one full warmup. The initial host load average is 0.55, with approximately
99% idle CPU in the pre-run samples; the host is still not exclusively reserved.

| Path | SIFT100K p50 / p99 ms | SIFT1M p50 / p99 ms |
| --- | ---: | ---: |
| Native C++, workspace reset control | 15.35 / 17.33 | 37.65 / 45.57 |
| Native C++, direct workspace reuse | 2.48 / 2.97 | 10.07 / 13.32 |
| Native bridge, handle-owned reuse | 2.48 / 2.94 | 10.11 / 13.40 |
| Rust Provider, handle-owned reuse | 2.47 / 2.91 | 10.09 / 13.56 |
| Prepared snapshot SQL | 6.66 / 7.52 | 15.06 / 18.73 |

Recall@10 is unchanged at 99.77% / 99.40%, with identical ranked IDs/distances
across layers and repeats and exact original SQL embeddings. Snapshot SQL round
medians are 6.664/6.648/6.669 ms and 15.052/15.066/15.054 ms; Provider round
medians are 2.469/2.468/2.472 ms and 10.065/10.085/10.112 ms. During the snapshot
workers, one-minute host load averages are 1.06 to 1.30 on 100K and 1.25 to 1.46
on 1M. The 1M snapshot median is 26.4% lower than the preceding 20.45 ms run,
and its p99 decreases from 34.23 to 18.73 ms. This same-artifact recheck supports
environmental interference as a contributor to the earlier timing variation;
it is not a new code optimization or a guarantee on other hosts.

Default strict SQL remains slow: p50 is 708.05 / 5,364.03 ms on the four control
queries, 12 measured samples, with 1M round medians
5,365.46/6,092.71/5,351.78 ms. Its validation/reopen work still varies even on
the quieter host. Snapshot diagnostics again confirm 15/15 later provider and
snapshot hits. The default 256 MiB budget still rejects SIFT1M; its snapshot row
uses the same explicit **benchmark-only 1 GiB** shell, not a changed production
default. First snapshot queries still cost 1,903.17 / 5,257.86 ms.

Both previous reports are retained, not overwritten or excluded. The new full
reports and raw samples are
`build/committed-sift100k-idle-20261003-234042/summary.json` and
`build/committed-sift1m-idle-20261003-234042/summary.json`.

### Full-Record API Comparison

`scripts/bench_index_sift_end_to_end.py` compares prepared DuckDB through its
C API with OpenData's `sift_repeat` entry point. Both return the top-10 original
IDs, squared L2 distances and complete Float32[128] embeddings. The SQL
projection omits the fixture-only `label`; OpenData uses its standard full-record
search path. This is closer to equivalent application work than comparing the
ID/distance-only Provider directly against OpenData's record-returning API.

Each worker keeps one database/connection open for a complete warmup and three
measured query-set rounds. DuckDB also keeps one prepared handle. Query value
construction/binding happens before each timer; timing ends when the full
caller-owned records are available. C API chunk extraction and embedding copies
are included. Neither engine enables a query profiler. Output serialization,
validation, open/prepare/close and rate-limit scheduling are excluded; there is
no query producer/QPS limiter in this workload. Raw records are validated against
the original fvecs, squared L2 distances and original ground truth, including
warmup. Each engine's ranked results must agree across repeats; only the SQL
results must agree with SPFresh Provider parity. Cross-engine ANN rankings need
not be identical. Parity samples must cover every requested query; a larger
contiguous query set can be reused for a smaller `--queries` run. All supplied
parity repeats are checked before selecting that prefix.

The C API executable is compiled against the qualified DuckDB headers and linked
with the exact libraries/extension archive from a recorded shell link command,
replacing only shell object files. It does not rebuild or change the extension.
`build-capi` records the compile/link argv, source/archive/binary hashes and
declared compiled artifact budget; the budget flag is provenance, not a runtime
setting. Relative archive paths are resolved against `--sdk-root`, matching the
linker's working directory. For example:

```bash
uv run --with numpy python scripts/bench_index_sift_end_to_end.py build-capi \
  --link-manifest /absolute/path/to/qualified-shell/link-command.json \
  --sdk-root /absolute/path/to/matching-duckdb-build \
  --duckdb-include /absolute/path/to/matching-duckdb/src/include \
  --output-dir build/sift-capi-default --artifact-budget-mib 256

uv run --with numpy python scripts/bench_index_sift_end_to_end.py run \
  --dataset sift100k \
  --input-manifest /absolute/path/to/groundtruth-check.json \
  --fixture /absolute/path/to/sealed-sift100k-fixture \
  --capi build/sift-capi-default/bench_index_sift_capi \
  --opendata /absolute/path/to/sift_repeat \
  --opendata-root /absolute/path/to/opendata \
  --opendata-config /absolute/path/to/original-sift100k-bench.toml \
  --parity-samples /absolute/path/to/qualified-provider/samples.json \
  --probes 64 --warmup-rounds 1 --rounds 3 \
  --output-dir build/sift100k-full-record-api
```

The optional OpenData comparison requires a benchmark-instrumented checkout
providing `sift_repeat` and its full-record, phase-barrier and cache-counter
protocols. The local reports below used that additional benchmark harness,
not an unmodified upstream CLI. This PR does not provide or modify OpenData's
engine; retain the recorded harness source and binary hashes when reproducing
the comparison.

Build that OpenData entry point with
`cargo build --locked --release -p vector-bench --bin sift_repeat`. The driver
uses the original storage path plus the normal bencher's `/0` suffix, the original
query/ground-truth paths and unchanged nprobe. Omitted nprobe remains omitted in
the generated config; the effective value comes from the running engine, not a
duplicated default in Python. There is no ingest or index tuning. Each output
directory must be new. A partial report is retained with failed status on errors.
Relative local object-store paths are resolved against `--opendata-root`, the
worker's working directory, including when staging private copies under resource
limits.

For SIFT1M, use the original `nprobe256.toml`, `--probes 512` and a separately
linked **benchmark-only 1 GiB** C API binary. The default 256 MiB artifact budget
cannot retain that index. This comparison is explicit snapshot mode, not default
strict SQL performance. OpenData retains its 1 GiB SlateDB block cache; the two
budgets constrain different objects and are not equal RSS limits.

One caller is not one CPU: OpenData uses Rayon/Tokio=4, whereas DuckDB uses
threads=1 and SPFresh OMP=1. The driver records GNU time user/system CPU, CPU
percentage, peak RSS, wall time, load averages, binary hashes and actual settings.
OS caches are not evicted and the host is not CPU-isolated. OpenData's existing
store is opened writable and may compact; the Vortex generation remains sealed.
Serial inverse-mean API latency is not client/serialization throughput.

`--opendata-maintenance off` is an explicit query-only control. It writes a
run-local SlateDB settings file disabling the embedded compactor and GC. It does
not change the original config, index parameters, query contents or cache budget.
The default remains `default`, preserving the original maintenance behavior.

#### Local Full-Record Results, 2026-10-02

Both full corpora use the original first 1,000 queries, k=10, one complete warmup
and three measured rounds in each worker (3,000 measured calls). Input and sealed
generation hashes are unchanged. Every warmup/measured embedding and distance
was verified against the original base rows, every engine's repeats agree, and
SQL matches the existing Provider parity. No search algorithm was changed.

| Workload | SQL snapshot C API p50 / p99 ms | OpenData API p50 / p99 ms | SQL / OpenData Recall@10 |
| --- | ---: | ---: | ---: |
| SIFT100K, original maintenance | 7.46 / 10.83 | 8.26 / 9.73 | 99.77% / 99.59% |
| SIFT1M, original maintenance | 14.64 / 21.78 | 17.79 / 20.88 | 99.40% / 99.10% |
| SIFT1M, maintenance-off control | 15.03 / 19.02 | 17.61 / 20.78 | 99.40% / 99.10% |

The original-maintenance 1M run logged four nonfatal SlateDB GC object-store
errors. Searches, close and correctness validation succeeded, but that run is
not a clean-background baseline. The explicit maintenance-off repeat had no
logged errors and preserved 9,521 centroids, recall and ranked-result parity.
OpenData p50 changed only from 17.79 to 17.61 ms; this does not support attributing
the main latency difference to GC. SQL was rerun too, rather than mixing a new
OpenData result with the old SQL timing. This is a query-only control, not a fix
for the underlying GC errors.

The SQL/OpenData three-round p50s on 100K were
7.447/7.482/7.468 ms and 8.295/8.298/8.178 ms. On the clean-background 1M control
they were 15.576/15.009/14.516 ms and 17.652/17.744/17.276 ms. The host remains
non-isolated, and SQL's round medians show drift; these are local repeat results,
not a universal engine ranking. SQL's median was lower in both comparisons,
but its 100K tail was worse. The different profiler-based results and earlier
single-round OpenData workload are retained above, not silently replaced or
treated as proof of an OpenData performance regression.

Peak worker RSS was SQL/OpenData 138/149 MiB on 100K and 387/946 MiB on the
maintenance-off 1M control. GNU time CPU percentages were 99%/306% and 99%/318%
over their respective whole runs, including untimed output and startup. These
are not isolated query CPU costs. The block-cache and retained-artifact budgets
limit different objects; neither is a total process memory limit.

These are **warm explicit snapshots**, not strict default SQL measurements.
100K uses the default 256 MiB artifact budget; 1M still needs the benchmark-only
1 GiB archive. Production defaults remain unchanged. First search was about
2.04 s on SQL versus 28.6 ms on OpenData for 100K; on the maintenance-off 1M
control it was 6.08 s versus 46.3 ms. OpenData additionally spent 0.24/2.25 s
opening its database; SQL's separate prepare cost was below 1 ms. Warm latency
does not eliminate the initial verification/open cost.

Raw paired reports are `build/index-sift100k-e2e-20261002/summary.json`,
`build/index-sift1m-e2e-20261002/summary.json`, and
`build/index-sift1m-e2e-no-maintenance-20261002/summary.json`. Each contains the
effective settings, binary/source identities, worker command lines and timings;
their directories retain full-record samples and GNU time/resource logs. The C API build manifests live
in `build/index-e2e-capi-20261002-default` and
`build/index-e2e-capi-20261002-cache1g`. OpenData's local run overview is
`/home/kaka/opendata/bench-runs/sift-e2e-20261002/summary.md`.

Local qualification passed 169 Python tests, including seven compiled C API
protocol cases, all seven Rust benchmark library tests (three new repeat tests) and all-target/all-feature
Clippy for `vector-bench`. CI now includes the pure benchmark parser/validation
tests; compiled protocol cases require `VORTEX_SIFT_CAPI` and skip without that
artifact. Workflow YAML parsing and duplicate-key/indentation/whitespace checks
pass. Applying Vortex's stricter YAML style config to this separate repository
reports existing empty-value/comment-spacing issues on both HEAD and the
modified workflow; these unrelated formatting issues were not changed.
Remote CI and ASAN were not run.

#### Resource-Controlled Comparison

Equal configured cache sizes do not imply equal caching mechanisms or total
memory. The constrained mode instead aligns the allowed resources and requires
close Recall@10 before accepting a paired result:

- `--cpu 8 --memory-mib 2048` launches sequential systemd user services with
  affinity to logical CPU 8, `CPUQuota=100%`, `MemoryMax=2 GiB` and swap disabled.
  A working Linux cgroup-v2/systemd user delegation is required. Each untimed
  phase validates the live affinity, quota, memory/swap limits and OOM counters;
  recording requested settings alone is not considered verification.
- OpenData Rayon/Tokio workers are 1; DuckDB threads and SPFresh OMP are 1.
  IO/blocking helper threads are not removed. One CPU budget is not a promise
  that the process has only one OS thread.
- Both APIs return ID, squared-L2 distance and the complete original embedding.
  The original first 1,000 queries, k=10, one full warmup and three measured
  rounds are unchanged. Every returned record and repeat is validated; SQL
  also retains Provider parity. Index construction and ingestion are excluded.
- Recall must be at least 99%, with an absolute gap no larger than 0.002
  (0.2 percentage points). `--opendata-nprobe` explicitly calibrates quality
  without editing the original config. Diagnostics freeze the main worker's
  effective value, including when the config omitted `nprobe`.
- Main workers disable the metrics recorder and SQL timing trace. Separate
  one-warmup/one-measured workers collect existing SlateDB counters and SQL
  handle/source cache hits under the same constraints. Diagnostic timings are
  retained but are not headline latencies.

For example, reuse the qualified 100K inputs, fixture and libraries:

```bash
uv run --no-project --with numpy python scripts/bench_index_sift_end_to_end.py run \
  --dataset sift100k \
  --input-manifest /home/kaka/opendata/bench-runs/sift-baseline-20261002/groundtruth-check.json \
  --fixture build/index-sift100k-20261002/fixture \
  --capi build/index-resource-capi-20261002-default/bench_index_sift_capi \
  --opendata /home/kaka/opendata/target/bench-release/release/sift_repeat \
  --opendata-root /home/kaka/opendata \
  --opendata-config /home/kaka/opendata/bench-runs/sift-baseline-20261002/sift100k/bench.toml \
  --parity-samples build/index-sift100k-workspace-20261002/provider/samples.json \
  --probes 64 --cpu 8 --memory-mib 2048 --opendata-maintenance off \
  --output-dir build/new-sift100k-resource-run
```

For 1M, use the matching 1M fixture/parity and benchmark-only 1 GiB C API
artifact, `nprobe256.toml`, `--probes 512 --opendata-nprobe 320`, and the opposite
order `--order opendata sql-snapshot-capi`. Output directories must be new.
The production 256 MiB artifact limit is not changed.

OpenData uses private copies of the original local SlateDB store, made inside
each worker cgroup with distinct inodes, so newly populated file cache is charged
there rather than inherited from the original store. Staging is outside API
latencies but included in whole-run time and cgroup memory peak. The original
store is not ingested into or modified. Vortex uses the original sealed
generation. Maintenance is explicitly off for this query-only control, not fixed.

`MemoryMax` limits memory charged to the unit, not all host memory: shared cache
already charged to another cgroup is not globally reassigned, and Vortex's
original files are not cloned. No global OS-cache eviction or CPU isolation is
performed. Record process RSS and cgroup peak separately; the latter includes
charged file cache, staging and output. A nonzero `memory.events.max` indicates
limit pressure/reclaim, not an OOM; OOM counters disqualify the run.

Phase reports distinguish logical read bytes (`/proc/PID/io` `rchar`) from
kernel-accounted storage reads (`read_bytes`), and include minor/major faults and
CPU time. Measured-round deltas include untimed binding, validation and sample
output; the isolated first-search delta covers only that API call. Do not divide
round CPU time by query count and present it as pure search CPU.

SlateDB `db_cache.access_count` keeps hit/miss and entry-kind labels. Its fetch
hits include requests sharing an in-flight load. SQL provider/snapshot hits
mean retained handles/source buffers, not posting-block or OS-cache hits. These
are different metrics and must not be combined into a common cache-hit rate.
SQL diagnostics require an initial miss in both caches and hits on every later
query, including the rest of warmup. Before calculating any OpenData round deltas,
the driver validates every adjacent pair of snapshots: each registered counter
must remain present and never decrease, including across warmup, round boundaries
and close. Newly registered counters are allowed. Violations identify the phase
and counter and fail the run instead of producing an `ok` report. This contract
applies to every exported counter; the harness must omit interval histogram counts
or convert them to cumulative totals before exporting them as counters.
The corrected OpenData harness uses `BenchRecorder::snapshot_counters` to read
registered counters directly, without draining histograms or exporting their
derived counts. This is not a metric-name allowlist: genuine counters with a
`_count` suffix remain included, even when a histogram has the same derived
count name. Reports hash `bencher/src/metrics.rs` and `bencher/Cargo.toml` alongside
the existing benchmark sources so the recorder implementation is traceable.
This is a more controlled warm resource/quality comparison, not identical cache
implementations or strict-default SQL performance.

#### Verified-Open Dependency Alignment, 2026-10-07

Both extension adapters, their lockfiles, the production wheel source gate and
the installed-wheel workflow now use Vortex PR #18's formal merge,
`30265bd0fdcf70477acaff3f380a015dc6b2f261`. It includes the posting views below
and the verified-open path that validates artifacts while creating their private
copies. See the upstream
[verified-open report](https://github.com/AstroVela/vortex/blob/30265bd0fdcf70477acaff3f380a015dc6b2f261/vortex-duckdb/VERIFIED_OPEN_BENCHMARK.md)
for that optimization's measurements.

PR #32's installed-wheel smoke had retained the older `c6f7e497` workflow pin
while its lockfile used `67303ad0`. The source preflight rejected this mismatch
before installing or running the wheel. A regression now compares the workflow's
expected revision and version with the reviewed build source and lockfile; it
failed before the workflow correction. Each regenerated lockfile changes only
39 Vortex Git source IDs, with every other parsed field unchanged.

The merged source passes 76 focused release/workflow tests and 48 subtests,
both adapters' locked all-feature scoped Clippy, and a fresh Vane Rust release
build. Relinking the recorded DuckDB v1.5.0 SDK objects with that unpatched
archive passes the SQL regression in both copying and view modes: 66 independent
processes, 38 negative cases and fixture Recall@10 of 1.0 per mode. Provenance
and per-process logs are retained at `build/verified-open-merged-20261007/`.
The installed-wheel source preflight itself accepts the current pin and rejects
the stale one. Remote wheel/Ray qualification is a separate CI run.

The SIFT tables below retain their measured `67303ad0` source identity. They are
not measurements of `30265bd0`; this dependency update does not rerun the full
SIFT performance matrix or change the default cache budgets.

#### Merged Posting-View Integration, 2026-10-05

For these measurements, both extension adapters and their lockfiles pinned
Vortex PR #17's formal merge,
`67303ad0157f873eea54e9f333737a4ad6bdaa36`, from duckdb-vortex PR #31's merge
`348d44eebb39009c291c776104a6c6649d8460d5`. Each lockfile changes only 39 Vortex
Git source IDs; other parsed fields are identical. The production wheel source
gate uses the same revision. Native, Provider, Rust archives, SQL shells and C API
executables were rebuilt before timing; this section does not reuse the older
SQL binaries reported below. SDK link objects were reused, with their source
owner and DuckDB v1.5.0 recorded in the new build manifests.

Copying (`VORTEX_SPFRESH_POSTING_VIEW=0`) and read-only posting view (`=1`, the
native default) use the exact same binaries and original sealed generations.
The toggle is captured when a native handle opens. View mode retains the
per-posting `fstat` identity guard; it is not an unchecked pointer benchmark.
No algorithm, strict SQL default or production 256 MiB artifact budget changes.
SIFT1M alone uses a separately labeled benchmark-only 1 GiB snapshot archive;
only its retained-artifact constant differs from the merged crate's source.

The constrained driver now explicitly forwards the posting-view setting into
each systemd worker, records it in worker metadata and the report, and rejects
values other than `0` and `1` before launching workers. An unset value explicitly
selects `1` instead of inheriting a potentially different systemd manager value.
Before this fix, five new regression cases failed because the worker lacked the
setting or the report omitted it. Pure command and real systemd tests also
verify that unrelated credentials are not forwarded or logged.

All four complete-record comparisons use the original first 1,000 queries,
k=10, one warmup and three measured rounds, CPU 8 with a one-core quota, 2 GiB
charged memory, no swap and one compute worker. OpenData maintenance is off and
its corrected `68cde646d05165ece33c6e56fe4f345ce6a64f17` harness binary is frozen
across A/B. Probes remain SQL 64/512 and OpenData nprobe 100/320 for 100K/1M.
100K runs copy then view, with SQL first in each pair; 1M runs view then copy,
with OpenData first. No builds or tests run concurrently with measurements.

| Workload | SQL copy C API p50 / p99 ms | SQL view C API p50 / p99 ms | View p50 reduction | SQL / OpenData Recall@10 |
| --- | ---: | ---: | ---: | ---: |
| SIFT100K | 5.294 / 5.998 | 4.722 / 5.037 | 10.8% | 99.77% / 99.59% |
| SIFT1M | 13.886 / 17.386 | 8.927 / 10.447 | 35.7% | 99.40% / 99.46% |

View p99 is 16.0%/39.9% lower on 100K/1M. OpenData's paired copy-run/view-run
p50/p99 values are 11.232/12.173 and 11.208/12.553 ms on 100K, and
37.183/42.158 and 37.675/42.801 ms on 1M. Posting view does not affect OpenData;
both controls are retained rather than selecting one favorable timing.
SQL's copy/view round medians are 5.265/5.301/5.312 versus
4.701/4.725/4.740 ms on 100K, and 13.690/13.950/14.080 versus
8.904/8.912/8.966 ms on 1M.

Every main worker validates 40,000 complete records and measures 3,000 calls.
Recall floor/gap, repeat and SQL/Provider parity, counter continuity and live
resource checks pass, with no OOMs or logged errors. Independent diagnostics
retain 60 registered counter series through close. SlateDB measured data-block
hits/misses remain 510,636/0 and 1,208,324/0. Both SQL caches start with a miss
and hit on every later query, including 1,000/1,000 measured hits. These metrics
still do not imply identical cache implementations.

| Workload | SQL copy RSS / cgroup peak MiB | SQL view RSS / cgroup peak MiB | OpenData copy-run / view-run RSS MiB |
| --- | ---: | ---: | ---: |
| SIFT100K | 139 / 281 | 311 / 268 | 139 / 141 |
| SIFT1M | 388 / 854 | 749 / 738 | 1,019 / 1,012 |

Posting view substantially increases mapped-file RSS. Its lower charged cgroup
peak is not evidence of lower total memory: the original shared file pages may
already be charged elsewhere, unlike each worker's private SlateDB copy. Both
OpenData 1M workers reach the 2,048 MiB charged limit and record whole-worker
pressure events (1,884/4,805 for copy-run/view-run, including staging), without
OOM. Production artifact budgeting is unchanged and is not a total RSS limit.

SQL's three measured-round logical read totals drop from
13,133,244,738 to 7,068,027 bytes on 100K and from 68,953,285,689 to
7,044,027 on 1M. Read-syscall totals drop from 240,003/1,583,607 to 48,003.
These whole-round `rchar`/`syscr` counters include untimed output and validation;
they are not physical disk bytes or counts of `fstat` calls. All main measured
storage-read and major-fault deltas are zero. This is a warm-path read/copy
reduction, not a cold-storage result.

One-second samples wholly inside measured-round windows show CPU 26, CPU 8's
SMT sibling, averaging 98.60%-99.83% idle across the eight main workers, with
none below 90% idle. Boundary samples are omitted using the post-run
wall-clock/monotonic offset. These windows include work outside the API timer;
the host and CPU frequency remain unisolated. SQL view's fresh-process first
search still costs about 1.93 s/5.60 s for 100K/1M, excluding open, versus
OpenData 29/142 ms plus separate open costs of 230/2,393 ms. Warm gains do not
remove validation and initial materialization costs.

The subsequent layered A/B uses the same frozen binaries, inputs, probes and
copy/view order. Each driver and its children inherit a verified CPU 8/one-core,
2 GiB/no-swap systemd scope. Native and Provider each measure 3,000 calls after
complete per-chunk warmups; their individual timers exclude open, close and
sample output. The profiler-based SQL distribution is independent of the
uninstrumented C API distribution above.

| Layer | SIFT100K copy / view p50 ms | SIFT1M copy / view p50 ms |
| --- | ---: | ---: |
| Native C++, workspace reset control | 15.929 / 13.075 | 39.688 / 25.273 |
| Native C++, workspace reuse | 2.611 / 1.994 | 10.954 / 6.101 |
| C bridge | 2.632 / 1.997 | 11.041 / 6.061 |
| Rust Provider | 2.521 / 1.981 | 10.529 / 6.016 |
| SQL snapshot, DuckDB profiler enabled | 6.837 / 6.041 | 15.721 / 10.532 |

Every Native/Bridge/Provider copy/view pair has exactly equal ranked IDs and
distances for all 4,000 warmup/measured samples. Cross-layer and repeat parity
also pass. Native and Provider return IDs/distances; SQL materializes full rows.
The workspace-reuse C++ path is a single-handle/thread lower bound, without
bridge lifecycle guards. Do not subtract mixed API/profile timings and label
the difference as pure adapter overhead.

Strict SQL controls use only query IDs 0/333/666/999, with 12 measured samples:
copy/view p50 is 726.365/696.798 ms on 100K and 5,569.946/5,585.996 ms on 1M.
Their costs remain dominated by strict validation; their small-subset recall
and percentiles are not the full-query distribution or evidence of strict
default acceleration. Both 1M modes verify that the production 256 MiB
snapshot archive rejects the 534,710,361-byte generation before using the
separate 1 GiB benchmark archive. No budget check was bypassed.

Whole-driver layer windows show mean sibling idle of 99.50%-99.77%, with no
complete one-second sample below 90%. These windows cover validation, all
layers and output, not isolated query timers. Charged whole-driver peaks are
517/502 MiB on 100K and 1,098/1,058 MiB on 1M for copy/view, with no OOMs.

Reports are retained in the `feat/vane-index-posting-view-20261005` worktree at
`build/e2e-{sift100k,sift1m}-{copy,view}-full/summary.json`, alongside raw full
records, diagnostics, frozen scripts and phase snapshots. The 64-query smoke
passed in both modes and is not included in the table. Build manifests are
`build/capi-{default,cache1g}/build.json`; frozen binaries, link commands and
the experimental source copy remain local. Host evidence is
`build/posting-host-20261005.json` and `build/e2e-host-analysis.json`.
Layer reports are `build/layers-{sift100k,sift1m}-{copy,view}/summary.json`, with
their independent trace at `build/posting-layer-host-20261005.json`.
`build/posting-comparison.json` independently validates limits, cache/counter
continuity, source/input/binary identity and exact Native/Provider A/B ranks,
and records raw-report hashes, computed improvements and layer host windows.

The rebuilt default/1 GiB C API SHA-256 values are
`3fc5d440bdbf63f7ef93bea62181d8a414f7f5edfda54ec7905a9f3b9f42923d` and
`bb3123b52b72f769fe566191dbc6847f45e71268a45a6ec04308907d57d790ef`.
OpenData remains
`536c4abfa33be998a2d05fa377696eb318c5b1f4a76c710ed97ebb513470a5a1`.
Its matching source commit is local and has not yet been pushed.
Before/after binary guards verify unchanged artifacts throughout smoke and
full A/B. The new provenance audit verifies the clean merged source, the
lockfile-only Git changes and the sole experimental source constant change.
Original stores, fixtures and historical reports are untouched.

To repeat, use the constrained command above with the newly built C API,
matching existing fixture/parity, explicit `--opendata-nprobe 100` or `320`,
`--queries 1000 --warmup-rounds 1 --rounds 3`, and a fresh output directory
for each `VORTEX_SPFRESH_POSTING_VIEW=0` and `=1` run. Use the frozen OpenData
binary instead of a mutable target artifact; for 1M retain the separate 1 GiB
C API and opposite engine order. The local `build/run_posting_ab.py` records
the complete commands and runs the fixed copy/view ordering sequentially.

Verification before timing: 247 benchmark Python tests including real new C API
and systemd cases with no skips; 57 release/provenance tests plus 16 subtests;
both extension adapters' locked all-target/all-feature Clippy with no warnings;
three native CTest cases repeated five times in each mode; and SQL integration
in both modes, each with 66 independent processes and 38 negative cases.
Black, Python 3.11 Ruff (`E4,E7,E9,F,I`) and diff whitespace checks pass.
After measurements, a combined final Python run again passes all 304 tests and
16 subtests without skips, using the frozen new C API and real systemd workers.
No ASAN, full-workspace test/build, remote CI or paid service was run.

#### Repeat After Host Load Check, 2026-10-05

Following a report of host load during the previous run, both datasets were
rerun with the same frozen binaries, inputs, Provider parity, search parameters,
engine order and resource settings. Each main worker again uses 1,000 queries,
one warmup and three measured rounds, with independent cache diagnostics.
No benchmark implementation or binary was rebuilt for this repeat.

The matching OpenData harness and recorder sources are committed as
`68cde646d05165ece33c6e56fe4f345ce6a64f17`. Raw reports retain their original
measurement-time base revision and source-file hashes.

| Workload | SQL snapshot C API p50 / p99 ms | OpenData API p50 / p99 ms | SQL / OpenData Recall@10 |
| --- | ---: | ---: | ---: |
| SIFT100K | 5.37 / 5.85 | 11.40 / 12.33 | 99.77% / 99.59% |
| SIFT1M | 13.96 / 17.31 | 37.45 / 42.45 | 99.40% / 99.46% |

The first 100K repeat passed all driver checks and gave SQL 5.33/5.78 ms and
OpenData 11.77/13.86 ms. One-second host samples showed CPU 26, the benchmark
CPU's SMT sibling, averaging only 94.33% idle during OpenData's measured rounds.
A complete second 100K run was therefore collected and selected for the table
based on its quieter sibling trace. All first-run data is retained.

For the selected runs, CPU 26 averages 100.00%/99.94% idle during SQL/OpenData
100K measured rounds and 99.39%/99.73% on 1M. None of their measured samples
has sibling idle below 90%. Samples are aligned with monotonic phase snapshots
using the post-run wall-clock offset; only complete one-second intervals inside
measured rounds are used. These round windows include work outside the API timer
and the host remains unisolated.

SQL round medians are 5.338/5.373/5.385 ms on 100K and
13.919/13.945/13.997 ms on 1M. OpenData's are 11.467/11.373/11.363 and
37.050/37.505/37.635 ms. Compared with the preceding run, 1M SQL p50 is 6.4%
lower and p99 is 38.3% lower. This is a timing comparison with unchanged
artifacts; the prior run has no continuous sibling trace to isolate causation.

Both selected reports pass Recall, complete-record, repeat/Provider parity,
counter continuity and live resource checks, with zero OOMs or logged errors.
Each main worker verifies 40,000 records and measures 3,000 calls. Diagnostics
retain 60 registered counter series through close; data-block hits/misses are
510,636/0 and 1,208,324/0. SQL has an initial Provider/source miss and all later
queries hit, including 1,000/1,000 measured hits in each cache.

Main RSS/cgroup peaks are SQL 140/281 MiB and OpenData 175/544 MiB on 100K,
and SQL 388/861 MiB and OpenData 1,089/2,048 MiB on 1M. OpenData 1M records
4,872 whole-worker memory-pressure events, including staging, with no OOM.
All main measured storage-read and major-fault deltas are zero. Different cache
semantics and the charged-memory limitations described above still apply.

Selected raw reports are
`build/index-sift100k-counter-recheck-20261005/summary.json` and
`build/index-sift1m-counter-repeat-20261005/summary.json`. The first 100K report
is `build/index-sift100k-counter-repeat-20261005/summary.json`. Host traces,
phase-aligned analyses and the analysis script are retained under `build/`.
The overview, exact reproduction arguments and computed comparison are in
`/home/kaka/opendata/bench-runs/sift-counter-repeat-20261005/`.
Existing code tests were not rerun: this work reuses the previously qualified
artifacts and adds benchmark/report verification. Raw samples, host traces and
binary artifacts are retained locally.

#### Counter-Qualified Baseline, 2026-10-04

Both datasets now pass the current adjacent-snapshot checks without weakening
the driver. The OpenData diagnostic fix exports only genuine cumulative counters
and leaves histogram samples intact. Two regressions failed before the fix:
a same-named histogram increased a native counter from 7 to 9, and collecting
counter diagnostics consumed histogram samples. Both now pass; additional tests
cover labels, idle phases, zero counters and later registration.

The driver is based on PR #30's formal merge
`b0eb820313314f6d18cfd5e2899fee69b1befd1f`, with recorder-source hashes added.
SQL deliberately reuses the exact 2026-10-02 C API binaries and build manifests,
not newly built merge-head or posting-view binaries. OpenData is rebuilt with
the same Rust 1.97.1 release settings; its search implementation is unchanged.
Input, SQL binary, Provider parity and search-configuration identities match
the older resource-controlled runs for both datasets.

Each main worker runs the original first 1,000 queries, k=10, one complete
warmup and three measured rounds. CPU 8, a one-core quota, 2 GiB memory, no swap,
single compute workers, maintenance disabled and cache budgets are unchanged.
SQL probes are 64/512 and OpenData nprobe is 100/320 for 100K/1M. Order remains
SQL-first for 100K and OpenData-first for 1M. The 64-query smoke passed before
these runs and is not included in the headline results.

| Workload | SQL snapshot C API p50 / p99 ms | OpenData API p50 / p99 ms | SQL / OpenData Recall@10 |
| --- | ---: | ---: | ---: |
| SIFT100K | 5.36 / 5.86 | 11.67 / 13.51 | 99.77% / 99.59% |
| SIFT1M | 14.91 / 28.05 | 38.42 / 44.14 | 99.40% / 99.46% |

Every main worker validates 40,000 complete records, including warmup, with
3,000 measured calls. Repeat parity, SQL/Provider parity and the Recall floor/gap
all pass. SQL round p50s are 5.329/5.359/5.398 ms on 100K and
14.896/14.951/14.884 ms on 1M; OpenData's are 11.735/11.635/11.621 and
38.608/38.448/38.200 ms. These are warm explicit-snapshot results, not strict
SQL defaults or cold-disk latency. The host/SMT sibling remain unisolated.
Differences from earlier timings are not evidence of an algorithmic speedup:
SQL binaries are unchanged, and headline workers disable diagnostics.

Both independent one-warmup/one-measured diagnostics retain 60 registered
counter series after first search through close, with every adjacent snapshot
passing presence and monotonicity validation. Measured SlateDB data-block hits
are 510,636/1,208,324 for 100K/1M, with zero misses. Index/filter misses are also
zero; unused stats counters have no hit rate. SQL diagnostics have an initial
Provider/source miss, hits on every subsequent query, and 1,000/1,000 measured
hits. These remain different cache metrics, not proof of identical caches.

Main peak RSS / cgroup peak are SQL 140/281 MiB and OpenData 172/359 MiB on
100K, and SQL 389/859 MiB and OpenData 1,084/2,048 MiB on 1M. OpenData 1M
records 3,678 whole-worker memory-limit pressure events, including staging;
all eight main/diagnostic workers have verified live limits, no OOMs and no
logged errors. All main measured kernel storage reads and major faults are
zero. SQL measured logical reads remain 13,133,244,738/68,953,285,689 bytes,
versus OpenData's 56,547/438,309. These whole-round counters include work
outside the API timer and are not physical disk traffic or pure search CPU.

Fresh-process first searches cost SQL/OpenData 1,975/56 ms on 100K and
5,847/160 ms on 1M, excluding separate database open costs. Warm medians do
not remove SQL's first-use validation and materialization cost.

Raw reports are in the `feat/vane-index-counter-baseline-20261004` worktree:

- `build/index-sift100k-counter-final-20261004/summary.json`
- `build/index-sift1m-counter-final-20261004/summary.json`
- `build/index-sift100k-counter-smoke-20261004/summary.json`

They retain full records, timings, diagnostic counters, resource snapshots,
configs, frozen drivers and binary/source hashes. The local overview and exact
commands are in
`/home/kaka/opendata/bench-runs/sift-counter-baseline-20261004/summary.md`.
That directory also preserves both OpenData binaries, source bundles and
`build.json`; the new binary SHA-256 is
`536c4abfa33be998a2d05fa377696eb318c5b1f4a76c710ed97ebb513470a5a1`.
Original stores and all historical reports remain untouched.

Verification: 30 Rust library tests, 237 Python tests including real C API and
systemd/cgroup cases with no skips, scoped all-target/all-feature Clippy,
Rustfmt, Black and Python 3.11 Ruff checks (`E4,E7,E9,F,I`). Doctest checking
passed with no runnable cases and two existing ignored examples. Broader Ruff
rules still flag pre-existing executable-bit and `subprocess.run(check=...)`
warnings; this change does not rewrite unrelated code. No full workspace
build/test, remote CI, ASAN or paid service was run.

#### Local Resource-Controlled Results, 2026-10-02

The 2026-10-02 and 2026-10-03 reports below were accepted by the earlier driver.
Their OpenData harness exported duration histogram counts as cumulative counters
even though each snapshot drains the histogram buckets. The current checks of
adjacent snapshots reject these historical diagnostic files when those counters
disappear or decrease. Correct the harness and collect new diagnostics before
claiming qualification under the current checks; the latency tables remain
historical measurements.

Both final reports passed the earlier resource, quality, full-record and repeat
checks with no logged errors or OOMs. Main workers have recorder/tracing disabled.
Each result contains 3,000 measured calls after one complete 1,000-query warmup,
with the same 2 GiB/one-logical-CPU limits. OpenData maintenance is off in both datasets.

| Workload | SQL snapshot C API p50 / p99 ms | OpenData API p50 / p99 ms | SQL / OpenData Recall@10 | OpenData nprobe |
| --- | ---: | ---: | ---: | ---: |
| SIFT100K | 5.60 / 6.61 | 11.75 / 13.76 | 99.77% / 99.59% | 100 |
| SIFT1M | 18.23 / 29.61 | 39.20 / 51.78 | 99.40% / 99.46% | 320 |

The Recall gaps are 0.18 and 0.06 percentage points. A preliminary 1M calibration
at `nprobe=384` reached 99.65%, exceeding the allowed gap from SQL's 99.40%; it
was rejected. The qualified `320` point was then frozen for all final rounds and
diagnostics. Calibration uses the same first 1,000 queries, not a held-out
quality evaluation. Preliminary instrumented/calibration runs are not mixed
into this table. The original `nprobe256.toml` remains unchanged.

SQL/OpenData round p50s are 5.523/5.641/5.648 and 11.690/11.761/11.821 ms on
100K, and 16.316/18.563/19.312 and 38.702/39.102/39.902 ms on 1M. The 1M SQL
rounds drift materially. The host is an E5-2686 v4 with `schedutil`, CPU 8's SMT
sibling 26 is not isolated, and other host work remains possible. These are
local allowed-resource results, not a universal speedup claim or proof of a
regression against the earlier multi-worker/different-quality comparison.

| Workload | SQL peak RSS / cgroup peak MiB | OpenData peak RSS / cgroup peak MiB |
| --- | ---: | ---: |
| SIFT100K | 139 / 281 | 173 / 362 |
| SIFT1M | 388 / 853 | 1,029 / 2,048 |

OpenData 1M recorded 4,748 `memory.events.max` pressure events over the whole
worker, including staging; all OOM counters remain zero. Headline phase samples
observed up to 194/257 OpenData OS threads on 100K/1M versus 4 SQL threads,
despite both compute-worker settings and CPU budget being one. These are sampled
thread counts, not an exhaustive peak-thread census. The 1M whole-run CPU
percentages were SQL 98% and OpenData 99%, not per-query CPU utilization.

Independent measured diagnostics report 510,636/1,208,324 SlateDB data-block
hits with zero misses on 100K/1M; index and filter hits also have zero misses.
SQL has an initial handle/source miss followed by 1,000/1,000 measured hits in
each diagnostic. The different hit definitions above still apply: this confirms
both warm paths, not equivalent cache implementations.

Across the three main measured rounds, SQL logical reads are 13,133,244,738 bytes
on 100K and 68,953,285,689 on 1M, versus OpenData's 56,547 and 449,547. Kernel
storage reads are SQL 0/1,437,696 bytes and OpenData 0/0. All measured major-fault
deltas are zero. Warm caches do not remove SQL's logical read/copy/syscall work;
these counters are not themselves a CPU profile or a common cache-hit ratio.

First search still costs SQL/OpenData 2,053/81 ms on 100K and 7,202/145 ms on
1M, plus separate database open costs of 16/419 ms and 16/2,674 ms. SQL prepare
is below 1 ms. This is not a cold-disk measurement, and warm medians do not
eliminate first-use validation/materialization costs.

Final raw reports are `build/index-sift100k-resource-final-20261002/summary.json`
and `build/index-sift1m-resource-final-20261002/summary.json`. Each retains frozen
driver/resource scripts, main/diagnostic full-record samples, configs, binary and
source hashes, phase counter snapshots and GNU time logs. Build manifests are
under `build/index-resource-capi-20261002-{default,cache1g}`. The local overview
is `/home/kaka/opendata/bench-runs/sift-resource-20261002/summary.md`.

Local qualification passed 186 Python tests (including compiled C API and live
systemd cases), 9 Rust benchmark library tests, all-target/all-feature Clippy,
Rustfmt, Black/Ruff and C++ clang-format. Doctest checking passed with no runnable
cases. The pure resource/driver tests are included in CI; compiled C API and
systemd integration cases require `VORTEX_SIFT_CAPI` and
`VORTEX_SIFT_SYSTEMD_TESTS=1`, respectively. Workflow parsing and narrow YAML
checks pass; the borrowed full strict YAML config still reports the same
pre-existing comment-spacing/empty-value issues on HEAD and the working tree.
Remote CI and ASAN were not run.

#### Local Stability Repeat, 2026-10-03

This local-only repeat reuses the exact input, binary, benchmark-source and
Provider-parity hashes from the qualified 2026-10-02 resource-controlled runs.
It changes neither search implementations nor production defaults. No paid API,
cloud resource, download, index rebuild or ingestion is involved; the driver uses
cached NumPy through `uv run --offline --no-project --with numpy python`.

Both datasets use the same original first 1,000 queries, k=10, one complete
warmup and five measured rounds under the existing CPU 8 / 2 GiB / no-swap
constraints. nprobe is frozen at 100/320 and SQL probes at 64/512 for 100K/1M.
Engine order is reversed from the previous runs: OpenData then SQL for 100K,
SQL then OpenData for 1M. The host and its SMT sibling remain unisolated; order,
run length and host conditions can all affect comparisons with the older runs.

| Workload | SQL snapshot C API p50 / p99 ms | OpenData API p50 / p99 ms | SQL / OpenData Recall@10 |
| --- | ---: | ---: | ---: |
| SIFT100K | 5.59 / 10.91 | 11.90 / 23.50 | 99.77% / 99.59% |
| SIFT1M | 16.87 / 30.34 | 39.67 / 77.80 | 99.40% / 99.46% |

Each headline worker validates 60,000 full records, including warmup, and
contains 5,000 measured calls. All repeats and SQL/Provider parity checks pass.
The earlier driver marked both reports `ok`, with the same Recall floor/gap checks
and verified live resource limits for all main and diagnostic workers, with no
logged errors or OOM events. Diagnostics remain separate from headline timing.
These are warm explicit-snapshot SQL results, not strict-default or cold-disk latencies.

SQL's 1M round p50s are 16.905/16.747/16.699/16.852/17.250 ms; the earlier
16.316-to-19.312 ms upward trend does not recur. OpenData's 1M round p50s are
39.427/39.778/40.276/39.675/39.255 ms. Stable medians do not imply stable tails:
OpenData's p99 increases from 13.76 to 23.50 ms on 100K and from 51.78 to
77.80 ms on 1M; SQL's 100K p99 also increases from 6.61 to 10.91 ms. Each
OpenData 1M round has p99 between 77.13 and 78.18 ms. These variations are not
evidence of a code regression or optimization, since binaries are unchanged;
their cause has not been established by a CPU profile.

Main-worker peak RSS / cgroup memory are SQL 140/290 MiB and OpenData 176/366
MiB on 100K, and SQL 389/862 MiB and OpenData 1,074/2,048 MiB on 1M. OpenData
1M records 3,568 memory-limit pressure events over the worker, including 14 in
the first measured round and none in the remaining measured rounds. This is
reclaim pressure, not OOM. Across five measured rounds SQL 1M issues
114,922,142,815 logical read bytes with zero kernel storage reads; OpenData
issues 805,435 logical read bytes and 4,096 kernel storage read bytes. Both
record zero measured major faults. These are whole-round counters, not isolated
search CPU/IO, but they do not support attributing the tail change to bulk disk
reads. SQL's repeated logical reads/copies and OpenData's tails warrant focused
CPU profiles before changing caching or search parameters.

Independent diagnostics again show zero measured SlateDB data/index/filter
misses, with 510,636/1,208,324 data-block hits for 100K/1M, and SQL reports
1,000/1,000 Provider and source hits for each dataset. Their different cache
semantics still apply; they do not establish identical cache implementations.

For reproduction, add `--rounds 5 --opendata-nprobe 100` and
`--order opendata sql-snapshot-capi` to the 100K example above, with a fresh output
directory. For the corresponding 1M inputs/libraries, freeze nprobe 320 and use
`--rounds 5 --order sql-snapshot-capi opendata`. Raw reports, samples and frozen
drivers are retained under
`build/index-sift100k-resource-repeat-20261003-reversed/summary.json` and
`build/index-sift1m-resource-repeat-20261003-reversed/summary.json`.

Verification for this repeat is the driver qualification plus independent JSON
checks of provenance, samples, resource phases and diagnostics, and Markdown
diff checks. Engine code and test suites are unchanged; the earlier unit tests,
Clippy and formatting checks were not rerun. No commit, push, remote CI or ASAN
run is included.

#### Posting Read/Copy Profile, 2026-10-03

This diagnostic reuses the same SIFT1M generation and qualified native, Provider
and 1-GiB-artifact-budget SQL C API binaries. It selects the original first 256
queries (the existing native/Provider harness limit), k=10, max_check=32768,
internal_results=512 and search_pages=128. All layers run sequentially with one
warmup and five measured rounds, CPU 8, one-core quota, 2 GiB memory and no swap.
No search implementation, input, index, recall parameter or production default
changes. SQL uses explicit snapshot validation and returns full original rows;
native and Provider return IDs/row addresses and distances.

The separate uninstrumented p50 / p99 results are 11.81 / 15.63 ms for direct
C++ workspace reuse, 11.56 / 15.65 ms for Provider and 14.97 / 18.89 ms for SQL.
All six baseline/profile workers return the exact previously qualified ranked
IDs and Float32 distances; both SQL workers additionally validate 15,360 full
records each. Recall@10 is 99.6875% on this subset. These numbers do not replace
the preceding 1,000-query headline results or demonstrate an optimization.

`perf record` samples user and kernel CPU at a fixed 5-ms period with 16-KiB
DWARF stacks. Temporary per-binary entry/return uprobes identify all 1,536 native
queries per worker, allowing startup, warmup and teardown to be excluded. SQL
also checks its phase-barrier monotonic timestamps against the probes. The
profiler runs outside the worker cgroup. No global perf permission setting is
changed, and probes and worker units are removed after capture.

| Warm CPU samples | Native | Provider | SQL snapshot |
| --- | ---: | ---: | ---: |
| Samples in selected window | 2,883 | 2,674 | 3,714 |
| Posting read path, inclusive | 50.99% | 49.03% | 35.38% |
| Kernel copy routines, self | 37.43% | 37.47% | 27.30% |
| AVX L2 distance, self | 14.92% | 17.54% | 11.52% |
| User-space memcpy/memmove, self | 0.03% | 0.00% | 0.48% |
| Memset, self | 5.06% | 6.06% | 4.68% |

Inclusive read samples contain the kernel-copy samples; rows must not be summed.
The observed chain is `ExtraStaticSearcher<float>::SearchIndex` ->
`SimpleFileIO::ReadBinary` -> `istream::read` -> `read` -> `filemap_read` ->
`copy_page_to_iter` -> `rep_movs_alternative`. The last instruction alone accounts
for 36.18% / 36.69% / 26.33% of native / Provider / SQL samples. The main memset
stack is the existing BKT `WorkSpace::Reset` / visited-set clearing path.

Provider stacks often stop at `SimpleFileIO` or the distance routine, so its
outer native/head/posting inclusive percentages are incomplete and are not
compared. Native and SQL resolve the complete posting read chain. Return probes
replace the caller return address above SPANN, so ancestry above that frame is
not used for attribution. SQL has one empty callchain and six unknown leaf
samples (0.16% including that empty sample). Whole-round SQL sampling includes
untimed binding and CSV output; approximately 2.93% is identifiable numeric
output formatting. Percentages describe sampled CPU, not query wall latency.

The uninstrumented SQL measured phases issue 29,285,178,680 logical read bytes
and 675,680 read calls: approximately 21.82 MiB and 528 reads per query. Kernel
storage-read bytes and major faults are both zero. The instrumented run has the
same logical read byte/call counts. Its sampled kernel CPU share (33.12%) agrees
with phase counters (6.15 system seconds of 18.57 total process CPU seconds).

A separate `strace` check uses queries 0/85/170/255 for two identical passes.
Excluding the posting header, it observes 4,094 reads and 4,094 `SEEK_SET` calls
on `postings.bin`, transferring 166,117,306 bytes. Both passes repeat exactly
the same read offsets and sizes; unique byte coverage is 75,812,830 bytes.
Results match the uninstrumented native baseline. Trace timings are excluded
from performance comparisons.

The next focused experiment is a borrowed, read-only posting view over the
verified private generation, with bounds/lifetime checks, existing memory
budgets, and a fallback for formats requiring a writable decoding buffer.
It must feed posting processing directly: implementing `ReadBinary` with mmap
followed by memcpy would retain the full-volume copy. Replacing seek/read with
pread only targets the much smaller seek portion (about 2-3% inclusive here).
Strict validation defaults, reference identity and the fixed search_pages
contract must remain unchanged. Measure an A/B on full SIFT100K/SIFT1M query
sets and verify exact results/recall before claiming a speedup. Visited-set
clearing is a secondary candidate after the reader experiment.

Raw `perf.data`, decoded stacks, query samples, syscall traces, resource phases,
binary/input hashes and local capture/analysis scripts are retained under
`build/index-posting-profile-20261003/`; `cpu-summary.json`, `results.json` and
`native-syscalls-baseline/io-summary.json` contain the structured results.
Profile-run medians are 11.18 / 10.42 / 13.92 ms, kept separately from normal
timing. The unisolated host and sequential order preclude interpreting their
lower values as negative profiler overhead or an engine improvement. This
investigation adds benchmark documentation and local diagnostic artifacts only;
engine unit tests, Rust/C++ builds and remote CI were not rerun.

## Synthetic Workload

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
| `bridge` | C ABI search, including per-call options, serialization and workspace lifecycle guards | Native open once; reuse behavior depends on the recorded binary |
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
as DuckDB engine overhead. `cpp-reuse` bypasses the bridge's lifecycle and option
guards; current bridge builds implement handle-owned reuse independently.
The older local matrices below predate that change and still reset in the bridge.

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

The measured core source was committed as
`8d5f08e5bc85663b6272fadbd81067a8028f6fd3`, containing the checksum,
layered-benchmark and explicit-snapshot changes. Vortex #15 was merged as
`dd15b32254ca9649ee1fd1925aadc9fd53b8bfc6`; the measured and merged commits have
the same Git tree. The #28 integration pinned that merged commit; the current
workspace-reuse follow-up pins its own core commit instead. Measurements used
the same DuckDB v1.5.0 SDK
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
workspace lifetime rules were unchanged in this earlier snapshot-only candidate;
its bridge still reset at call boundaries. See the newer handle-owned SIFT results
above for the separate workspace change.

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

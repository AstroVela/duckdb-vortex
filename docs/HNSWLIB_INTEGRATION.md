# Optional hnswlib SQL Integration

Local qualification on 2026-10-07, based on duckdb-vortex `57707c0`. Both adapters
pin merged Vortex #19, `114a1f2f59e6daf8377f1f2127b01652fd9448bf`. This is a formal
Git dependency build, not the earlier local-source overlay. There is no version
bump, ASAN addition, SQL projection/rebind optimization, or default budget change.
See [build switches and SQL examples](STATIC_INDEX_SQL.md#optional-hnswlib-backend).

## Build Identity

- hnswlib v0.9.0: `d9b3608c83d83b46c96e25088cb1d729b29dcfe9`, clean native headers.
- Rust 1.97.1, release, 16 codegen units, no LTO, no debug info, four build jobs.
- Reused local Vane DuckDB v1.5.0 SDK, source ID `d8a9d61d59`. The old SDK source
  directory was absent; headers were recovered from its exact Git tree
  `d8a9d61d598c103bdedc321def50ad3e55c71a26` in the local Vane repository.
- Fresh Rust adapters and shell/C API links, reusing matching DuckDB static
  engine and extension-loader objects. This is not a full engine rebuild or a
  validation of the current v1.5.5 production wheel, signing, or Ray lane.
- Both lockfiles move exactly 39 existing Vortex Git packages and add
  `vortex-index-hnswlib`. No unrelated dependency fields change; no `[patch]`.
- hnswlib-only links without OpenMP or SPFresh/zstd runtime dependencies.

Local artifact root, abbreviated as `$RUN` below:

```text
/home/kaka/duckdb-vortex-worktrees/vane-index-hnswlib-integration-20261007/build/hnsw-integration
```

`sql-{none,hnsw,spfresh,both}/build.json` records archive/shell hashes, source
identity and runtime libraries. `capi-hnsw/build.json` records the benchmark
source, link command, binary and archive hashes. `build_matrix.py` and `link.py`
retain the exact local qualification commands without adding SDK-specific paths
to the product build.

## Functional Checks

Both adapters passed release compilation for all four backend combinations, plus
`cargo clippy --locked --offline --release --all-targets --all-features --no-deps
-- -D warnings` against the matching local SDK. Logs: `build-hnsw.log` for the
initial Vane hnswlib build, `build-matrix.json` for the remaining seven builds,
and `clippy-vortex-extension{,-vane}.log`.

The CMake matrix covers both ordinary DuckDB and Vane adapters, all four backend
combinations, default-off behavior, missing native source, unsupported platform,
and hnswlib-only configuration with OpenMP discovery disabled. These configuration
tests use a stub Corrosion/DuckDB fixture; native qualification is separate.

Fresh Vane shells passed the registration matrix: no SQL index functions in the
default build, only the selected backend in single-backend builds, and both
backends available together. Unselected backends reject a valid reference for
the other backend. Raw results: `$RUN/runtime-matrix.json`.

| Native Shell | SQL Backend | Processes | Negative Cases | Fixture Recall@10 |
| --- | --- | ---: | ---: | ---: |
| hnswlib only | hnswlib.static | 87 | 51 | 1.0 |
| SPFresh only | spfresh.static | 83 | 47 | 1.0 |
| Both | hnswlib.static | 87 | 51 | 1.0 |
| Both | spfresh.static | 83 | 47 | 1.0 |

Each suite builds 4,096 eight-dimensional rows in two interleaved Vortex files.
Checks include separate-process reopen, physical row mapping, squared-L2 scores,
strict/snapshot prepared repeats, NUL rejection, invalid vector types, reference
pinning, source/artifact mutation, and execution-time filesystem/access policy.
Snapshot retains original rows and index after disk changes; fresh strict queries
still reject those changes. Results are in `regression-*/summary.json` and the
per-process logs, not a performance measurement.

hnswlib construction uses `threads=1`. Counts 0, 2, 4 and 8 are rejected without
publishing a reference or artifacts. The generic SQL build path creates its
store before invoking the backend, so an empty unsealed generation can remain
after this error. The current owner-managed cleanup contract is unchanged; this
integration does not claim automatic failed-build generation cleanup.

Default-budget controls (`controls-default/report.json`) verify SIFT100K and
SIFT1M strict results against original vectors and the frozen native results:

- SIFT100K: first Provider miss, second Provider hit; no snapshot hits in strict mode.
- SIFT1M: both strict executions miss and reopen, because its graph exceeds the
  unchanged 256 MiB retained-artifact budget.
- SIFT1M snapshot: explicit budget error, not silent fallback or a default limit
  increase. The encoded-source limit remains 512 MiB and the handle limit eight.

## Search Benchmark Method

`scripts/bench_index_hnsw_layers.py` compares one frozen graph through native
hnswlib, the public Vortex Provider, and prepared full-record SQL snapshot mode.
It validates IDs, ranks, distances, original SQL vectors, official recall,
cross-layer parity, complete rounds, artifact hashes and actual cgroup limits.
Warm-cache timing is separate from startup, strict/default-budget controls, and
cache diagnostics. It does not measure build throughput or cold-disk performance.
The driver requires Python 3.12 and NumPy, and consumes previously qualified
native binaries, build records, and imported graph references; it does not build
those native comparison artifacts itself.

SIFT100K uses 100,000 Float32 vectors, 128 dimensions, squared L2, top-10, M=32,
efConstruction=200, seed=100 and efSearch=96. The graph is 78,821,308 bytes,
SHA-256 `44b08c08ee404b2bd1dfa369c4e9d5c0968bcf64bc77c958c7ac6c8e7cc73261`.
It is the historical eight-thread-built graph used by the earlier search
comparison, imported byte-for-byte into the Vortex store. This is only a frozen
search baseline; it does not qualify the unsafe upstream parallel builder.
The current SQL builder's single-thread contract is checked separately above.

Native and Provider time ANN plus result extraction/mapping. SQL additionally
performs Vortex original-row take and extracts the full 128-dimensional vectors
through the C API. The SQL fixture retains its existing FLOAT-array parameter
and automatic rebind behavior; local parameter-type and projection optimizations
are deliberately excluded.

Three independent processes per layer, each with 1,000 warmup queries and three
measured rounds of 1,000 queries, give 9,000 hot samples per layer. Process order
alternates. Each worker gets CPU 8, one CPU quota, 2 GiB memory, no swap, and one
DuckDB/Tokio/Rayon/OpenMP worker. CPU 26 is its SMT sibling. The driver rejects
compiler overlap and excessive host/sibling activity. The controller is pinned
to CPU 9 before input loading or preflight, so its own monitoring cannot occupy
the worker core or prevent the idle check from completing. Separate diagnostics use
16 queries, one warmup and one measured round, and require the initial cache miss
followed by Provider/snapshot hits.

The Provider example is built from the pinned Vortex checkout with
`cargo +1.97.1 build --locked --release -p vortex-index-hnswlib --features native
--example bench_provider`, with `VORTEX_HNSWLIB_SOURCE` set. The SQL C API driver
is built by `bench_index_sift_end_to_end.build_capi` from the fresh shell's link
manifest and matching SDK headers, recording the unchanged 256 MiB budget.

```bash
uv run --no-project --python 3.12 --with numpy python scripts/bench_index_hnsw_layers.py \
  --input-manifest "$DATA/groundtruth-check.json" \
  --dataset sift100k --reference "$STORE/sift100k/index.json" \
  --native-index-record "$NATIVE/store/sift100k/hnswlib/index.json" \
  --native-build-provenance "$NATIVE/binary/build.json" \
  --provider "$RUN/bench_provider" \
  --sql-build-provenance "$RUN/capi-hnsw/build.json" \
  --ef 96 --queries 1000 --warmup 1 --rounds 3 --repeats 3 \
  --cpu 8 --sibling 26 --output-dir "$RUN/sift100k-repeat-qualified"
```

Use a new output directory for every run. For diagnostics, use `--queries 16
--warmup 1 --rounds 1 --repeats 1 --diagnostics`. A complete SIFT1M snapshot matrix
is not part of this default-budget qualification; it requires an explicitly
separate budget experiment, not relabeling the oversized error as a hot result.

## SIFT100K Results

Final runs: `$RUN/sift100k-repeat-qualified/report.json` and
`$RUN/sift100k-diagnostic-isolated/report.json`, both `status: ok`. The preliminary
run was superseded after fixing controller isolation. The interrupted diagnostic
preflight and the first isolated attempt (rejected by the resource launcher's old
caller-affinity assumption) are retained for debugging, not included below.

| Layer | Hot Samples | p50 ms | p95 ms | p99 ms | Recall@10 | Max Process RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Native hnswlib | 9,000 | 0.200692 | 0.258206 | 0.283837 | 0.9977 | 93.42 |
| Vortex Provider | 9,000 | 0.198624 | 0.253556 | 0.283182 | 0.9977 | 95.14 |
| Prepared SQL snapshot, full records | 9,000 | 2.917957 | 3.067126 | 3.155165 | 0.9977 | 180.74 |

All three fresh-process repeats preserve ranked result parity. Per-process hot
p50 ranges are 0.196512..0.206110 ms native, 0.196681..0.203061 ms Provider,
and 2.904517..2.950643 ms SQL. The tiny difference between native and Provider
medians is within run variation, not evidence of a faster wrapper.

Native graph open takes a median 194.94 ms; Provider verified open takes
848.04 ms. These precede their query timers. SQL opens the index on first
execution: its median first query is 909.77 ms, separate from the hot table.
SQL database open and prepare medians are 13.84 ms and 0.41 ms respectively.
These are fresh-process costs with warm filesystem caches, not cold-disk claims.

All nine timed workers passed actual CPU/memory/swap checks. No compiler overlap
was observed; global busy fraction stayed below 5.49% and the measured core's
SMT sibling below 3.05%. Every measured interval had zero kernel-accounted
storage read bytes and zero major faults. Logical reads and sample output still
occur; this is a warm-memory baseline, not an I/O scalability result.

Independent diagnostics verify an initial Provider/snapshot miss, followed by
hits, with 16/16 hits in the measured round. These represent retained handles
and sources, not cached ANN answers or posting-block cache hits.

Binary SHA-256 identities:

```text
Native:   0fbf36364149dd643b97cc6a1af2888987ca8cbbd7b661b21b8ccc3e900dde90
Provider: 4d19c9d36e6ecffa8d8efa7e889b11a40f0acb1829bc9087e5519dc2e8ee7846
SQL CAPI: eb2288bcc0161be46598e456c073ce2c38290bbe707b29385820f3cbf3a29167
Driver:   e96dd2e9224a704d7afc59b70ddeee7ac5f3d5d258b8b3725ac5a15cc2efbe69
```

Most remaining hot latency is above the ANN kernel. This comparison includes
SQL binding/execution, original-row take and full-vector C API extraction; it
does not isolate their individual costs. The separate SQL rebind/projection
optimization should be evaluated against this formal-dependency baseline.

## Other Verification

- 337 focused pytest cases passed with real C API and systemd/cgroup tests
  enabled, no skips (`python-tests-isolated.log`). Four driver-controller
  affinity tests cover preferred/fallback CPUs, no separate CPU, and isolation
  before input processing. Resource tests verify independent controller/worker
  affinity, restoration after success or spawn failure, and invalid CPU rejection.
- 10 provider-release tooling tests passed, including the source-pin gate.
- Both adapters pass nightly Rustfmt; the changed benchmark Python scripts/tests
  pass Black and Ruff with Python 3.12 as the target (excluding `EXE001`, matching
  existing non-executable scripts).
- Both changed workflow files parse correctly. Strict YAML lint with the Vortex
  monorepo configuration still reports 62 existing diagnostics in
  `VaneExtension.yml` and 15 in `VaneIntegration.yml`; a structured comparison
  against the base confirms no new diagnostics. No unrelated formatting churn.
- No full workspace test, ASAN job, production wheel build, remote CI run,
  signing/publishing or Ray integration run was performed for this local change.

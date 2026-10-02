# Experimental Static Index SQL

This opt-in path composes `vortex-duckdb/index` with `vortex-index-spfresh/native`.
SQL bindings use the common `IndexBuilder`, `IndexProvider`, `IndexStore`, and
`IndexSource::take` contracts. SPFresh is a registered implementation, not a
second index catalog or an independent C++ SQL bridge.

## Build

The default extension build is unchanged. Static SPFresh currently requires
Linux x86_64, a qualified PIC native build, and the matching Vortex revision.
Build the native dependencies in the pinned Vortex checkout:

```bash
bash vortex-index-spfresh/native/build.sh
export VORTEX_SPFRESH_NATIVE="$PWD/target/spfresh-native/build"
```

Pass these options to the ordinary DuckDB or the separately invoked Vane CMake
build described in the project guides:

```text
-DVORTEX_ENABLE_INDEX_SPFRESH=ON
-DVORTEX_SPFRESH_NATIVE=/absolute/path/to/vortex/target/spfresh-native/build
```

The Rust manifests enable `index-spfresh` only for this option. Native revision
and patch stamps are checked by the backend's build script. There is no SPDK,
RocksDB, online update support, or sanitizer addition in this integration.

## SQL

Create `/indexes` first and provide the exact ordered file inventory. Every
file must have the same schema, and all rows must have finite Float32 fixed-size
vectors without NULL rows or elements. Nullable schema markers are permitted
after actual values are checked. The reference filename must not already exist.

```sql
SELECT * FROM vortex_index_build(
    ['/data/even.vortex', '/data/odd.vortex'], '/indexes/embedding.json',
    'embedding', 'spfresh.static',
    '{"format_version":1,"dimension":8,"head_count":64,"posting_page_limit":12,"replicas":4}'
);

SELECT rank, file_id, row_offset, distance, "row".id, "row".label
FROM vortex_index_search(
    '/indexes/embedding.json', [1,2,3,4,5,6,7,8]::FLOAT[], 10
)
ORDER BY rank;
```

Building runs on execution, not bind or EXPLAIN. The owner seals and qualifies
the completed generation before exclusively publishing a reference with the
expected source snapshot/schema and manifest digest. A separate process can
reopen that reference. `file_id` starts at one in file-list order; `row_offset`
is a physical file-local row offset, not an application ID. The original `row`
is fetched in candidate order, and `distance` is squared L2.

Optional query `backend_options` is JSON owned by SPFresh. All three fields are
required when supplied: `max_check`, `internal_results`, and `search_pages`.
`search_pages` must equal the build's `posting_page_limit`; lowering it is
rejected. SPFresh supports `k <= 4096` and its existing resource budgets apply.

This first implementation is synchronous local SQL, not Ray index execution.
It requires full coverage, enabled external access, and local filesystem access.
Disabling `LocalFileSystem` rejects both build and search, including already-bound
prepared statements. NUL bytes are rejected in every string argument before any
index I/O. The default `validation_mode := 'strict'` verifies files, manifest
and artifacts on every execution;
replacing or removing a source fails, and prepared queries reject changed
reference bytes. WHERE applies after candidate retrieval and is not filtered
top-k. Exact distance-ordering queries are not rewritten to ANN. There is no
mutable table catalog. Build publication is external to DuckDB transactions, so
rollback does not undo it. Owners manage unreferenced generations and crash leftovers.

## Prepared Provider Reuse

With a companion Vortex revision supporting prepared-provider reuse, a prepared
statement can retain the generic `Arc<dyn Index>` handle for its bound reference.
SPFresh is one implementation of that interface; SQL does not introduce a native
SPFresh cache API. Preparing still performs no provider open. Execution can retain
the first successfully opened provider, and later executions of the same owner
can reuse it, including parameter and catalog rebinds.

```sql
PREPARE nearest AS
SELECT rank, distance, "row".id
FROM vortex_index_search('/indexes/embedding.json', $1::FLOAT[], 10)
ORDER BY rank;

EXECUTE nearest([1,2,3,4,5,6,7,8]::FLOAT[]);
EXECUTE nearest([2,3,4,5,6,7,8,9]::FLOAT[]);
DEALLOCATE nearest;
```

In the default strict mode, reference identity, source files, manifest, artifacts,
and DuckDB access policy are still verified on every execution before a cached
handle is used. Strict mode has no validation-byte or source-reader cache.
Independent prepared owners and
connections do not share handles. Deallocation or C API handle destruction
releases the owner's retained handles and private 0700 scratch directories;
native handles close before scratch is removed.

Each connection may retain at most eight handles with a combined 256 MiB of
sealed artifact bytes. This is not an RSS or native-heap bound, and it does not
raise existing builder or per-search resource limits. A full budget or oversized
generation falls back to the uncached open/search/close path in strict mode.
Failed opens release their reservation and scratch. Ad-hoc strict statements
remain uncached.

Choosing prepared SQL alone does not enable reuse in an older pinned artifact.
Align the outer dependency with the companion Vortex revision before building;
diagnostic events expose `provider_cache_hit` when this implementation is present.

## Explicit Snapshot Mode

With a snapshot-capable companion Vortex revision, the separate named option
`validation_mode := 'snapshot'` retains both the verified `LocalFileSource` and
the generic provider index for the original prepared owner. It does not belong
in the provider's `backend_options` JSON. The strict default is unchanged.

```sql
PREPARE nearest_snapshot AS
SELECT rank, distance, "row".id
FROM vortex_index_search(
    '/indexes/embedding.json', $1::FLOAT[], 10,
    validation_mode := 'snapshot'
)
ORDER BY rank;

EXECUTE nearest_snapshot([1,2,3,4,5,6,7,8]::FLOAT[]);
EXECUTE nearest_snapshot([2,3,4,5,6,7,8,9]::FLOAT[]);
DEALLOCATE nearest_snapshot;
```

PREPARE still performs no provider/source open. The first execution fully validates
the source, sealed manifest and artifacts before opening them. A successfully
opened index and its verified source are retained even if subsequent query work
fails; failed verification or provider open retains nothing. Later executions
use the same in-memory source bytes and index, including original-row fetching.
Replacing, corrupting or deleting the original source, manifest or artifacts
after that open does not update or invalidate the retained view. This is not
filesystem-atomic capture or DuckDB transactional
snapshot isolation, and writers must remain stopped during initial verification.

Every execution still checks reference identity and DuckDB access policy;
disabling external access or `LocalFileSystem` rejects even a retained snapshot.
Changing the reference is rejected, not silently adopted. Publish a new generation
and reference, then prepare a new owner to accept and validate it. Query vectors,
`k` and backend options may vary without changing the retained source. Mode names
are case-sensitive, and NULL, unknown values and NUL bytes are rejected.

Snapshot and strict scans have distinct cache keys. A strict scan in an owner
containing a snapshot scan still performs full validation. Independent prepared
owners and connections do not share handles or sources. C API handles and SQL
PREPARE owners have the same lifetime rules; ad-hoc statements do not share a
snapshot across independent queries. A live prepared owner is required at
execution. Deallocation releases retained bytes, handles and scratch.

The existing aggregate eight-handle and 256 MiB artifact-byte limits apply to
both modes. A connection may additionally retain at most 512 MiB of encoded
source bytes. These are retained input budgets, not RSS bounds: decoded arrays,
metadata, native heaps and transient initial verification consume additional
memory. Snapshot mode reports an explicit budget error instead of falling back
to rereading a potentially different view. Dropping an owner frees its capacity;
failed provider opens also release reservations and scratch. The strict mode's
uncached fallback remains unchanged. Native workspace reuse is not enabled.

Diagnostics add `validation_mode` and `snapshot_cache_hit`; the first execution
is a snapshot miss, and subsequent retained executions are hits. See the
[benchmark results](INDEX_SQL_BENCHMARK.md#explicit-snapshot-comparison) for the
first-call cost and hot-query comparison; do not treat hot latency as cold-disk
or production-wheel performance.

## Qualification

Run the regression script with a newly built native shell:

```bash
python3 scripts/test_index_sql.py \
  --duckdb /absolute/path/to/build/duckdb \
  --output-dir /absolute/path/to/a/new/test-directory
```

For a shell without built-in Vortex, add `--extension /path/to/vortex.duckdb_extension`.
It must use the same engine ABI; the script permits loading the unsigned local
artifact. The test builds two interleaved source files, exits, queries from fresh
processes, compares original rows/squared-L2 scores, measures recall against
exact search, and checks repeat/prepared equality and invalid/stale inputs.
Per-process logs, CSV results and `summary.json` are retained. This bounded
fixture is a correctness qualification, not a performance benchmark.

For repeatable same-process performance comparisons and phase diagnostics, see
[Static index SQL benchmark](INDEX_SQL_BENCHMARK.md).

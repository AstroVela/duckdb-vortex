# SPFresh native batch search on Vane

The optional `VORTEX_ENABLE_SPTAG_SPFRESH` build adds
`vortex_spfresh_search_batch` and `vortex_spfresh_search_batch_blob` to the
Vane Vortex extension. Both use the SPFresh C++ implementation through the
ported C ABI bridge in `src/spfresh/`. Vortex file scans and COPY retain the
AstroVela implementation and the branch's locked Rust dependencies.

## Source identities

- Extension baseline: AstroVela/duckdb-vortex `v1.5-variegata_vane`,
  `124a8d4102ce4aad52972c502710a554b28db2f5`.
- Vortex dependency: AstroVela/vortex
  `3da8a2848b5d10d028e69471c5c97bd3dc785a03`. Its complete source tree equals
  `duckdb/v1.5-variegata_vane` at `f368699df218798ae5aaa345c851970a50a64907`.
- Vane engine: the existing `vane-extension.toml` pin,
  `79049f382ba6ee79d035c09cc8b5d3538e5bbe6a` (v0.2.0).
- Local qualification uses SPFresh/SPFresh `5893eb61ee3b18610b6b00f1939be7dae1af8904`
  with the existing local build, SPDK memory implementation, and search fixes.
  An upstream-only build has not been qualified with these fixtures.

## Build

Use the existing Vane source-build prerequisites, including its Arrow/Flight
dependencies and Rust 1.97.1. In the Vane CMake build, pass:

```text
-DDUCKDB_EXTENSION_CONFIGS=<extension>/extension_config_vane.cmake
-DVORTEX_ENABLE_SPTAG_SPFRESH=ON
-DVORTEX_SPTAG_HOME=<built-SPFresh-checkout>
```

SPFresh must supply `libssdservingLib.a`,
`libSPTAGLibStatic.a`, and `libDistanceUtils.a` under `Release/`, plus shared
zstd, TBB, NUMA, and OpenMP. Shared zstd avoids duplicate symbols from the
Rust archive's embedded zstd. The feature defaults to OFF. Build the `shell`
target to obtain a native engine with both Vortex IO and SPFresh SQL functions.

For a loadable extension, all SPFresh archives must be built with `-fPIC`
(for example, `-DCMAKE_POSITION_INDEPENDENT_CODE=ON` in their CMake build).
The current local archives support the native shell but are not PIC; the
`vortex_loadable_extension` link fails with `R_X86_64_PC32` until they are
rebuilt. A distributable provider package has not been qualified.

## SQL contract

The table path must refer to a local directory with the existing
`indexes/_index_catalog.json` and the registered index's version-1
`vortex-sptag-spfresh-store` manifest. Existing registrations from the
prototype are supported. Catalog and manifest dimensions, vector counts,
and metrics must agree. The native store must be immutable while queries
run; publish a changed manifest when replacing an index.

```sql
SELECT query_id, id, distance
FROM vortex_spfresh_search_batch(
    '/absolute/table/path', 'index_name', '[[1,2,3],[4,5,6]]', 10,
    spfresh_backend := 'ssdserving_lib',
    spfresh_max_check := 4096,
    spfresh_internal_result_num := 64,
    spfresh_posting_page_limit := 16
);
```

Each example vector must match the registered dimension. The BLOB variant
takes `(table_path, index_name, queries_blob, query_count, dim, k)` followed
by the same named parameters. The BLOB is contiguous native-endian float32
data, with no header. IDs are native SPFresh IDs. Results are not guaranteed
to be sorted without SQL ORDER BY.

One native index is cached per connection. Metadata and configuration are
reread at execution, including for prepared queries; changing either or the
search options replaces the cached handle. Queries and native calls are
serialized within the connection. NULL arguments, non-finite vectors,
invalid shapes, and disabled external access are rejected.

This implements local, in-process read-only searches over registered stores.
SPFresh index creation, registration/update/flush SQL functions, and Ray
distributed search callbacks are not part of this port yet. Existing Vortex
distributed scans and COPY retain their branch behavior. The benchmark's
SPDK memory implementation measures in-memory search, not NVMe performance.

## Qualification

The test uses an existing parity benchmark JSON as its native C ABI
reference and the same SpaceV Int8 query file:

```bash
python3 scripts/test_spfresh_native.py \
  --duckdb /path/to/build/duckdb \
  --reference-summary /path/to/previous/summary.json \
  --queries /path/to/query.i8bin \
  --query-count 100 \
  --output-json /path/to/qualification.json
```

Set the same SPFresh storage environment used to produce the reference.
For the local memory implementation this includes `SPFRESH_SPDK_USE_MEM_IMPL=1`
and `SPFRESH_SPDK_MEM_FILE=<store>/ssdmapping_postings`.

The test checks a Vortex COPY/read round trip, exact ID and distance parity
against native results, JSON/BLOB agreement, repeated searches, prepared
BLOB execution, recall against recorded exact truth, and invalid inputs.
It reports correctness, not a latency benchmark.

Local qualification on 2026-09-28 passed for both 100 and 1,000 queries over
the existing updated SpaceV store (100,000 active vectors, 100 dimensions,
k=10). IDs and distances matched the recorded direct C ABI results exactly;
recall was 0.955 and 0.9433 respectively. Both runs passed Vortex IO,
JSON/BLOB/repeated/prepared execution, and ten negative-input checks. The
1,000-query run also exercised output spanning multiple DuckDB data chunks.
The shell and JSON reports are retained under `build/spfresh-validation/`
in the integration worktree.

# Vane integration and qualification

[Usage guide](../VANE_README.md) · [Project overview](../README.md)

### Vane build lane

The Vane build is independent from the ordinary DuckDB extension build. It is
described by `vane-extension.toml`, uses `extension_config_vane.cmake`, and
compiles against `external/duckdb` from the exact Vane commit recorded in the
manifest. The manifest also pins the exact vcpkg revision used by both local
and hosted Vane builds. The ordinary `make` and `make test` commands continue
to use the repository's `duckdb/`, `extension-ci-tools/`, and
`extension_config.cmake`.

Initialize the Vane CI tooling and validate the exact source identities:

```sh
git submodule update --init vane-extension-ci-tools
make vane_validate
make vane_identity
make vane_verify_vcpkg
```

Run the Vane-native extension lane with the repository's vcpkg toolchain:

```sh
export VCPKG_TOOLCHAIN_PATH="$PWD/vcpkg/scripts/buildsystems/vcpkg.cmake"
make vane_ci
```

On 64-bit x86 Linux, build and verify the statically linked Vane wheel with:

```sh
make vane_wheel
```

All Vane targets use the `vane_` prefix and fail if the Vane tooling, exact
source revision, distributed headers, or required build inputs are absent.

The Vane lane selects `vortex-extension-vane/Cargo.toml`; the ordinary lane
continues to select `vortex-extension/Cargo.toml`. Both manifests pin
`AstroVela/vortex@3da8a2848b5d10d028e69471c5c97bd3dc785a03`, which includes the
common DuckDB-filesystem-backed Vortex writer and adapts the distributed
bind-data enum to the current Vane SDK. The Vane integration manifest
also pins
`AstroVela/vane@d1460a580455f01485e2e508e05d0049cb18a105`
(`vane-ai==0.2.0.dev663`). CMake explicitly
sets `VORTEX_VANE_DISTRIBUTED=1` only for this lane; both Rust adapters
translate it to `#[cfg(vortex_vane_distributed)]`, while C++ uses the matching
`VORTEX_VANE_DISTRIBUTED` definition. The filesystem writer is intentionally
shared so ordinary DuckDB and Vane can both use connection-scoped httpfs
settings and secrets. All Vane scan/COPY protocol code remains conditionally
compiled, and no ordinary DuckDB source, submodule, or registration path is
replaced.

The Vane loader calls one exported Rust shim for both runtime initialization
and catalog registration. The companion C++ registrar remains internal to the
Rust artifact, so the same entry point works with staticlib and cdylib builds.

### Independent Vane provider

`VaneExtension.yml` builds and qualifies the separate
`vane-extension-vortex` wheel, with exact `vane-ai==0.2.0.dev663` dependencies
for CPython 3.10–3.14 on manylinux_2_28_x86_64. This profile statically embeds
Rust and DuckDB inside the dynamic artifact but does not link Vortex into the
base Vane wheel; only `vortex_duckdb_cpp_init` is publicly exported.

See [Vane provider release](VANE_RELEASE.md) for installed local/two-worker
qualification, fixed source identities, first-publisher configuration and the
protected manual TestPyPI workflow. A separate production manifest prepares
TestPyPI staging and same-byte PyPI promotion after an actual production-key-aware
Vane runtime is released. Signing, packaging and publishing use isolated jobs;
the dev663 contract stays unchanged. Adding the workflow does not itself publish
a package; the existing native/static qualification remains independent.

### Vane distributed Vortex scans and COPY

Vane registers distributed callbacks for both `read_vortex` and
`vortex_scan`, including the `VARCHAR` and `LIST<VARCHAR>` overloads. Planning
turns the coordinator's fully bound file listing into immutable owned state,
serializes that state for workers, and creates one split per bound file. A
worker opens exactly the files named by its assigned splits; it never expands
the original path or glob again. The split identity includes the scan UUID,
stable file index, canonical source URL and object path, and the file's byte
length plus its storage ETag and/or object version. A missing file, changed
object identity or byte length, duplicate split, unknown split, foreign scan
UUID, or non-canonical payload fails the query.

A fully pushed-down final aggregate is planned as one indivisible split over
the complete pruned file set because independent final aggregates cannot be
combined by this protocol. Empty scans and empty worker assignments are valid.
Vortex late materialization is disabled in Vane builds because its second scan
would require coupled split assignment.

The file-oriented write path uses DuckDB's normal `COPY ... (FORMAT VORTEX)`
plan, including `relation.write_file(..., format="vortex")`. The Vane-only bind
clone and serializer carry owned column names and logical types. After worker
plan deserialization, Vane assigns a unique run/task/attempt path; the Vortex
writer opens that actual path in `init_global` and returns the exact path, row
count, and closed-file byte size. No writer, stream, filesystem, or execution
context is serialized.

Workers only create immutable attempt artifacts. The coordinator selects one
successful attempt per partition, records only those files in the manifest,
removes visible unselected attempts, and writes the committed marker last. The
marker is the authoritative publication boundary; readers must never infer a
committed dataset from files visible under the output prefix. Empty input
publishes a valid readable zero-row Vortex file through the same protocol.

| Capability | Vane behavior |
| --- | --- |
| Distributed `read_vortex` / `vortex_scan` | Enabled |
| Local `COPY ... (FORMAT VORTEX)` | Enabled |
| Distributed Vortex COPY on shared POSIX storage | Enabled |
| Distributed Vortex COPY on S3-compatible storage | Enabled |
| Sub-file/row-group splitting | Not implemented; tracked by #9 |

Worker metadata checks and every subsequent range read are pinned to the
coordinator-selected identity. Versioned stores read the selected object
version; ETag-protected stores reject an identity-changing replacement even
when its byte length is unchanged. A backend that provides neither a version
nor an ETag is rejected during bind, with no path-and-size fallback. Every
worker must be able to access the same canonical URLs with equivalent
credentials.

For local output, every worker and the coordinator must see the same POSIX
namespace. For S3-compatible output, workers write unique immutable object
keys through DuckDB's httpfs filesystem, using the connection settings and
credentials replayed by Vane. Credentials are not part of the Vortex bind,
result metadata, manifest, marker, or object names. Before publication, a
failed run can be cleaned and retried under a fresh run/attempt identity. Once
manifest publication has started, automatic failure handling retains
artifacts for diagnosis; an explicit coordinator reconciliation may remove
them only after proving that no committed marker exists. A committed run is
never removed by uncommitted-run cleanup, and coordinator-finalize replay
reads the existing manifest idempotently.

Run the focused protocol and lifecycle qualification under ASAN/LSAN with:

```sh
make vane_scan_lifecycle VANE_BUILD_JOBS=8
```

This Vane-only target uses a separate Debug build directory. Its scan cases
repeatedly serialize and reconstruct assigned plans through short-lived
connections, destroy an assigned plan to model a lost task, replay it in a
fresh worker context, execute the same prepared worker plan twice, and
distribute file assignments across independent DuckDB instances. Its COPY
cases exercise transient-context plan cloning, independent workers, injected
failure and retry, winner/loser selection, exact statistics, committed-manifest
readback, empty output, pre-publication cleanup, and conservative uncertain
publication handling. ASAN and LSAN fail the target on use-after-free, invalid
destruction, or leaked lifecycle state. The hosted Vane workflow runs the same
target in a dedicated read-only CI job.

The hosted workflow then downloads the one verified `vane-vortex-wheel`
artifact into two independent clean virtual environments. The default Ray gate
and the two-execution-node Ray gate run from test directories outside the
checkout with Python isolated mode enabled. Both reject source-tree imports,
disable DuckDB extension auto-install and auto-load, require Vortex to report
`STATICALLY_LINKED`, and log the wheel checksum together with the exact Vane
and DuckDB binary identities. Ray workers inherit that installed wheel before
the cluster starts; they do not install, compile, or download extension code.

The packaged Ray qualification covers single, list, and glob scans; schema and
content; projection, filter, aggregate, and `file_index` pruning; empty input;
repeated relation-plan execution; and comparison of SQL and Relation APIs against
independent Python expectations. It also kills a real Vane worker actor while a Vortex split is
running and requires the replacement worker to finish attempt 1 with the exact
replayed result. Distributed COPY covers multiple selected worker files, an
empty output, failed-task cleanup and explicit retry, committed-manifest
readback, and exclusion of a visible file not selected by that manifest. The
same Ray job writes to an isolated MinIO service and injects upload, metadata,
manifest, and committed-marker failures. It verifies immutable retry keys,
conservative reconciliation, credential redaction, and exact downloaded
readback. The ordinary distribution workflow separately loads its packaged
native extension into DuckDB 1.5.5 and proves an S3 write authorized only by a
temporary DuckDB secret.

| Packaged qualification | Status |
| --- | --- |
| Local-fast Vortex scan and COPY from the installed wheel | Supported |
| Two-node Ray Vortex scan and shared-POSIX COPY from the same wheel | Supported |
| Real actor-loss scan replay | Supported |
| COPY failure cleanup, retry, selected manifest, and exact readback | Supported |
| Native DuckDB Vortex COPY to S3-compatible storage | Supported |
| Two-node Ray Vortex COPY to S3-compatible storage | Supported |
| Object-store publication failure injection | Supported |
| Sub-file/row-group Vortex scan splitting | Deferred to #9 |

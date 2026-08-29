# Vortex DuckDB

Rust bindings for the Vane-provided DuckDB source tree. This vendored package
requires `DUCKDB_SOURCE_DIR`, `DUCKDB_VERSION`, and its `vane` feature; it does
not download, select, or build a different DuckDB distribution.

## Vendored source

This directory is based on `vortex-duckdb` from
[`vortex-data/vortex@7e06a99bb7772087c9546137ea6f4593235426a6`](https://github.com/vortex-data/vortex/tree/7e06a99bb7772087c9546137ea6f4593235426a6/vortex-duckdb).
It is vendored because Vane's explicit scan protocol must serialize and restore
the reader's private Rust bind state without copying that layout into DuckDB or
Vane core. All distributed registration, bind serialization, task filtering,
and worker-reader reconstruction are gated by the `vane` Cargo feature. The
enclosing repository selects this crate only for its separately configured Vane
build; ordinary DuckDB builds use `vortex-extension/Cargo.toml` and the upstream
Git dependency instead.

When refreshing this snapshot, start from the exact upstream directory named
above and preserve the Vane-only delta under `#[cfg(feature = "vane")]` and
`VORTEX_VANE_DISTRIBUTED`. Keep the protobuf and split formats versioned, keep
bound file sizes mandatory, and regenerate both `include/vortex.h` and the
enclosing `vortex-extension-vane/Cargo.lock`. Do not copy generated `cpp.rs`
bindings into the repository.

## Build and validation

Do not build this package as an independent DuckDB distribution. The enclosing
Vane CMake lane selects the exact Vane DuckDB source, forwards its source path
and fork version, enables the `vane` feature, and links the resulting adapter
into the extension. The committed workspace lockfile is shared with cbindgen's
metadata pass so dependency resolution is identical in clean and incremental
builds.

This vendored package intentionally omits the upstream workspace-only
development dependencies. Validate it through the enclosing `duckdb-vortex`
native, Vane protocol, sanitizer, and Ray integration targets documented in the
repository-level README.

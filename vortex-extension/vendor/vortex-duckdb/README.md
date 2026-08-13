# Vortex DuckDB

Rust bindings for DuckDB. Supports DuckDB precompiled libraries for fast builds and from source builds for debugging.

## Vendored source

This directory is based on `vortex-duckdb` from
[`vortex-data/vortex@7e06a99bb7772087c9546137ea6f4593235426a6`](https://github.com/vortex-data/vortex/tree/7e06a99bb7772087c9546137ea6f4593235426a6/vortex-duckdb).
It is vendored because Vane's explicit scan protocol must serialize and restore
the reader's private Rust bind state without copying that layout into DuckDB or
Vane core. All distributed registration, bind serialization, task filtering,
and worker-reader reconstruction are gated by the `vane` Cargo feature. With
that feature disabled, the upstream C API registration, multi-file/glob scan,
late-materialization, and filter/projection paths remain in use.

Three compatibility and safety corrections are intentionally shared by both
build lanes: generated bindings detect whether the selected DuckDB C API
contains GEOMETRY/VARIANT, statistics use the matching DuckDB statistics type
and release returned values on every path, and an empty projected-column vector
is converted to Rust's empty slice without calling `from_raw_parts` on a null
pointer. None of these shared corrections enables or applies Vane task state in
an ordinary DuckDB build.

## Prerequisites

- **Ninja**: `brew install ninja` (macOS) | `apt-get install ninja-build` (Ubuntu)
- **CMake**: `brew install cmake` (macOS) | `apt-get install cmake` (Ubuntu)
- **C++20 compatible compiler**: GCC or Clang

## Build Modes

### Default (Release)

Link against the precompiled DuckDB release build.

```bash
cargo build -p vortex-duckdb
```

### Debug Build

Opt into DuckDB debug build: `VX_DUCKDB_DEBUG=1`.

```bash
VX_DUCKDB_DEBUG=1 cargo build -p vortex-duckdb
```

### AddressSanitizer & ThreadSanitizer

Enable both ASAN & TSAN: `VX_DUCKDB_SAN=1`.

```bash
VX_DUCKDB_DEBUG=1 VX_DUCKDB_SAN=1 cargo build -p vortex-duckdb
```

## Environment Variables

| Variable          | Effect                          |
| ----------------- | ------------------------------- |
| `VX_DUCKDB_DEBUG` | Build from source in debug mode |
| `VX_DUCKDB_ASAN`  | Enable AddressSanitizer         |

## Running Tests

This vendored package intentionally omits the upstream workspace-only
development dependencies, so it is not a standalone test workspace. Validate
it through the enclosing `duckdb-vortex` native, Vane protocol, sanitizer, and
Ray integration targets instead.

## Testing the extension with DuckDB

By default, our tests use a precompiled build which means you don't get an
.extension which you can load in DuckDB. If you want to test a full setup,

1. Clone [duckdb-vortex](https://github.com/vortex-data/duckdb-vortex)
   repository.

2. If there is an api difference between duckdb-vortex's duckdb submodule and
   vortex's vortex-duckdb/duckdb submodule, checkout duckdb-vortex to previous
   commit. For example, if duckdb-vortex's HEAD uses 1.6 API but vortex's HEAD
   uses 1.5.2, checkout duckdb-vortex at 8a41ee6ebd9.

3. Update duckdb-vortex's submodules. Replace vortex/ submodule by a softlink to
   your local vortex repository.
4. Inside duckdb-vortex, run make -j.

./target/release/duckdb will be a duckdb instance with vortex-duckdb already
loaded.

## Testing a custom DuckDB tag

Change `DUCKDB_VERSION` environment variable value to a preferred hash or commit
(local build), or change build.rs (for testing in CI).

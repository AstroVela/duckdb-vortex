# Vortex DuckDB

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship
your own DuckDB extension.

---

## Building

### Install required system dependencies

#### MacOS

```shell
brew install pkg-config
```

### Managing dependencies

DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow
the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:

```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an
extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example
extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not
work without removing the dependency.

### Build steps

Now to build the extension, run:

```sh
make
```

The extension CMake requires `DUCKDB_VERSION` to be set such that Vortex can build against a pinned version. 
When building outside of CI, a version can be passed as `-DDUCKDB_VERSION=<duckdb-version>` to CMake.

The main binaries that will be built are:

```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/vortex_duckdb/vortex_duckdb.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `vortex_duckdb.duckdb_extension` is the loadable binary as it would be distributed.

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
continues to select `vortex-extension/Cargo.toml`. The Vane manifest pins the
companion adapter commit
`AstroVela/vortex@090a557d0dc692dcd39b4b5823f2c06aa870fc83`, based on the same
`vortex-data/vortex@7e06a99bb7772087c9546137ea6f4593235426a6` revision used by
the native manifest. CMake explicitly sets `VORTEX_VANE_DISTRIBUTED=1` only
for this lane; both Rust adapters translate it to
`#[cfg(vortex_vane_distributed)]`, while C++ uses the matching
`VORTEX_VANE_DISTRIBUTED` definition. No ordinary DuckDB source, submodule,
manifest, or registration path is replaced.

The Vane loader calls one exported Rust shim for both runtime initialization
and catalog registration. The companion C++ registrar remains internal to the
Rust artifact, so the same entry point works with staticlib and cdylib builds.

### Vane distributed Vortex scans

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

The current scope is deliberately limited to distributed reads:

| Capability | Issue #4 behavior |
| --- | --- |
| Distributed `read_vortex` / `vortex_scan` | Enabled |
| Local `COPY ... (FORMAT VORTEX)` | Enabled |
| Distributed Vortex COPY | Not registered; tracked by #6 |
| Sub-file/row-group splitting | Not implemented; tracked by #9 |

Worker metadata checks and every subsequent range read are pinned to the
coordinator-selected identity. Versioned stores read the selected object
version; ETag-protected stores reject an identity-changing replacement even
when its byte length is unchanged. A backend that provides neither a version
nor an ETag is rejected during bind, with no path-and-size fallback. Every
worker must be able to access the same canonical URLs with equivalent
credentials.

To build and run the focused protocol executable after the Vane-native build:

```sh
cmake --build build/vane-native --target vortex_distributed_protocol_test --parallel
./build/vane-native/extension/vortex/vortex_distributed_protocol_test
```

## Running the extension

To run the extension code, simply start the shell with `./build/release/duckdb`.

### Writing a file

To write a table to a vortex file use `COPY .. TO '...' (FORMAT VORTEX)`:

```sql
COPY (SELECT * from generate_series(0, 4)) TO 'FILENAME.vortex' (FORMAT VORTEX);
```

This will create a compressed vortex file from the sql table.

### Reading a file

To read a table from a vortex file:

```sql
select * from read_vortex('FILENAME.vortex');
```

This command also supports glob syntax e.g. `read_vortex('FILE_WITH_GLOB*.vortex')`.

## Running the tests

Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL
tests in `./test/sql`. These SQL tests can be run using:

```sh
make test
```

### Installing the deployed binaries

To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:

```shell
duckdb -unsigned
```

Python:

```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions': 'true'})
```

NodeJS:

```js
db = new duckdb.Database(":memory:", { allow_unsigned_extensions: "true" });
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the
extension
you want to install. To do this run the following SQL query in DuckDB:

```sql
SET
custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```

Note that the `/latest` path will allow you to install the latest extension version available for your current version
of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:

```sql
INSTALL
vortex_duckdb
LOAD vortex_duckdb
```

## Debugging

To build the extension in debug mode, run:

```sh
make debug
```

This will create debug binaries in the `./build/debug` directory, which can be used with a debugger for troubleshooting and development.

```sh
./build/debug/duckdb -unsigned
RUST_BACKTRACE=1 ./build/debug/duckdb -unsigned
lldb -- ./build/debug/duckdb -unsigned
```

## Building shared Vortex library

```sh
~/duckdb-vortex make EXT_FLAGS='-DUSE_SHARED_VORTEX=1' reldebug -j
```

## Cherry-pick workflow

When a PR is merged to `main`, the cherry-pick workflow automatically applies the squash commit to the current release branch.

The current release branch is configured in `.github/workflows/CherryPick.yml` via the `RELEASE_BRANCH` env var.

## Changing the native Vortex version

The Vortex version is defined in `vortex-extension/Cargo.toml`. It can be a git commit, tag, branch or even a local path:

```toml
vortex-duckdb = { path = "<path/to/vortex/vortex-duckdb>"}
```

See the Cargo docs for [git](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies-from-git-repositories) or [path](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-path-dependencies) dependencies for full details.

The Vane lane is intentionally pinned separately. Its adapter changes live on
an AstroVela/vortex branch based on the exact native Vortex revision and remain
behind the explicit `VORTEX_VANE_DISTRIBUTED` build mode. To update it, review
and merge the companion Vortex change first, pin its immutable commit in
`vortex-extension-vane/Cargo.toml`, then regenerate
`vortex-extension-vane/Cargo.lock`. Validate both the ordinary DuckDB lane and
the Vane lane before merging the update.

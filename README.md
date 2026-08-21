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

### Building against Vane

The normal `make`, `make test`, and packaging targets continue to use the
upstream `duckdb/` and `extension-ci-tools/` submodules. Vane is an additional,
explicit build lane provided by `vane-extension-ci-tools/`:

```sh
git submodule update --init --recursive
make vane_validate
make vane_ci \
  VCPKG_TOOLCHAIN_PATH="$PWD/vcpkg/scripts/buildsystems/vcpkg.cmake"
```

On Linux x86-64, the same exact pins can produce and verify a Vane wheel with
Vortex statically linked:

```sh
make vane_wheel \
  VCPKG_TOOLCHAIN_PATH="$PWD/vcpkg/scripts/buildsystems/vcpkg.cmake"
```

`vane-extension.toml` pins the exact Vane revision used by this lane. Local
targets prepare a clean checkout under `build/vane-source`; the reusable CI
workflow checks out the same revision in its isolated workspace. Both compile
against that checkout's `external/duckdb` and never use the ordinary `duckdb/`
submodule. The checked-in `vane/` gitlink is pinned to the same revision for
direct development and test support, but is not silently trusted as the CI
source.

The Vane lane uses the additive `vane_extension_config.cmake`; the existing
`extension_config.cmake` remains the configuration for ordinary DuckDB. The
Vane-specific table-function registration and distributed scan callbacks are
enabled only by that additive config after it verifies Vane's distributed
table-function header; ordinary DuckDB builds retain the existing C API
registration path. The Vane CI workflow validates the native lane, builds and
verifies the statically linked wheel, then runs the Vortex scan and retry tests
against a local two-node Ray cluster. It does not download or install a Vortex
extension at worker runtime.

The current elementary split granularity for row-producing scans is one
already-bound Vortex file. If DuckDB pushes a final aggregate completely into
Vortex, its complete pruned file set is instead kept in one indivisible split;
splitting it would produce per-worker final aggregates that cannot be combined
correctly. A split records the canonical source URL, object path, and byte
length, and a worker opens only the files assigned to that split. Missing files
and byte-length changes fail closed. The reader API used here does not expose a
stable fragment, snapshot, or object-version identifier, so a same-path,
same-size in-place rewrite cannot be detected. Distributed inputs must
therefore remain immutable or use versioned/content-addressed paths for the
lifetime of planning and retries, and every worker must be able to access those
paths with equivalent credentials. Vane builds also disable Vortex late
materialization because its second scan would otherwise require coupled split
assignment.

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

## Changing Vortex version

The Vortex version is defined in `vortex-extension/Cargo.toml`. It can be a git commit, tag, branch or even a local path:

```toml
vortex-duckdb = { path = "<path/to/vortex/vortex-duckdb>"}
```

See the Cargo docs for [git](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-dependencies-from-git-repositories) or [path](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#specifying-path-dependencies) dependencies for full details.

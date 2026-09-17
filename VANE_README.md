# Vortex Extension for Vane

[Project overview](README.md) · [DuckDB guide](DUCKDB_README.md)

Write and query Vortex files through Vane's SQL and Python Relation APIs.
Supported scans and file writes use the default Ray runner. Leave `VANE_RUNNER`
unset; no runner-selection call is needed.

## Install a provider package

Install `vane-extension-vortex` with the exact `vane-ai` version required by
its package metadata. Use the same matching wheels on the application, Ray
coordinator, and workers. Provider versions include an artifact identity and
differ from the base runtime version.

For development wheels, replace the following placeholders with published
matching versions for your interpreter and platform. Use a fresh wheel directory:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
VORTEX_VERSION='<provider-version>'
VANE_VERSION='<matching-vane-version>'
python -m pip download --no-deps --only-binary=:all: \
  --index-url https://test.pypi.org/simple/ --dest vortex-wheels \
  "vane-extension-vortex==$VORTEX_VERSION" "vane-ai==$VANE_VERSION"
python -m pip install --index-url https://pypi.org/simple/ \
  ./vortex-wheels/*.whl grpcio
python -m pip check
```

See [provider releases](docs/VANE_RELEASE.md) for package identities and
release channels. Load the provider:

```python
import vane
from vane import col

connection = vane.connect()
vane.load_installed_extension("vortex", connection=connection)
```

No `set_runner_ray()`, local runner, or `ray.init()` call is needed in the
application. Vane initializes Ray when required. To connect to an existing
cluster, supply `RAY_ADDRESS` before starting Python.

## Usage

Run all Python blocks in order in one process from a fresh working directory.
No sample download or cloud service is needed. The local paths below are shared
by Ray processes on the same physical host. Use new output paths when repeating
the walkthrough.

Vortex is a file format, so this example writes files before querying them.
It does not attach a table catalog or demonstrate table-level INSERT, UPDATE,
DELETE, or MERGE. To transform data, write a new derived dataset.

### 1. Write a dataset

```python
from pathlib import Path

output_path = Path("vortex_demo").resolve()
source = connection.sql("""
    SELECT i::BIGINT AS id,
           (i % 4)::INTEGER AS bucket,
           ('value-' || i::VARCHAR)::VARCHAR AS payload
    FROM range(1000) AS source(i)
""")
source.write_file(str(output_path), format="vortex")
```

`.write_file()` builds a COPY plan and dispatches it to Ray. Workers write
attempt files; the coordinator selects successful files and publishes a
manifest followed by a committed marker. The output is a dataset prefix,
not a single Vortex file with a predictable filename. Use absolute paths
because actors may have different working directories.

### 2. Resolve the committed files

Do not read the output with a data-file glob: a file's presence alone does not
prove that it belongs to the committed dataset. The current `.write_file()`
API returns no commit metadata, and distributed SQL COPY does not support
`RETURN_FILES` or `RETURN_STATS`.

For this local walkthrough, the following helper finds the sole committed
run in a **fresh, single-writer output directory**, then asks Vane's native
reader to validate its commit metadata and return the selected files:

```python
def committed_files(base_path):
    commit_root = Path(str(base_path) + ".duckdb_commit")
    markers = list(commit_root.glob("*/committed"))
    if len(markers) != 1:
        raise RuntimeError("Expected exactly one committed run in this fresh output")
    run_id = markers[0].parent.name
    result = vane.ray_cxx.read_committed_copy_direct_write_result(
        str(base_path), run_id, connection
    )
    return [entry["final_path"] for entry in result["files"]]

files = committed_files(output_path)
assert files
```

This helper uses the pinned runtime's low-level commit API and local metadata
layout; it is not a general S3 listing or multi-writer discovery API. Production
consumers should receive the exact run ID from their write orchestration and
resolve that run's manifest. Never choose an arbitrary or newest data file.

### 3. Query with SQL

```python
connection.sql("""
    SELECT count(*) AS rows, sum(id)::BIGINT AS id_sum
    FROM read_vortex(?)
""", params=[files]).show()
# rows = 1000, id_sum = 499500

print(connection.sql("""
    SELECT id, bucket, payload FROM read_vortex(?) WHERE id < 5
""", params=[files]).fetchall())
```

The list parameter names only committed files. `vortex_scan` is an alternative
scan function:

```python
connection.sql(
    "SELECT count(*) AS rows FROM vortex_scan(?)", params=[files]
).show()
# rows = 1000
```

Use `.show()` for display and `.fetchall()` when application code needs Python
rows. Both execute supported relations on Ray. Row order is unspecified
without ORDER BY. The bounded detail previews use `.fetchall()` because
`.show()` adds a limit that can trigger a batch-index error in the tested
runtime; aggregate displays above are supported.

### Use the Relation API

```python
events = connection.sql("SELECT * FROM read_vortex(?)", params=[files])
filtered = events.filter(col("id") >= 900).select(col("id"), col("payload"))
print(filtered.filter(col("id") < 905).fetchall())
filtered.aggregate("count(*) AS rows, sum(id)::BIGINT AS id_sum").show()
# rows = 100, id_sum = 94950

events.aggregate("bucket, count(*) AS rows", group_expr="bucket").show()
# Each of the four buckets has 250 rows.
```

Relations are lazy. The separate preview filter does not change `filtered`.
Sums are cast to `BIGINT` for a standard Arrow integer result.

### 4. Write and read a derived dataset

```python
derived_path = Path("vortex_demo_filtered").resolve()
filtered.write_file(str(derived_path), format="vortex")
derived_files = committed_files(derived_path)
connection.sql("""
    SELECT count(*) AS rows, sum(id)::BIGINT AS id_sum
    FROM read_vortex(?)
""", params=[derived_files]).show()
# rows = 100, id_sum = 94950
```

This writes a new dataset without modifying the original files. Unlike CTAS
(**CREATE TABLE AS SELECT**), COPY writes query rows to files without creating
a catalog table. No `ATTACH` or SQL `SET` is needed for this walkthrough;
provider loading still initializes the client connection.

## Execution and storage boundaries

| Operation | Execution |
| --- | --- |
| Provider loading and commit-file discovery | Client initialization / metadata access |
| `read_vortex`, `vortex_scan`, filters, and aggregates | Ray execution |
| `.write_file(..., format="vortex")` / supported SQL COPY | Ray writers, coordinator commit |
| Table-level INSERT, UPDATE, DELETE, MERGE | Not supplied by this file-format extension |

The coordinator binds an immutable file list, and workers read only their
assigned files. Splitting is currently per file, not per row group. A fully
pushed-down final aggregate uses one split over its complete pruned file set;
Ray execution does not imply that every small query uses several workers.
Vortex late materialization is disabled in Vane builds.

Workers must access the same paths. For multiple physical hosts, use shared
storage mounted at the same absolute path or an appropriately configured
S3-compatible store. Remote reads require an ETag or object version to pin
identity; a backend without either is rejected. The self-contained example
qualifies local storage only. See [integration notes](docs/vane.md) for remote
credentials, immutable attempts, retry behavior, and uncertain commit outcomes.

## Build from source

Ordinary Make targets build for DuckDB. Vane targets use the exact identities
in [vane-extension.toml](vane-extension.toml) and the separate Vane Rust manifest.
The development Vane pin is `d1460a580455f01485e2e508e05d0049cb18a105`:

```bash
git clone --branch v1.5-variegata_vane --recurse-submodules \
  https://github.com/AstroVela/duckdb-vortex.git
cd duckdb-vortex
make vane_validate
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' make vane_ci
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' make vane_wheel
```

These commands require the toolchain described by the
[shared build tools](https://github.com/AstroVela/vane-extension-ci-tools).
The static wheel is a separate installation path from the provider recipe.
Use non-editable installs. See [integration and qualification](docs/vane.md)
and [provider releases](docs/VANE_RELEASE.md) for lifecycle, native, Ray, and
S3-compatible storage testing.

## Tested examples

All seven Python blocks above were executed sequentially on 2026-09-17 with
Python 3.12, non-editable provider wheels, and `vane-ai==0.2.0.dev663` from
Vane revision `d1460a580455f01485e2e508e05d0049cb18a105`. The extension was rebuilt from this branch with Vortex adapter
`3da8a2848b5d10d028e69471c5c97bd3dc785a03`, which adapts the distributed
bind-data enum to the current SDK. The engine source ID was `d8a9d61d59`.

The test left `VANE_RUNNER` unset, asserted the default Ray runner, and used
an owned cluster with two CPU execution nodes on one physical host. All blocks
passed in 30.22 seconds, with two Ray writes and nine Ray reads including
additional assertions. Checks compared all 1,000 original rows, all 100 derived
rows, and both resolved file lists against the writers' committed results.

This validates the local provider walkthrough, not S3-compatible storage,
multi-host deployment, or the source-build recipe. Installation used matching
local wheels; replace the TestPyPI placeholders with published versions.

## Re-run the walkthrough test

With matching provider and Vane wheels installed, run the checked-in test from
this extension's checkout. Leave `VANE_RUNNER` and `RAY_ADDRESS` unset:

```bash
python -m pip install pytest
python -I -m pytest -q -s test/vane/test_vane_readme.py
```

The test executes the Python blocks from this guide in a fresh temporary
directory, asserts the default Ray runner, checks the resulting data, and owns
and cleans up a same-host Ray cluster with two CPU execution nodes.

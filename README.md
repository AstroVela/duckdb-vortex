# Vortex for DuckDB and Vane

Read and write the Vortex columnar file format with SQL. This repository
provides an ordinary DuckDB extension and a Vane provider for distributed
scans and file writes on Ray.

| Runtime | Guide | Execution |
| --- | --- | --- |
| DuckDB | [DUCKDB_README.md](DUCKDB_README.md) | Native extension, local and S3-compatible file access |
| Vane | [VANE_README.md](VANE_README.md) | Default Ray runner, distributed scans and COPY |

Vortex is a file format, not a transactional table catalog. The Vane walkthrough
writes a dataset, resolves its committed files, queries it with SQL and the
Relation API, and writes a derived dataset. It requires no runner configuration.

Use the artifacts for your runtime: DuckDB extension binaries and Vane provider
wheels are not interchangeable. See the [Vane integration notes](docs/vane.md)
for scan/write guarantees and test coverage, and the
[provider release guide](docs/VANE_RELEASE.md) for packaging.

This repository is based on the
[DuckDB extension template](https://github.com/duckdb/extension-template).
See [LICENSE](LICENSE) for licensing.

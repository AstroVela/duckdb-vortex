# Vortex for DuckDB and Vane

Read and write the Vortex columnar file format with SQL. This repository
provides an ordinary DuckDB extension and a Vane provider for distributed
scans and file writes on Ray.

| Runtime | Guide | Execution |
| --- | --- | --- |
| DuckDB | [DUCKDB_README.md](DUCKDB_README.md) | Native extension, local and S3-compatible file access |
| Vane | [VANE_README.md](VANE_README.md) | Default Ray runner, distributed scans and COPY |

### Tantivy full-text search

Build with `-DVORTEX_ENABLE_INDEX_TANTIVY=ON` for explicit multi-file
`vortex_fts_build` / `vortex_fts_search` SQL functions.
See the [user guide](docs/TANTIVY_FTS_USER_GUIDE.md) and
[design document](docs/TANTIVY_FTS_DESIGN.md), including the companion Iceberg
snapshot workflow and resource limits.

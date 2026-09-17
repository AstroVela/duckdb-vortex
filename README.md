# Vortex for DuckDB and Vane

Read and write the Vortex columnar file format with SQL. This repository
provides an ordinary DuckDB extension and a Vane provider for distributed
scans and file writes on Ray.

| Runtime | Guide | Execution |
| --- | --- | --- |
| DuckDB | [DUCKDB_README.md](DUCKDB_README.md) | Native extension, local and S3-compatible file access |
| Vane | [VANE_README.md](VANE_README.md) | Default Ray runner, distributed scans and COPY |

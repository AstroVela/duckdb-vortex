#!/usr/bin/env python3
"""Prove that the packaged native DuckDB extension writes Vortex to S3."""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

_HARNESS_ROOT = str(Path(__file__).resolve().parent)
sys.path.insert(0, _HARNESS_ROOT)
try:
    from vortex_s3_test_support import (
        MinioConfig,
        assert_credentials_absent,
        assert_no_ambient_aws_credentials,
        configure_s3_secret,
        require_equal,
        require_true,
        sql_string,
    )
finally:
    sys.path.remove(_HARNESS_ROOT)

del _HARNESS_ROOT


def main() -> None:
    import duckdb

    assert_no_ambient_aws_credentials()
    config = MinioConfig.from_env()
    extension_path = Path(os.environ.get("VORTEX_EXTENSION_PATH", "")).resolve()
    require_true(
        extension_path.is_file(),
        "VORTEX_EXTENSION_PATH must identify the packaged native extension",
    )
    connection = duckdb.connect(
        ":memory:",
        config={
            "allow_unsigned_extensions": "true",
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        connection.execute("INSTALL httpfs")
        configure_s3_secret(connection, config)
        connection.execute(f"LOAD {sql_string(extension_path)}")
        extension = connection.execute(
            "SELECT loaded, install_mode "
            "FROM duckdb_extensions() WHERE extension_name = 'vortex'"
        ).fetchone()
        require_equal(
            extension,
            (True, "NOT_INSTALLED"),
            "native extension load identity",
        )

        key = f"native/{uuid.uuid4().hex}/data.vortex"
        output_uri = config.uri(key)
        connection.execute(
            "COPY ("
            "SELECT i::BIGINT AS id, (i % 8)::INTEGER AS part, "
            "('row-' || i::VARCHAR)::VARCHAR AS payload, "
            "CASE WHEN i % 11 = 0 THEN NULL ELSE (i * 3)::INTEGER END AS nullable_value "
            "FROM range(128) source(i)"
            f") TO {sql_string(output_uri)} (FORMAT VORTEX)"
        )
        require_equal(config.list_keys(key), [key], "native S3 Vortex object")

        with tempfile.TemporaryDirectory(prefix="native-vortex-s3-") as temporary:
            local_path = Path(temporary) / "data.vortex"
            config.download(output_uri, local_path)
            assert_credentials_absent(
                local_path.read_bytes(), config, "native Vortex object"
            )
            rows = connection.execute(
                "SELECT count(*)::BIGINT, count(DISTINCT id)::BIGINT, sum(id)::BIGINT, "
                "min(id)::BIGINT, max(id)::BIGINT, "
                "count(*) FILTER (WHERE part IS DISTINCT FROM (id % 8)::INTEGER)::BIGINT, "
                "count(*) FILTER (WHERE payload IS DISTINCT FROM "
                "'row-' || id::VARCHAR)::BIGINT, "
                "count(*) FILTER (WHERE "
                "(id % 11 = 0 AND nullable_value IS NOT NULL) OR "
                "(id % 11 <> 0 AND nullable_value IS DISTINCT FROM "
                "(id * 3)::INTEGER))::BIGINT "
                f"FROM read_vortex({sql_string(local_path)})"
            ).fetchall()
            require_equal(
                rows,
                [(128, 128, 8128, 0, 127, 0, 0, 0)],
                "native S3 Vortex exact readback",
            )

        secret_metadata = connection.execute(
            "SELECT secret_string "
            "FROM duckdb_secrets() WHERE name = 'vortex_minio'"
        ).fetchone()
        require_true(secret_metadata is not None, "DuckDB S3 secret is missing")
        require_true(
            config.secret_key not in str(secret_metadata[0]),
            "DuckDB S3 secret metadata exposes the secret key",
        )
        assert_credentials_absent(output_uri, config, "native output URI")
        print("native DuckDB Vortex S3 write qualification passed", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()

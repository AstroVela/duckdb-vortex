#!/usr/bin/env python3
"""Shared assertions for installed-wheel Vane/Vortex qualification."""

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Callable
from pathlib import Path

FILE_COUNT = 4
ROWS_PER_FILE = 32
TOTAL_ROWS = FILE_COUNT * ROWS_PER_FILE


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def require_true(value: bool, description: str) -> None:
    if not value:
        raise AssertionError(description)


def error_chain_contains(error: BaseException, expected_message: str) -> bool:
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        if expected_message in str(current):
            return True
        seen.add(id(current))
        current = (
            current.__cause__ if current.__cause__ is not None else current.__context__
        )
    return False


def require_error(
    description: str,
    operation: Callable[[], object],
    expected_message: str,
) -> None:
    try:
        operation()
    except Exception as error:
        if not error_chain_contains(error, expected_message):
            raise AssertionError(
                f"{description}: expected error containing {expected_message!r}, got {error!r}"
            ) from error
    else:
        raise AssertionError(f"{description}: operation unexpectedly succeeded")


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def vortex_file_list(files: list[Path] | tuple[Path, ...]) -> str:
    require_true(bool(files), "a Vortex file list must not be empty")
    return "[" + ", ".join(sql_string(path) for path in files) + "]"


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def verify_installed_runtime(
    vane: object, connection: object, expected_runner: str
) -> dict[str, str]:
    require_equal(
        os.environ.get("VANE_RUNNER"), expected_runner, "configured Vane runner"
    )

    expected_revision = os.environ.get("VANE_EXPECTED_REVISION", "")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_revision):
        raise AssertionError(
            "VANE_EXPECTED_REVISION must be an exact lowercase commit SHA"
        )
    expected_package_version = os.environ.get("VANE_EXPECTED_PACKAGE_VERSION", "")
    if not re.fullmatch(
        r"[0-9]+\.[0-9]+\.[0-9]+(?:\.dev[0-9]+)?", expected_package_version
    ):
        raise AssertionError(
            "VANE_EXPECTED_PACKAGE_VERSION must be an exact canonical package version"
        )
    expected_fork_version = os.environ.get("VANE_EXPECTED_FORK_VERSION", "")
    if not re.fullmatch(
        r"v[0-9]+\.[0-9]+\.[0-9]+-vane\.[0-9a-f]{10}", expected_fork_version
    ):
        raise AssertionError(
            "VANE_EXPECTED_FORK_VERSION must be an exact Vane DuckDB fork version"
        )
    expected_source_id = os.environ.get("VANE_EXPECTED_DUCKDB_SOURCE_ID", "")
    if not re.fullmatch(r"[0-9a-f]{10}", expected_source_id):
        raise AssertionError(
            "VANE_EXPECTED_DUCKDB_SOURCE_ID must be an exact DuckDB SourceID"
        )
    expected_vortex_revision = os.environ.get("VORTEX_EXPECTED_REVISION", "")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_vortex_revision):
        raise AssertionError(
            "VORTEX_EXPECTED_REVISION must be an exact lowercase commit SHA"
        )
    expected_vortex_version = os.environ.get("VORTEX_EXPECTED_VERSION", "")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", expected_vortex_version):
        raise AssertionError(
            "VORTEX_EXPECTED_VERSION must be an exact semantic version"
        )
    wheel_sha256 = os.environ.get("VANE_WHEEL_SHA256", "")
    if not re.fullmatch(r"[0-9a-f]{64}", wheel_sha256):
        raise AssertionError("VANE_WHEEL_SHA256 must be an exact lowercase SHA-256")

    prefix = Path(sys.prefix).resolve()
    module_path = Path(vane.__file__).resolve()
    require_true(
        _is_relative_to(module_path, prefix),
        f"Vane was not imported from the clean wheel environment: {module_path}",
    )

    forbidden_text = os.environ.get("VANE_FORBIDDEN_SOURCE_ROOT", "").strip()
    if forbidden_text:
        forbidden = Path(forbidden_text).resolve()
        require_true(
            not _is_relative_to(module_path, forbidden),
            f"Vane unexpectedly loaded from the extension checkout: {module_path}",
        )
        for entry in sys.path:
            if not entry:
                continue
            resolved = Path(entry).resolve()
            require_true(
                not _is_relative_to(resolved, forbidden),
                f"the isolated Python path contains the extension checkout: {resolved}",
            )

    for setting in ("autoinstall_known_extensions", "autoload_known_extensions"):
        actual = connection.execute(f"SELECT current_setting('{setting}')").fetchone()
        require_equal(str(actual[0]).lower(), "false", f"{setting} setting")

    extension = connection.execute(
        "SELECT loaded, install_mode, extension_version FROM duckdb_extensions() "
        "WHERE extension_name = 'vortex'"
    ).fetchone()
    if extension is None:
        raise AssertionError("the packaged Vane wheel does not contain Vortex")
    require_equal(extension[1], "STATICALLY_LINKED", "Vortex install mode before LOAD")
    require_equal(
        extension[2], expected_vortex_version, "Vortex extension version before LOAD"
    )
    connection.execute("LOAD vortex")
    loaded = connection.execute(
        "SELECT loaded, install_mode, extension_version FROM duckdb_extensions() "
        "WHERE extension_name = 'vortex'"
    ).fetchone()
    require_equal(
        loaded,
        (True, "STATICALLY_LINKED", expected_vortex_version),
        "loaded Vortex wheel identity",
    )

    library_version, source_id = connection.execute(
        "SELECT library_version, source_id FROM pragma_version()"
    ).fetchone()
    require_equal(
        str(library_version),
        expected_fork_version,
        "installed Vane DuckDB fork version",
    )
    require_equal(
        str(source_id), str(vane.__git_revision__), "installed Vane DuckDB SourceID"
    )
    require_equal(
        str(source_id),
        expected_source_id,
        "installed Vane DuckDB expected SourceID",
    )
    require_equal(
        str(vane.__version__),
        expected_package_version,
        "installed Vane package version",
    )

    identity = {
        "extension": "vortex",
        "install_mode": str(loaded[1]),
        "library_version": str(library_version),
        "module_path": str(module_path),
        "package_version": str(vane.__version__),
        "runner": expected_runner,
        "source_id": str(source_id),
        "vane_revision": expected_revision,
        "vortex_revision": expected_vortex_revision,
        "vortex_version": expected_vortex_version,
        "wheel_sha256": wheel_sha256,
    }
    print(json.dumps(identity, sort_keys=True), flush=True)
    return identity


def create_vortex_fixture(connection: object, root: Path) -> tuple[list[Path], Path]:
    root.mkdir(parents=True, exist_ok=False)
    files: list[Path] = []
    for file_index in range(FILE_COUNT):
        start = file_index * ROWS_PER_FILE
        stop = start + ROWS_PER_FILE
        path = root / f"part-{file_index:02d}.vortex"
        connection.execute(
            f"""
            COPY (
                SELECT
                    i::BIGINT AS id,
                    (i % 8)::INTEGER AS part,
                    ('row-' || i::VARCHAR)::VARCHAR AS payload,
                    CASE WHEN i % 11 = 0 THEN NULL ELSE (i * 3)::INTEGER END AS nullable_value
                FROM range({start}, {stop}) source(i)
            ) TO {sql_string(path)} (FORMAT VORTEX)
            """
        )
        require_true(
            path.is_file() and path.stat().st_size > 0,
            f"fixture file was not written: {path}",
        )
        files.append(path)

    empty_path = root / "empty.vortex"
    connection.execute(
        f"""
        COPY (
            SELECT
                i::BIGINT AS id,
                (i % 8)::INTEGER AS part,
                ('row-' || i::VARCHAR)::VARCHAR AS payload,
                (i * 3)::INTEGER AS nullable_value
            FROM range(0) source(i)
        ) TO {sql_string(empty_path)} (FORMAT VORTEX)
        """
    )
    require_true(
        empty_path.is_file() and empty_path.stat().st_size > 0,
        "the zero-row Vortex fixture was not written",
    )

    files_sql = vortex_file_list(files)
    summary = connection.execute(
        "SELECT count(*)::BIGINT, count(DISTINCT id)::BIGINT, sum(id)::BIGINT, "
        "min(id)::BIGINT, max(id)::BIGINT, "
        "count(*) FILTER (WHERE payload IS DISTINCT FROM 'row-' || id::VARCHAR)::BIGINT, "
        "count(*) FILTER (WHERE nullable_value IS NULL)::BIGINT "
        f"FROM read_vortex({files_sql})"
    ).fetchall()
    require_equal(
        summary, [(128, 128, 8128, 0, 127, 0, 12)], "native Vortex fixture content"
    )
    return files, empty_path


def scan_cases(files: list[Path], empty_path: Path) -> list[tuple[str, str]]:
    files_sql = vortex_file_list(files)
    single_sql = sql_string(files[0])
    glob_sql = sql_string(files[0].parent / "part-*.vortex")
    empty_sql = sql_string(empty_path)
    return [
        (
            "single-path content",
            "SELECT id, part, payload, nullable_value "
            f"FROM read_vortex({single_sql}) ORDER BY id",
        ),
        (
            "path-list content",
            "SELECT id, part, payload, nullable_value "
            f"FROM read_vortex({files_sql}) ORDER BY id",
        ),
        (
            "glob content",
            "SELECT id, part, payload, nullable_value "
            f"FROM read_vortex({glob_sql}) ORDER BY id",
        ),
        (
            "vortex_scan alias",
            "SELECT id, file_index FROM vortex_scan("
            f"{files_sql}) WHERE id BETWEEN 29 AND 68 ORDER BY id",
        ),
        (
            "projection and filter pushdown",
            "SELECT id, payload FROM read_vortex("
            f"{files_sql}) WHERE part IN (2, 5) AND id BETWEEN 17 AND 111 ORDER BY id",
        ),
        (
            "aggregate pushdown",
            "SELECT count(*)::BIGINT, min(id)::BIGINT, max(id)::BIGINT, sum(id)::BIGINT "
            f"FROM read_vortex({files_sql}) WHERE id >= 19 AND id < 103",
        ),
        (
            "virtual-column aggregate",
            "SELECT min(file_index)::UBIGINT, max(file_index)::UBIGINT, "
            f"count(*)::BIGINT FROM read_vortex({files_sql})",
        ),
        (
            "file_index pruning",
            "SELECT file_index::UBIGINT, count(*)::BIGINT, min(id)::BIGINT, max(id)::BIGINT "
            f"FROM read_vortex({files_sql}) WHERE file_index IN (1, 3) "
            "GROUP BY file_index ORDER BY file_index",
        ),
        (
            "zero-match file pruning",
            "SELECT count(*)::BIGINT, sum(id)::BIGINT "
            f"FROM read_vortex({files_sql}) WHERE file_index = 99",
        ),
        (
            "explicit empty split",
            "SELECT id, file_index "
            f"FROM read_vortex({files_sql}) WHERE file_index = 99 ORDER BY id",
        ),
        (
            "empty Vortex input",
            f"SELECT count(*)::BIGINT FROM read_vortex({empty_sql})",
        ),
    ]


def verify_scan_schema(connection: object, files: list[Path], empty_path: Path) -> None:
    expected = [
        ("id", "BIGINT"),
        ("part", "INTEGER"),
        ("payload", "VARCHAR"),
        ("nullable_value", "INTEGER"),
    ]
    for description, source in (
        ("non-empty", vortex_file_list(files)),
        ("empty", sql_string(empty_path)),
    ):
        rows = connection.execute(
            "DESCRIBE SELECT id, part, payload, nullable_value "
            f"FROM read_vortex({source})"
        ).fetchall()
        require_equal(
            [(row[0], row[1]) for row in rows], expected, f"{description} Vortex schema"
        )


def verify_known_case_results(
    connection: object, files: list[Path], empty_path: Path
) -> None:
    files_sql = vortex_file_list(files)
    require_equal(
        connection.execute(
            "SELECT count(*)::BIGINT, min(id)::BIGINT, max(id)::BIGINT, sum(id)::BIGINT "
            f"FROM read_vortex({files_sql}) WHERE id >= 19 AND id < 103"
        ).fetchall(),
        [(84, 19, 102, 5082)],
        "known aggregate result",
    )
    require_equal(
        connection.execute(
            "SELECT min(file_index)::UBIGINT, max(file_index)::UBIGINT, "
            f"count(*)::BIGINT FROM read_vortex({files_sql})"
        ).fetchall(),
        [(0, 3, 128)],
        "known virtual-column aggregate result",
    )
    require_equal(
        connection.execute(
            "SELECT file_index::UBIGINT, count(*)::BIGINT, min(id)::BIGINT, max(id)::BIGINT "
            f"FROM read_vortex({files_sql}) WHERE file_index IN (1, 3) "
            "GROUP BY file_index ORDER BY file_index"
        ).fetchall(),
        [(1, 32, 32, 63), (3, 32, 96, 127)],
        "known file_index pruning result",
    )
    require_equal(
        connection.execute(
            "SELECT count(*)::BIGINT, sum(id)::BIGINT "
            f"FROM read_vortex({files_sql}) WHERE file_index = 99"
        ).fetchall(),
        [(0, None)],
        "known zero-match result",
    )
    require_equal(
        connection.execute(
            f"SELECT count(*)::BIGINT FROM read_vortex({sql_string(empty_path)})"
        ).fetchall(),
        [(0,)],
        "known empty input result",
    )


def assert_exact_dataset(
    connection: object, files: list[Path], description: str
) -> None:
    files_sql = vortex_file_list(files)
    rows = connection.execute(
        "SELECT count(*)::BIGINT, count(DISTINCT id)::BIGINT, sum(id)::BIGINT, "
        "min(id)::BIGINT, max(id)::BIGINT, "
        "count(*) FILTER (WHERE part IS DISTINCT FROM (id % 8)::INTEGER)::BIGINT, "
        "count(*) FILTER (WHERE payload IS DISTINCT FROM 'row-' || id::VARCHAR)::BIGINT, "
        "count(*) FILTER (WHERE "
        "(id % 11 = 0 AND nullable_value IS NOT NULL) OR "
        "(id % 11 <> 0 AND nullable_value IS DISTINCT FROM (id * 3)::INTEGER))::BIGINT "
        f"FROM read_vortex({files_sql})"
    ).fetchall()
    require_equal(rows, [(TOTAL_ROWS, TOTAL_ROWS, 8128, 0, 127, 0, 0, 0)], description)
    schema = connection.execute(
        "DESCRIBE SELECT id, part, payload, nullable_value "
        f"FROM read_vortex({files_sql})"
    ).fetchall()
    require_equal(
        [(row[0], row[1]) for row in schema],
        [
            ("id", "BIGINT"),
            ("part", "INTEGER"),
            ("payload", "VARCHAR"),
            ("nullable_value", "INTEGER"),
        ],
        f"{description} schema",
    )

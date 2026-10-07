# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def cmake_fixture(tmp_path):
    source = tmp_path / "fixture"
    source.mkdir()
    corrosion = source / "corrosion"
    corrosion.mkdir()
    (corrosion / "CMakeLists.txt").write_text(
        "cmake_minimum_required(VERSION 3.22)\n"
        "function(corrosion_import_crate)\n"
        "  add_library(vortex_duckdb-static INTERFACE)\n"
        '  file(WRITE "${CMAKE_BINARY_DIR}/cargo-flags.txt" "${ARGN}")\n'
        "endfunction()\n"
        "function(corrosion_set_env_vars)\n"
        '  file(WRITE "${CMAKE_BINARY_DIR}/cargo-env.txt" "${ARGN}")\n'
        "endfunction()\n"
    )
    (source / "stub.cpp").write_text("int fixture() { return 0; }\n")
    (source / "CMakeLists.txt").write_text(
        "cmake_minimum_required(VERSION 3.22)\nproject(index_configuration LANGUAGES CXX)\n"
        'set(DUCKDB_VERSION "1.5.0")\nset(DUCKDB_MODULE_BASE_DIR "${CMAKE_SOURCE_DIR}")\n'
        'set(DUCKDB_EXPORT_SET "fixture")\nset(INSTALL_LIB_DIR "lib")\n'
        "function(build_static_extension name)\n"
        '  add_library(${name}_extension STATIC "${CMAKE_SOURCE_DIR}/stub.cpp")\n'
        "endfunction()\n"
        "function(build_loadable_extension name)\n"
        '  add_library(${name}_loadable_extension SHARED "${CMAKE_SOURCE_DIR}/stub.cpp")\n'
        "endfunction()\n"
        f'add_subdirectory("{ROOT}" extension)\n'
        "get_target_property(links vortex_extension LINK_LIBRARIES)\n"
        'file(WRITE "${CMAKE_BINARY_DIR}/links.txt" "${links}")\n'
    )
    hnsw = source / "hnswlib-source/hnswlib"
    hnsw.mkdir(parents=True)
    (hnsw / "hnswlib.h").touch()
    spfresh = source / "spfresh"
    spfresh.mkdir()
    for archive in ("libspfresh_core.a", "libspfresh_distance.a", "libzstd.so"):
        (spfresh / archive).touch()
    (spfresh / "zstd-library.txt").write_text(str(spfresh / "libzstd.so") + "\n")
    for header in (
        "function/distributed_table_function.hpp",
        "execution/distributed/copy_to_file.hpp",
        "execution/distributed/copy_finalize.hpp",
    ):
        path = source / "src/include/duckdb" / header
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    return source


def configure(source, *flags):
    return subprocess.run(
        [
            shutil.which("cmake") or "cmake",
            "-S",
            str(source),
            "-B",
            str(source / "build"),
            f"-DFETCHCONTENT_SOURCE_DIR_CORROSION={source / 'corrosion'}",
            f"-DVORTEX_HNSWLIB_SOURCE={source / 'hnswlib-source'}",
            f"-DVORTEX_SPFRESH_NATIVE={source / 'spfresh'}",
            *flags,
        ],
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.mark.parametrize("vane", [False, True])
@pytest.mark.parametrize(
    "hnsw,spfresh", [(False, False), (True, False), (False, True), (True, True)]
)
def test_cmake_optional_backends_forward_only_selected_dependencies(
    cmake_fixture, hnsw, spfresh, vane
):
    source = cmake_fixture
    result = configure(
        source,
        f"-DVORTEX_ENABLE_INDEX_HNSWLIB={'ON' if hnsw else 'OFF'}",
        f"-DVORTEX_ENABLE_INDEX_SPFRESH={'ON' if spfresh else 'OFF'}",
        f"-DVORTEX_VANE_DISTRIBUTED={'ON' if vane else 'OFF'}",
        f"-DCMAKE_DISABLE_FIND_PACKAGE_OpenMP={'OFF' if spfresh else 'TRUE'}",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    flags = (source / "build/cargo-flags.txt").read_text().split(";")
    environment = (source / "build/cargo-env.txt").read_text()
    links = (source / "build/links.txt").read_text()
    assert ("index-hnswlib" in flags) is hnsw
    assert ("index-spfresh" in flags) is spfresh
    assert ("VORTEX_HNSWLIB_SOURCE=" in environment) is hnsw
    assert ("VORTEX_SPFRESH_NATIVE=" in environment) is spfresh
    assert ("gomp" in links) is spfresh
    assert ("libzstd.so" in links) is spfresh
    manifest = "vortex-extension-vane" if vane else "vortex-extension"
    assert f"{manifest}/Cargo.toml" in flags
    assert ("--locked" in flags) is vane
    assert ("VORTEX_VANE_DISTRIBUTED=1" in environment) is vane


def test_cmake_hnswlib_is_disabled_by_default(cmake_fixture):
    result = configure(cmake_fixture, "-DVORTEX_HNSWLIB_SOURCE=/missing")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "index-hnswlib" not in (cmake_fixture / "build/cargo-flags.txt").read_text()


def test_cmake_hnswlib_rejects_missing_source(cmake_fixture):
    result = configure(
        cmake_fixture,
        "-DVORTEX_ENABLE_INDEX_HNSWLIB=ON",
        "-DVORTEX_HNSWLIB_SOURCE=/missing",
    )
    assert result.returncode != 0
    assert "requires VORTEX_HNSWLIB_SOURCE" in result.stderr


def test_cmake_hnswlib_rejects_unsupported_platform(cmake_fixture):
    result = configure(
        cmake_fixture,
        "-DVORTEX_ENABLE_INDEX_HNSWLIB=ON",
        "-DCMAKE_SYSTEM_NAME=Linux",
        "-DCMAKE_SYSTEM_PROCESSOR=aarch64",
    )
    assert result.returncode != 0
    assert "requires Linux x86_64" in result.stderr

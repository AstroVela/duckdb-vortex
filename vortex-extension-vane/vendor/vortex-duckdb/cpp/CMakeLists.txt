# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# This CMake project is only used for IDE support (CLion, etc.).
# See build.rs for the actual build definition.
# Configure it with the exact Vane DuckDB source, for example
# `cmake -DDUCKDB_SOURCE_DIR=/path/to/vane/external/duckdb ...`.
#
# If your editor relies on a compilation database to enable LSP functionality,
# you can generate it by running `cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON` or
# `bear -- cargo build` (https://github.com/rizsotto/Bear).
cmake_minimum_required(VERSION 3.22)
project(vortex_duckdb_cpp)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to debug if build config is not explicitly set.
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Enable compiler warnings (matching build.rs flags).
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Werror")

if(NOT DEFINED DUCKDB_SOURCE_DIR OR "${DUCKDB_SOURCE_DIR}" STREQUAL "")
    message(FATAL_ERROR
        "DUCKDB_SOURCE_DIR must name the exact Vane DuckDB source tree"
    )
endif()
set(DUCKDB_INCLUDE "${DUCKDB_SOURCE_DIR}/src/include")
if(NOT EXISTS
   "${DUCKDB_INCLUDE}/duckdb/function/distributed_table_function.hpp")
    message(FATAL_ERROR
        "DUCKDB_SOURCE_DIR does not provide Vane distributed scan headers"
    )
endif()

include_directories(include ../include)
include_directories(SYSTEM ${DUCKDB_INCLUDE})
add_compile_definitions(VORTEX_VANE_DISTRIBUTED=1)

set(CPP_SOURCES
    aggregate_fn_pushdown.cpp
    cast_pushdown.cpp
    copy_function.cpp
    expr.cpp
    optimizer.cpp
    scalar_fn_pushdown.cpp
    spatial_overrides.cpp
    table_filter.cpp
    table_function.cpp
    vector.cpp
    vortex_duckdb.cpp
)
add_library(vortex ${CPP_SOURCES})

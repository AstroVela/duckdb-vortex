# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(vortex
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Custom extension configs are processed after DuckDB's BUILD_EXTENSIONS list.
# Add the already-registered local target here so embedding consumers (notably
# Vane's Python module) link this checkout instead of DuckDB's remote Vortex
# registry entry.
list(FIND BUILD_EXTENSIONS vortex VORTEX_BUILD_EXTENSION_INDEX)
if(VORTEX_BUILD_EXTENSION_INDEX EQUAL -1)
    list(APPEND BUILD_EXTENSIONS vortex)
endif()
if(NOT CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(BUILD_EXTENSIONS "${BUILD_EXTENSIONS}" PARENT_SCOPE)
endif()

# Vane's additive extension configuration. The ordinary DuckDB build keeps
# using extension_config.cmake unchanged.
if(NOT EXISTS "${DUCKDB_MODULE_BASE_DIR}/src/include/duckdb/function/distributed_table_function.hpp")
    message(FATAL_ERROR
            "vane_extension_config.cmake requires Vane distributed scan support"
    )
endif()

set(VORTEX_ENABLE_VANE ON CACHE BOOL
    "Enable Vane-only registration and explicit distributed scans" FORCE)
include(${CMAKE_CURRENT_LIST_DIR}/extension_config.cmake)

# Vane processes custom extension configs after BUILD_EXTENSIONS. Add the
# already-registered local target so embedding consumers (notably its Python
# module) link this checkout instead of DuckDB's remote Vortex registry entry.
list(FIND BUILD_EXTENSIONS vortex VORTEX_BUILD_EXTENSION_INDEX)
if(VORTEX_BUILD_EXTENSION_INDEX EQUAL -1)
    list(APPEND BUILD_EXTENSIONS vortex)
endif()
if(NOT CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(BUILD_EXTENSIONS "${BUILD_EXTENSIONS}" PARENT_SCOPE)
endif()

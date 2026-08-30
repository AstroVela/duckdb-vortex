# This configuration is used only by the independently invoked Vane build.
# The ordinary DuckDB build continues to use extension_config.cmake.
if(NOT DEFINED DUCKDB_MODULE_BASE_DIR OR "${DUCKDB_MODULE_BASE_DIR}" STREQUAL "")
    message(FATAL_ERROR
            "extension_config_vane.cmake requires Vane's DuckDB source directory"
    )
endif()

set(VORTEX_VANE_DISTRIBUTED_HEADER
    "${DUCKDB_MODULE_BASE_DIR}/src/include/duckdb/function/distributed_table_function.hpp"
)
if(NOT EXISTS "${VORTEX_VANE_DISTRIBUTED_HEADER}")
    message(FATAL_ERROR
            "extension_config_vane.cmake requires Vane distributed extension headers"
    )
endif()

set(VORTEX_VANE_DISTRIBUTED ON CACHE BOOL
    "Build Vane distributed Vortex support" FORCE)
set(BUILD_VORTEX_DISTRIBUTED_TESTS ON CACHE BOOL
    "Build the Vane distributed Vortex scan and COPY protocol test" FORCE)
include(${CMAKE_CURRENT_LIST_DIR}/extension_config.cmake)

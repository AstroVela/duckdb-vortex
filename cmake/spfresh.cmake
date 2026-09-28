if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "The SPFresh integration currently requires Linux")
endif()
find_package(OpenMP REQUIRED COMPONENTS CXX)
find_package(Threads REQUIRED)
set(SPFRESH_LIBRARIES)
foreach(name ssdservingLib SPTAGLibStatic DistanceUtils)
    set(archive "${VORTEX_SPTAG_HOME}/Release/lib${name}.a")
    if(NOT EXISTS "${archive}")
        message(FATAL_ERROR "Missing ${archive}; set VORTEX_SPTAG_HOME to a built SPFresh checkout")
    endif()
    list(APPEND SPFRESH_LIBRARIES "${archive}")
endforeach()
# The Rust archive already embeds zstd; another static copy conflicts at link time.
find_library(SPFRESH_zstd_LIBRARY NAMES libzstd.so libzstd.so.1 REQUIRED)
list(APPEND SPFRESH_LIBRARIES "${SPFRESH_zstd_LIBRARY}")
foreach(name tbb numa)
    find_library(SPFRESH_${name}_LIBRARY NAMES ${name}
                 HINTS "${VORTEX_SPTAG_HOME}/Release" REQUIRED)
    list(APPEND SPFRESH_LIBRARIES "${SPFRESH_${name}_LIBRARY}")
endforeach()

add_library(vortex_spfresh_bridge STATIC src/spfresh/sptag_spfresh_ffi.cpp)
set_target_properties(vortex_spfresh_bridge PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(vortex_spfresh_bridge PRIVATE
    "${VORTEX_SPTAG_HOME}/AnnService"
    "${VORTEX_SPTAG_HOME}/ThirdParty/zstd/lib")
target_compile_definitions(vortex_spfresh_bridge PRIVATE TBB NUMA)
target_link_libraries(vortex_spfresh_bridge PUBLIC ${SPFRESH_LIBRARIES}
    OpenMP::OpenMP_CXX Threads::Threads ${CMAKE_DL_LIBS} rt)
install(TARGETS vortex_spfresh_bridge EXPORT "${DUCKDB_EXPORT_SET}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

foreach(target ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
    target_sources(${target} PRIVATE src/spfresh_search.cpp)
    target_include_directories(${target} PRIVATE
        "${DUCKDB_MODULE_BASE_DIR}/third_party/yyjson/include" src/spfresh)
    target_compile_definitions(${target} PRIVATE VORTEX_ENABLE_SPTAG_SPFRESH=1)
    target_link_libraries(${target} vortex_spfresh_bridge)
endforeach()

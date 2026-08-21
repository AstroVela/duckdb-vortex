PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=vortex_duckdb
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
EXT_FLAGS ?=-DCMAKE_OSX_DEPLOYMENT_TARGET=12.0
export MACOSX_DEPLOYMENT_TARGET=12.0
export VCPKG_FEATURE_FLAGS=-binarycaching
export VCPKG_OSX_DEPLOYMENT_TARGET=12.0
export VCPKG_TOOLCHAIN_PATH := ${PROJ_DIR}vcpkg/scripts/buildsystems/vcpkg.cmake

# This is not needed on macOS, we don't see a tls error on load there.
ifeq ($(shell uname), Linux)
    export CFLAGS=-ftls-model=global-dynamic
endif

include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Keep the Vane lane additive: an existing checkout that has initialized only
# DuckDB's original CI tools can still use every ordinary extension target.
VANE_EXTENSION_CI_MAKEFILE := $(PROJ_DIR)vane-extension-ci-tools/makefiles/vane_extension.Makefile
ifneq ($(wildcard $(VANE_EXTENSION_CI_MAKEFILE)),)
include $(VANE_EXTENSION_CI_MAKEFILE)
else
.PHONY: vane_verify_ci_tools vane_validate vane_prepare vane_identity vane_native vane_ci \
	vane_wheel_dependencies vane_wheel
vane_verify_ci_tools vane_validate vane_prepare vane_identity vane_native vane_ci \
	vane_wheel_dependencies vane_wheel:
	@echo "vane-extension-ci-tools is not initialized; run: git submodule update --init vane-extension-ci-tools" >&2
	@exit 1
endif

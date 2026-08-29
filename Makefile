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

# Run Vane-only targets in a recursive Make invocation so their variables never
# enter DuckDB's upstream extension build.
VANE_EXTENSION_MAKEFILE := $(PROJ_DIR)vane-extension-ci-tools/makefiles/vane_extension.Makefile
VANE_EXTENSION_TARGETS := vane_verify_ci_tools vane_validate vane_prepare vane_identity \
	vane_native vane_ci vane_wheel_dependencies vane_wheel
.PHONY: $(VANE_EXTENSION_TARGETS)

$(VANE_EXTENSION_TARGETS):
	@test -f "$(VANE_EXTENSION_MAKEFILE)" || { \
		printf 'initialize vane-extension-ci-tools before running %s\n' "$@" >&2; \
		exit 2; \
	}
	+$(MAKE) --no-print-directory -f "$(VANE_EXTENSION_MAKEFILE)" "$@" \
		VANE_EXTENSION_ROOT="$(abspath $(PROJ_DIR))"

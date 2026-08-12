PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
DUCKDB_SRCDIR := $(PROJ_DIR)vane/external/duckdb

EXT_NAME=vortex_duckdb
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
EXT_FLAGS ?=-DCMAKE_OSX_DEPLOYMENT_TARGET=12.0
VANE_DISTRIBUTED_EXCHANGE ?= 0
VANE_VCPKG_TARGET_TRIPLET ?= x64-linux
VANE_VCPKG_INSTALLED_DIR ?= $(PROJ_DIR)vane/vcpkg_installed
VANE_DUCKDB_FORK_VERSION := $(strip $(shell python3 $(PROJ_DIR)vane/scripts/resolve_duckdb_fork_version.py --print-version))
VANE_DUCKDB_UPSTREAM_VERSION := $(word 1,$(subst -vane., ,$(VANE_DUCKDB_FORK_VERSION)))
VANE_DUCKDB_SOURCE_ID_SHORT := $(strip $(shell python3 $(PROJ_DIR)vane/scripts/sync_duckdb_source_id.py --print | cut -c1-10))
OVERRIDE_GIT_DESCRIBE ?= $(VANE_DUCKDB_UPSTREAM_VERSION)-0-g$(VANE_DUCKDB_SOURCE_ID_SHORT)
EXT_FLAGS := $(EXT_FLAGS) \
	-DDUCKDB_EXPLICIT_VERSION=$(VANE_DUCKDB_FORK_VERSION) \
	-DGIT_COMMIT_HASH=$(VANE_DUCKDB_SOURCE_ID_SHORT) \
	-DCMAKE_CXX_STANDARD=20 \
	-DCMAKE_CXX_STANDARD_REQUIRED=ON \
	-DCMAKE_CXX_EXTENSIONS=OFF \
	-DCMAKE_CXX_SCAN_FOR_MODULES=OFF \
	-DBUILD_DISTRIBUTED=ON \
	-DBUILD_VORTEX_DISTRIBUTED_TESTS=ON

ifeq ($(VANE_DISTRIBUTED_EXCHANGE),1)
	EXT_FLAGS := $(EXT_FLAGS) -DBUILD_DISTRIBUTED_EXCHANGE=ON \
		-DDUCKDB_DISTRIBUTED_EXCHANGE_USE_INSTALLED_LIBS=OFF \
		-DCMAKE_PREFIX_PATH=$(VANE_VCPKG_INSTALLED_DIR)/$(VANE_VCPKG_TARGET_TRIPLET)
else
	EXT_FLAGS := $(EXT_FLAGS) -DBUILD_DISTRIBUTED_EXCHANGE=OFF
endif

export MACOSX_DEPLOYMENT_TARGET=12.0
export VCPKG_FEATURE_FLAGS=-binarycaching
export VCPKG_OSX_DEPLOYMENT_TARGET=12.0
export VCPKG_TOOLCHAIN_PATH := ${PROJ_DIR}vcpkg/scripts/buildsystems/vcpkg.cmake

# This is not needed on macOS, we don't see a tls error on load there.
ifeq ($(shell uname), Linux)
    export CFLAGS=-ftls-model=global-dynamic
endif

EXTENSION_CI_MAKEFILE := $(PROJ_DIR)extension-ci-tools/makefiles/duckdb_extension.Makefile
EXTENSION_CI_TARGETS := all clean clean-python clangd configure_ci debug pull release relassert reldebug \
	test test_debug test_release test_reldebug update wasm_eh wasm_mvp wasm_threads

export PROJ_DIR DUCKDB_SRCDIR EXT_NAME EXT_CONFIG EXT_FLAGS OVERRIDE_GIT_DESCRIBE

.PHONY: $(EXTENSION_CI_TARGETS)
$(EXTENSION_CI_TARGETS):
	+$(MAKE) -f $(EXTENSION_CI_MAKEFILE) $@

.PHONY: format format-check format-fix format-main output_distribution_matrix set_duckdb_version

format-check:
	python3 $(DUCKDB_SRCDIR)/scripts/format.py --all --check --directories src test

format: format-fix

format-fix:
	python3 $(DUCKDB_SRCDIR)/scripts/format.py --all --fix --noconfirm --directories src test

format-main:
	python3 $(DUCKDB_SRCDIR)/scripts/format.py main --fix --noconfirm --directories src test

output_distribution_matrix:
	cat $(DUCKDB_SRCDIR)/.github/config/distribution_matrix.json

set_duckdb_version:
	@test -n "$(DUCKDB_GIT_VERSION)" || \
		{ echo "DUCKDB_GIT_VERSION must be set to $(VANE_DUCKDB_UPSTREAM_VERSION)" >&2; exit 1; }
	@test "$(DUCKDB_GIT_VERSION)" = "$(VANE_DUCKDB_UPSTREAM_VERSION)" || \
		{ echo "Vane pins DuckDB $(VANE_DUCKDB_UPSTREAM_VERSION), not $(DUCKDB_GIT_VERSION)" >&2; exit 1; }
	@echo "Using Vane's pinned DuckDB $(VANE_DUCKDB_UPSTREAM_VERSION) from $(DUCKDB_SRCDIR)"

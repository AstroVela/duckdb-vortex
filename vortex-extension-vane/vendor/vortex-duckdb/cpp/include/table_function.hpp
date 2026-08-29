// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#pragma once

#include "data.hpp"
#include "duckdb.h"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"

using namespace duckdb;

static_assert(sizeof(idx_t) == 8);

// We need this exposed to compare function addresses in optimizer.cpp
unique_ptr<FunctionData> duckdb_vx_table_function_bind(ClientContext &context,
                                                       TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types,
                                                       vector<string> &names);

struct TableFunctionProjectionExpressionInput {
    const LogicalGet &get;
    const Expression &expression;
    idx_t projection_idx;
};

// true if we can push down the expression, false otherwise
bool projection_expression_pushdown(ClientContext &context,
                                    const TableFunctionProjectionExpressionInput &input);

struct TableFunctionUngroupedAggregateInput {
    const LogicalGet &get;
    // Column scan index -> aggregate expression
    const vector<std::pair<idx_t, const Expression &>> &projections;
};

bool aggregate_pushdown(ClientContext &context, const TableFunctionUngroupedAggregateInput &input);

struct VortexBindData final : FunctionData {
#ifdef VORTEX_VANE_DISTRIBUTED
    VortexBindData(unique_ptr<CData> ffi_data, const vector<LogicalType> &types, const vector<string> &names)
        : ffi_data(std::move(ffi_data)), types(types), names(names) {
    }
#else
    VortexBindData(unique_ptr<CData> ffi_data, const vector<LogicalType> &types)
        : ffi_data(std::move(ffi_data)), types(types) {
    }
#endif
    unique_ptr<FunctionData> Copy() const override;
    bool Equals(const FunctionData &other) const override;

    unique_ptr<CData> ffi_data;
    vector<LogicalType> types;
#ifdef VORTEX_VANE_DISTRIBUTED
    vector<string> names;
    string scan_split_set_id;
    struct DistributedFile {
        string source_url;
        string path;
        idx_t size;

        bool operator==(const DistributedFile &other) const {
            return source_url == other.source_url && path == other.path && size == other.size;
        }
    };

    struct PortableSnapshot {
        string portable_bind;
        vector<DistributedFile> distributed_files;
        bool aggregate_scan = false;
    };

    PortableSnapshot CreatePortableSnapshot() const;

    string portable_bind;
    vector<DistributedFile> distributed_files;
    bool aggregate_scan = false;
    bool explicit_split_mode = false;
    bool splits_applied = false;
    vector<idx_t> eligible_file_indexes;
    vector<idx_t> assigned_file_indexes;
#endif
};

struct VortexGlobalData final : GlobalTableFunctionState {
#ifdef VORTEX_VANE_DISTRIBUTED
    explicit VortexGlobalData(unique_ptr<CData> ffi_data,
                              bool distributed = false,
                              bool force_empty_output = false)
        : ffi_data(std::move(ffi_data)), distributed(distributed), force_empty_output(force_empty_output) {
    }
#else
    explicit VortexGlobalData(unique_ptr<CData> ffi_data) : ffi_data(std::move(ffi_data)) {
    }
#endif

    idx_t MaxThreads() const override {
        return GlobalTableFunctionState::MAX_THREADS;
    }

    unique_ptr<CData> ffi_data;
#ifdef VORTEX_VANE_DISTRIBUTED
    bool distributed;
    bool force_empty_output;
#endif
};

struct VortexLocalData final : LocalTableFunctionState {
    explicit VortexLocalData(unique_ptr<CData> ffi_data) : ffi_data(std::move(ffi_data)) {
    }
    unique_ptr<CData> ffi_data;
};

struct VortexBindResults {
    vector<LogicalType> &return_types;
    vector<string> &names;
};

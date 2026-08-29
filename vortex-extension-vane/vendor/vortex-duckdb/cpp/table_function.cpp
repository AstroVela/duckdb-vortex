// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "data.hpp"
#include "error.hpp"
#include "table_function.hpp"
#include "expr.h"
#include "vortex_duckdb.h"
#include "table_function.h"
#include "vortex.h"

#include "duckdb.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#ifdef VORTEX_VANE_DISTRIBUTED
#include <algorithm>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/distributed_table_function.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#endif

using namespace std::string_literals;
constexpr column_t COLUMN_IDENTIFIER_FILE_INDEX = MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX;
constexpr column_t COLUMN_IDENTIFIER_FILE_ROW_NUMBER = MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;

unique_ptr<FunctionData> VortexBindData::Copy() const {
#ifdef VORTEX_VANE_DISTRIBUTED
    unique_ptr<CData> ffi_data_copy;
    if (ffi_data) {
        const auto copied_ffi_data = duckdb_table_function_bind_data_clone(ffi_data->DataPtr());
        ffi_data_copy = unique_ptr<CData>(reinterpret_cast<CData *>(copied_ffi_data));
    }
    auto result = make_uniq<VortexBindData>(std::move(ffi_data_copy), types, names);
    result->scan_split_set_id = scan_split_set_id;
    result->portable_bind = portable_bind;
    result->distributed_files = distributed_files;
    result->aggregate_scan = aggregate_scan;
    result->explicit_split_mode = explicit_split_mode;
    result->splits_applied = splits_applied;
    result->eligible_file_indexes = eligible_file_indexes;
    result->assigned_file_indexes = assigned_file_indexes;
    return result;
#else
    const auto copied_ffi_data = duckdb_table_function_bind_data_clone(ffi_data->DataPtr());
    auto ffi_data_p = unique_ptr<CData>(reinterpret_cast<CData *>(copied_ffi_data));
    return make_uniq<VortexBindData>(std::move(ffi_data_p), types);
#endif
}

bool VortexBindData::Equals(const FunctionData &other_base) const {
    const VortexBindData &other = other_base.Cast<VortexBindData>();
#ifdef VORTEX_VANE_DISTRIBUTED
    if (ffi_data || other.ffi_data) {
        // Runtime bind equality retains the upstream pointer-identity semantics.
        return ffi_data.get() == other.ffi_data.get();
    }
    return types == other.types && names == other.names && scan_split_set_id == other.scan_split_set_id &&
           portable_bind == other.portable_bind && distributed_files == other.distributed_files &&
           aggregate_scan == other.aggregate_scan && explicit_split_mode == other.explicit_split_mode &&
           splits_applied == other.splits_applied && eligible_file_indexes == other.eligible_file_indexes &&
           assigned_file_indexes == other.assigned_file_indexes;
#else
    // if "types" are different, "ffi_data" would also be different as it
    // contains types inside, so omit "types" from comparison.
    return ffi_data.get() == other.ffi_data.get();
#endif
}

#ifdef VORTEX_VANE_DISTRIBUTED
VortexBindData::PortableSnapshot VortexBindData::CreatePortableSnapshot() const {
    hugeint_t parsed_scan_id;
    if (!UUID::FromString(scan_split_set_id, parsed_scan_id, true) ||
        UUID::ToString(parsed_scan_id) != scan_split_set_id) {
        throw SerializationException("Vortex bind has an invalid distributed scan identity");
    }
    if (!ffi_data) {
        if (portable_bind.empty()) {
            throw SerializationException("Deserialized Vortex bind has no portable state");
        }
        return {portable_bind, distributed_files, aggregate_scan};
    }

    PortableSnapshot result;
    duckdb_vx_error error_out = nullptr;
    auto portable_data = duckdb_table_function_distributed_bind_serialize(ffi_data->DataPtr(), &error_out);
    if (error_out) {
        throw SerializationException(IntoErrString(error_out));
    }
    if (!portable_data) {
        throw SerializationException("Vortex failed to serialize distributed bind state");
    }
    auto portable = unique_ptr<CData>(reinterpret_cast<CData *>(portable_data));
    size_t portable_size = 0;
    const auto portable_bytes =
        duckdb_table_function_distributed_bind_bytes(portable->DataPtr(), &portable_size);
    if (!portable_bytes || portable_size == 0) {
        throw SerializationException("Vortex produced empty distributed bind state");
    }
    result.portable_bind.assign(reinterpret_cast<const char *>(portable_bytes), portable_size);

    auto file_count = duckdb_table_function_distributed_file_count(portable->DataPtr());
    result.aggregate_scan = duckdb_table_function_distributed_is_aggregate(portable->DataPtr());
    vector<DistributedFile> files;
    files.reserve(file_count);
    for (idx_t file_index = 0; file_index < file_count; file_index++) {
        VortexDistributedFileView view {};
        if (!duckdb_table_function_distributed_file_at(portable->DataPtr(), file_index, &view) ||
            !view.source_url || view.source_url_len == 0 || !view.path || view.path_len == 0 ||
            view.size == DConstants::INVALID_INDEX) {
            throw SerializationException("Vortex produced an invalid distributed file at index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
        DistributedFile file;
        file.source_url.assign(reinterpret_cast<const char *>(view.source_url), view.source_url_len);
        file.path.assign(reinterpret_cast<const char *>(view.path), view.path_len);
        file.size = view.size;
        files.push_back(std::move(file));
    }
    result.distributed_files = std::move(files);
    return result;
}
#endif

// This is a flaw of Duckdb API which doesn't allow passing non-const
// expressions. We never modify the value on Rust side.
static duckdb_vx_expr get_ffi_expr(const Expression &expr) {
    return reinterpret_cast<duckdb_vx_expr>(const_cast<Expression *>(&expr));
}

static void *get_ffi_bind(const FunctionData *bind_data) {
#ifdef VORTEX_VANE_DISTRIBUTED
    auto &bind = bind_data->Cast<VortexBindData>();
    if (!bind.ffi_data) {
        throw InternalException("Vortex runtime bind state is not initialized");
    }
    return bind.ffi_data->DataPtr();
#else
    return bind_data->Cast<VortexBindData>().ffi_data->DataPtr();
#endif
}

static void *get_ffi_global(GlobalTableFunctionState *state) {
    auto &global = state->Cast<VortexGlobalData>();
    auto ffi_global = global.ffi_data->DataPtr();
#ifdef VORTEX_VANE_DISTRIBUTED
    if (global.distributed) {
        return duckdb_table_function_distributed_global_data(ffi_global);
    }
#endif
    return ffi_global;
}

static void *get_ffi_local(LocalTableFunctionState *state) {
    return state->Cast<VortexLocalData>().ffi_data->DataPtr();
}

double
table_scan_progress(ClientContext &, const FunctionData *, const GlobalTableFunctionState *global_state) {
    auto &global = global_state->Cast<VortexGlobalData>();
    auto c_global_state = global.ffi_data->DataPtr();
#ifdef VORTEX_VANE_DISTRIBUTED
    if (global.distributed) {
        c_global_state = duckdb_table_function_distributed_global_data(c_global_state);
    }
#endif
    return duckdb_table_function_scan_progress(c_global_state);
}

static Value &UnwrapValue(duckdb_value value) {
    return *(reinterpret_cast<Value *>(value));
}

struct OwnedColumnStatistics {
    ~OwnedColumnStatistics() {
        if (value.min) {
            duckdb_destroy_value(&value.min);
        }
        if (value.max) {
            duckdb_destroy_value(&value.max);
        }
    }

    duckdb_column_statistics value {};
};

unique_ptr<BaseStatistics> numeric_stats(duckdb_column_statistics &stats, LogicalType type) {
    BaseStatistics out = NumericStats::CreateUnknown(type);
    if (stats.min) {
        NumericStats::SetMin(out, UnwrapValue(stats.min));
        duckdb_destroy_value(&stats.min);
    }
    if (stats.max) {
        NumericStats::SetMax(out, UnwrapValue(stats.max));
        duckdb_destroy_value(&stats.max);
    }
    if (!stats.has_null) {
        out.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
    }
    return out.ToUnique();
}

unique_ptr<BaseStatistics> string_stats(duckdb_column_statistics &stats, LogicalType type) {
    BaseStatistics out = StringStats::CreateUnknown(type);
    if (stats.min) {
        StringStats::SetMin(out, StringValue::Get(UnwrapValue(stats.min)));
        duckdb_destroy_value(&stats.min);
    }
    if (stats.max) {
        StringStats::SetMax(out, StringValue::Get(UnwrapValue(stats.max)));
        duckdb_destroy_value(&stats.max);
    }
    if (stats.max_string_length >> 63) {
        StringStats::SetMaxStringLength(out, uint32_t(stats.max_string_length));
    }
    if (!stats.has_null) {
        out.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
    }

    return out.ToUnique();
}

unique_ptr<BaseStatistics> base_stats(duckdb_column_statistics &stats, LogicalType type) {
    BaseStatistics out = BaseStatistics::CreateUnknown(type);
    if (!stats.has_null) {
        out.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
    }
    return out.ToUnique();
}

unique_ptr<BaseStatistics> statistics(ClientContext &, const FunctionData *bind_data, column_t column_index) {
    if (IsVirtualColumn(column_index)) {
        return {};
    }

    const auto &bind = bind_data->Cast<VortexBindData>();
#ifdef VORTEX_VANE_DISTRIBUTED
    if (!bind.ffi_data) {
        return {};
    }
#endif
    const void *const ffi_bind = get_ffi_bind(bind_data);

    OwnedColumnStatistics owned_statistics;
    auto &statistics = owned_statistics.value;
    if (!duckdb_table_function_statistics(ffi_bind, column_index, &statistics)) {
        return {};
    }

    const LogicalType type = bind.types[column_index];

    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::BIGINT:
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
    case LogicalTypeId::UBIGINT:
    case LogicalTypeId::UHUGEINT:
    case LogicalTypeId::HUGEINT: {
        return numeric_stats(statistics, type);
    }
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB: {
        return string_stats(statistics, type);
    }
    case LogicalTypeId::STRUCT: {
        // TODO(myrrc)
        // Duckdb's has_null has a different semantics for structs.
        // If we propagate our has_null, this breaks Duckdb optimizer.
        // You can reproduce it in struct.slt test in vortex-sqllogictests:
        return {};
    }
    default:
        return base_stats(statistics, type);
    }
}

bool projection_expression_pushdown(ClientContext &, const TableFunctionProjectionExpressionInput &input) {
#ifdef VORTEX_VANE_DISTRIBUTED
    // A logical-plan round trip reconstructs the bind from portable owned data.
    // There is deliberately no live Rust reader until a worker receives splits
    // and enters init_global with its real ClientContext. Keep any newly
    // discovered projection expression above the scan in that detached state.
    if (!input.get.bind_data->Cast<VortexBindData>().ffi_data) {
        return false;
    }
#endif
    duckdb_vx_expr ffi_expr = get_ffi_expr(input.expression);
    void *const ffi_bind = get_ffi_bind(input.get.bind_data.get());
    duckdb_vx_error error_out = nullptr;

    const bool ret = duckdb_table_function_pushdown_projection_expression( //
        ffi_bind,
        ffi_expr,
        input.projection_idx,
        &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }
    return ret;
}

extern "C" {
idx_t duckdb_vx_aggregate_len(duckdb_vx_agg_input ffi_input) {
    return reinterpret_cast<const TableFunctionUngroupedAggregateInput *>(ffi_input)->projections.size();
}

duckdb_vx_expr duckdb_vx_aggregate_at(duckdb_vx_agg_input ffi_input, idx_t i, idx_t *proj_idx) {
    const auto &input = *reinterpret_cast<const TableFunctionUngroupedAggregateInput *>(ffi_input);
    const auto &[scan_index, expr] = input.projections[i];
    *proj_idx = scan_index == COUNT_STAR_PROJ_IDX ? scan_index
                                                  : input.get.GetColumnIds()[scan_index].GetPrimaryIndex();
    return get_ffi_expr(expr);
}
}

bool aggregate_pushdown(ClientContext &, const TableFunctionUngroupedAggregateInput &input) {
#ifdef VORTEX_VANE_DISTRIBUTED
    // TryReplaceAggregate rewrites the LogicalGet's column IDs to aggregate
    // result positions. DuckDB table filters still refer to the original scan
    // columns, so retaining them would either attach a filter to the wrong
    // field or make physical planning fail (in particular for virtual file
    // columns). Keep the upper aggregate whenever table filters remain.
    if (!input.get.table_filters.filters.empty()) {
        return false;
    }
    // See projection_expression_pushdown: detached binds only contain the
    // serialized scan recipe, not a connection-scoped optimizer reader.
    if (!input.get.bind_data->Cast<VortexBindData>().ffi_data) {
        return false;
    }
#endif
    void *const ffi_bind = get_ffi_bind(input.get.bind_data.get());
    duckdb_vx_error error_out = nullptr;
    const auto ffi_input =
        reinterpret_cast<duckdb_vx_agg_input>(const_cast<TableFunctionUngroupedAggregateInput *>(&input));
    const bool res = duckdb_table_function_pushdown_projection_aggregates(ffi_bind, ffi_input, &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }
    return res;
}

unique_ptr<FunctionData> duckdb_vx_table_function_bind(ClientContext &,
                                                       TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types,
                                                       vector<string> &names) {
    VortexBindResults bind_results = {return_types, names};

    duckdb_vx_error error_out = nullptr;
    duckdb_vx_tfunc_bind_input bind_input = reinterpret_cast<duckdb_vx_tfunc_bind_input>(&input);
    duckdb_vx_tfunc_bind_result bind_result = reinterpret_cast<duckdb_vx_tfunc_bind_result>(&bind_results);
    duckdb_vx_data ffi_bind_data = duckdb_table_function_bind(bind_input, bind_result, &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }

    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_bind_data));
#ifdef VORTEX_VANE_DISTRIBUTED
    auto result = make_uniq<VortexBindData>(std::move(cdata), return_types, names);
    result->scan_split_set_id = UUID::ToString(UUID::GenerateRandomUUID());
    return result;
#else
    return make_uniq<VortexBindData>(std::move(cdata), return_types);
#endif
}

unique_ptr<GlobalTableFunctionState> init_global(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<VortexBindData>();
#ifdef VORTEX_VANE_DISTRIBUTED
    if (bind_data.explicit_split_mode && !bind_data.splits_applied) {
        throw InvalidInputException(
            "Detached distributed Vortex worker bind has no explicit split assignment");
    }
#endif

    duckdb_vx_tfunc_init_input ffi_input = {
        .bind_data = bind_data.ffi_data ? bind_data.ffi_data->DataPtr() : nullptr,
        .column_ids = input.column_ids.data(),
        .column_ids_count = input.column_ids.size(),
        .projection_ids = input.projection_ids.data(),
        .projection_ids_count = input.projection_ids.size(),
        .filters = reinterpret_cast<duckdb_vx_table_filter_set>(input.filters.get()),
        .client_context = reinterpret_cast<duckdb_client_context>(&context),
    };

    duckdb_vx_error error_out = nullptr;
    duckdb_vx_data ffi_global_data;
#ifdef VORTEX_VANE_DISTRIBUTED
    bool distributed = false;
    if (!bind_data.ffi_data) {
        const auto snapshot = bind_data.CreatePortableSnapshot();
        vector<idx_t> native_file_indexes;
        const vector<idx_t> *runtime_file_indexes = &bind_data.assigned_file_indexes;
        if (!bind_data.explicit_split_mode) {
            native_file_indexes.reserve(snapshot.distributed_files.size());
            for (idx_t file_index = 0; file_index < snapshot.distributed_files.size(); file_index++) {
                native_file_indexes.push_back(file_index);
            }
            runtime_file_indexes = &native_file_indexes;
        }
        // Optional filters (for example TopN's dynamic bound) are maintained
        // by an upstream operator that is absent from Vane's detached scan
        // plan. They are pruning hints, so ignoring them is correctness-safe;
        // required table filters still pass through unchanged.
        ffi_global_data = duckdb_table_function_init_global_distributed(
            reinterpret_cast<const uint8_t *>(snapshot.portable_bind.data()),
            snapshot.portable_bind.size(),
            runtime_file_indexes->data(),
            runtime_file_indexes->size(),
            bind_data.explicit_split_mode,
            &ffi_input,
            &error_out);
        distributed = true;
    } else
#endif
    {
        ffi_global_data = duckdb_table_function_init_global(&ffi_input, &error_out);
    }
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }

    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_global_data));
#ifdef VORTEX_VANE_DISTRIBUTED
    bool force_empty_output = false;
    force_empty_output = distributed && bind_data.explicit_split_mode && bind_data.splits_applied &&
                         bind_data.assigned_file_indexes.empty();
    return make_uniq<VortexGlobalData>(std::move(cdata), distributed, force_empty_output);
#else
    return make_uniq<VortexGlobalData>(std::move(cdata));
#endif
}

unique_ptr<LocalTableFunctionState>
init_local(ExecutionContext &, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
    const void *ffi_bind;
#ifdef VORTEX_VANE_DISTRIBUTED
    auto &global = global_state->Cast<VortexGlobalData>();
    if (global.distributed) {
        ffi_bind = duckdb_table_function_distributed_bind_data(global.ffi_data->DataPtr());
    } else
#endif
    {
        ffi_bind = get_ffi_bind(input.bind_data.get());
    }
    void *const ffi_global = get_ffi_global(global_state);

    duckdb_vx_data ffi_local_data = duckdb_table_function_init_local(ffi_bind, ffi_global);
    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_local_data));
    return make_uniq<VortexLocalData>(std::move(cdata));
}

void function(ClientContext &, TableFunctionInput &input, DataChunk &output) {
#ifdef VORTEX_VANE_DISTRIBUTED
    auto &global = input.global_state->Cast<VortexGlobalData>();
    if (global.force_empty_output) {
        output.SetCardinality(0);
        return;
    }
#endif
    void *const ffi_global = get_ffi_global(input.global_state.get());
    void *const ffi_local = get_ffi_local(input.local_state.get());

    duckdb_data_chunk chunk = reinterpret_cast<duckdb_data_chunk>(&output);
    duckdb_vx_error error_out = nullptr;
    duckdb_table_function_scan(ffi_global, ffi_local, chunk, &error_out);
    if (error_out) {
        throw InvalidInputException(IntoErrString(error_out));
    }
}

using FilterVec = vector<unique_ptr<Expression>>;

void pushdown_complex_filter(const FunctionData &bind_data, FilterVec &filters) {
#ifdef VORTEX_VANE_DISTRIBUTED
    // Leave filters in DuckDB's plan when optimizing a detached bind. The
    // worker will still apply already-serialized Vortex filters and its table
    // filters, without constructing a reader during deserialization.
    if (!bind_data.Cast<VortexBindData>().ffi_data) {
        return;
    }
#endif
    void *const ffi_bind = get_ffi_bind(&bind_data);
    duckdb_vx_error error_out = nullptr;

    for (auto iter = filters.begin(); iter != filters.end();) {
        duckdb_vx_expr ffi_expr = reinterpret_cast<duckdb_vx_expr>(iter->get());

        const bool pushed = duckdb_table_function_pushdown_complex_filter(ffi_bind, ffi_expr, &error_out);
        if (error_out) {
            throw BinderException(IntoErrString(error_out));
        }
        iter = pushed ? filters.erase(iter) : std::next(iter);
    }
}

unique_ptr<NodeStatistics> cardinality(ClientContext &, const FunctionData *bind_data) {
#ifdef VORTEX_VANE_DISTRIBUTED
    if (!bind_data->Cast<VortexBindData>().ffi_data) {
        return make_uniq<NodeStatistics>();
    }
#endif
    const void *const ffi_bind = get_ffi_bind(bind_data);

    duckdb_vx_node_statistics stats = {};
    duckdb_table_function_cardinality(ffi_bind, &stats);

    auto out = make_uniq<NodeStatistics>();
    out->has_estimated_cardinality = stats.has_estimated_cardinality;
    out->estimated_cardinality = stats.estimated_cardinality;
    out->has_max_cardinality = stats.has_max_cardinality;
    out->max_cardinality = stats.max_cardinality;

    return out;
}

extern "C" duckdb_value duckdb_vx_tfunc_bind_input_get_parameter(duckdb_vx_tfunc_bind_input ffi_input,
                                                                 size_t index) {
    D_ASSERT(ffi_input);
    const TableFunctionBindInput &input = *reinterpret_cast<TableFunctionBindInput *>(ffi_input);
    return reinterpret_cast<duckdb_value>(new Value(input.inputs[index]));
}

extern "C" void duckdb_vx_tfunc_bind_result_add_column(duckdb_vx_tfunc_bind_result ffi_result,
                                                       const char *name_str,
                                                       size_t name_len,
                                                       duckdb_logical_type ffi_type) {
    D_ASSERT(ffi_result);
    D_ASSERT(name_str);
    D_ASSERT(ffi_type);
    const VortexBindResults &result = *reinterpret_cast<VortexBindResults *>(ffi_result);
    const LogicalType logical_type = *reinterpret_cast<LogicalType *>(ffi_type);

    result.names.emplace_back(name_str, name_len);
    result.return_types.emplace_back(logical_type);
}

/**
 * Called at planning time to determine whether data is partitioned by a
 * given set of columns. Requested columns are GROUP BY parameters i.e. columns
 * over which the query aggregates.
 */
TablePartitionInfo get_partition_info(ClientContext &, TableFunctionPartitionInput &input) {
    const vector<column_t> &ids = input.partition_ids;
    // Our data is partitioned by array exporters. Each exporter processes a
    // single Array which belongs to a single file. If data is partitioned only
    // by file_index, there is one unique value for an Array. Otherwise there
    // may be multiple values.
    return (ids.size() == 1 && ids[0] == COLUMN_IDENTIFIER_FILE_INDEX)
               ? TablePartitionInfo::SINGLE_VALUE_PARTITIONS
               : TablePartitionInfo::NOT_PARTITIONED;
}

OperatorPartitionData get_partition_data(ClientContext &, TableFunctionGetPartitionInput &input) {
    void *const ffi_global = get_ffi_global(input.global_state.get());
    void *const ffi_local = get_ffi_local(input.local_state.get());
    duckdb_vx_partition_data partition_data;
    duckdb_table_function_get_partition_data(ffi_global, ffi_local, &partition_data);

    OperatorPartitionData out(partition_data.partition_index);

    // file_index_column_pos may be INVALID_IDX, but column_index will never
    // be INVALID_IDX, so we can compare directly
    for (const column_t column_index : input.partition_info.partition_columns) {
        if (column_index == partition_data.file_index_column_pos) {
            out.partition_data.emplace_back(Value::UBIGINT(partition_data.file_index));
        } else {
            throw InternalException(StringUtil::Format(
                "get_partition_data: requested column_index %d is not constant for given partition",
                column_index));
        }
    }
    return out;
}

extern "C" void duckdb_vx_string_map_insert(duckdb_vx_string_map map, const char *key, const char *value) {
    D_ASSERT(map);
    D_ASSERT(key);
    D_ASSERT(value);
    reinterpret_cast<InsertionOrderPreservingMap<string> *>(map)->insert(key, value);
}

InsertionOrderPreservingMap<string> to_string(TableFunctionToStringInput &input) {
    InsertionOrderPreservingMap<string> result;
#ifdef VORTEX_VANE_DISTRIBUTED
    auto &bind_data = input.bind_data->Cast<VortexBindData>();
    if (!bind_data.ffi_data) {
        result.insert("Function", "Vortex Scan");
        const auto file_count = bind_data.explicit_split_mode ? bind_data.assigned_file_indexes.size()
                                                              : bind_data.distributed_files.size();
        result.insert("Distributed files", std::to_string(file_count));
        return result;
    }
#endif
    duckdb_vx_string_map ffi_map = reinterpret_cast<duckdb_vx_string_map>(&result);
    const void *const ffi_bind = get_ffi_bind(input.bind_data.get());
    duckdb_table_function_to_string(ffi_bind, ffi_map);
    return result;
}

#ifdef VORTEX_VANE_DISTRIBUTED
namespace {

static constexpr uint8_t VORTEX_SPLIT_PAYLOAD_VERSION = 1;
static constexpr uint8_t VORTEX_BIND_SERDE_VERSION = 1;
static constexpr const char *VORTEX_SPLIT_CODEC = "vane.vortex-file-split";

static bool IsCanonicalVortexScanId(const string &scan_split_set_id) {
    hugeint_t parsed;
    return UUID::FromString(scan_split_set_id, parsed, true) && UUID::ToString(parsed) == scan_split_set_id;
}

static bool IsCanonicalVortexFilePath(const string &path) {
    if (path.empty() || path.front() == '/' || path.back() == '/' || path.find('\0') != string::npos) {
        return false;
    }
    idx_t segment_start = 0;
    while (segment_start < path.size()) {
        auto segment_end = path.find('/', segment_start);
        if (segment_end == string::npos) {
            segment_end = path.size();
        }
        const auto segment_size = segment_end - segment_start;
        if (segment_size == 0 || (segment_size == 1 && path[segment_start] == '.') ||
            (segment_size == 2 && path[segment_start] == '.' && path[segment_start + 1] == '.')) {
            return false;
        }
        segment_start = segment_end + 1;
    }
    return true;
}

static void AppendSplitByte(string &result, uint8_t value) {
    result.push_back(static_cast<char>(value));
}

static void AppendSplitU64(string &result, uint64_t value) {
    for (idx_t byte_index = 0; byte_index < sizeof(value); byte_index++) {
        AppendSplitByte(result, static_cast<uint8_t>((value >> (byte_index * 8U)) & 0xffU));
    }
}

static void AppendSplitString(string &result, const string &value) {
    AppendSplitU64(result, value.size());
    result.append(value);
}

// Binary payload v1:
//   "VXSP" | u8 version | string scan_split_set_id | u64 file_count |
//   repeated(u64 stable_file_index | string source_url | string path |
//            u64 immutable_size)
// Normal scans encode one file. Aggregate-pushed scans encode their complete
// pruned file set so a single worker computes the final aggregate exactly once.
static string EncodeVortexSplit(const vector<idx_t> &file_indexes,
                                const vector<VortexBindData::DistributedFile> &distributed_files,
                                const string &scan_split_set_id) {
    if (file_indexes.empty()) {
        throw InternalException("Cannot encode an empty distributed Vortex split");
    }
    if (!IsCanonicalVortexScanId(scan_split_set_id)) {
        throw InternalException("Cannot encode a distributed Vortex split without a canonical scan identity");
    }
    string result("VXSP", 4);
    AppendSplitByte(result, VORTEX_SPLIT_PAYLOAD_VERSION);
    AppendSplitString(result, scan_split_set_id);
    AppendSplitU64(result, file_indexes.size());
    for (auto file_index : file_indexes) {
        if (file_index >= distributed_files.size()) {
            throw InternalException("Cannot encode unknown distributed Vortex file index %llu",
                                    static_cast<unsigned long long>(file_index));
        }
        const auto &file = distributed_files[file_index];
        AppendSplitU64(result, file_index);
        AppendSplitString(result, file.source_url);
        AppendSplitString(result, file.path);
        AppendSplitU64(result, file.size);
    }
    return result;
}

class VortexSplitDecoder {
public:
    explicit VortexSplitDecoder(const string &payload_p) : payload(payload_p) {
    }

    uint8_t ReadByte() {
        if (offset >= payload.size()) {
            throw InvalidInputException("Truncated distributed Vortex split payload");
        }
        return static_cast<uint8_t>(payload[offset++]);
    }

    uint64_t ReadU64() {
        uint64_t result = 0;
        for (idx_t byte_index = 0; byte_index < sizeof(result); byte_index++) {
            result |= static_cast<uint64_t>(ReadByte()) << (byte_index * 8U);
        }
        return result;
    }

    string ReadString() {
        auto size = ReadU64();
        if (size > payload.size() - offset) {
            throw InvalidInputException("Invalid string length in distributed Vortex split payload");
        }
        auto result = payload.substr(offset, size);
        offset += size;
        return result;
    }

    void Finish() const {
        if (offset != payload.size()) {
            throw InvalidInputException("Distributed Vortex split payload contains trailing bytes");
        }
    }

private:
    const string &payload;
    idx_t offset = 0;
};

struct DecodedVortexFile {
    idx_t file_index;
    VortexBindData::DistributedFile file;
};

struct DecodedVortexSplit {
    string scan_split_set_id;
    vector<DecodedVortexFile> files;
};

static DecodedVortexSplit DecodeVortexSplit(const string &payload) {
    if (payload.size() < 5 || payload.compare(0, 4, "VXSP") != 0) {
        throw InvalidInputException("Invalid distributed Vortex split payload magic");
    }
    VortexSplitDecoder decoder(payload);
    for (idx_t magic_index = 0; magic_index < 4; magic_index++) {
        decoder.ReadByte();
    }
    auto version = decoder.ReadByte();
    if (version != VORTEX_SPLIT_PAYLOAD_VERSION) {
        throw InvalidInputException("Unsupported distributed Vortex split payload version %u", version);
    }
    DecodedVortexSplit result;
    result.scan_split_set_id = decoder.ReadString();
    if (!IsCanonicalVortexScanId(result.scan_split_set_id)) {
        throw InvalidInputException("Distributed Vortex split contains an invalid scan identity");
    }
    auto file_count = decoder.ReadU64();
    if (file_count == 0 || file_count > payload.size()) {
        throw InvalidInputException("Invalid file count in distributed Vortex split payload");
    }
    result.files.reserve(file_count);
    for (idx_t file_offset = 0; file_offset < file_count; file_offset++) {
        DecodedVortexFile decoded;
        decoded.file_index = decoder.ReadU64();
        decoded.file.source_url = decoder.ReadString();
        decoded.file.path = decoder.ReadString();
        decoded.file.size = decoder.ReadU64();
        if (decoded.file.size == DConstants::INVALID_INDEX) {
            throw InvalidInputException("Distributed Vortex split contains an invalid file size");
        }
        if (decoded.file.source_url.empty() || !IsCanonicalVortexFilePath(decoded.file.path)) {
            throw InvalidInputException("Distributed Vortex split contains an invalid file identity");
        }
        result.files.push_back(std::move(decoded));
    }
    decoder.Finish();
    return result;
}

static bool IsCanonicalVortexSplitId(const string &split_id) {
    if (split_id.empty()) {
        return false;
    }
    idx_t segment_start = 0;
    optional_idx previous;
    while (segment_start < split_id.size()) {
        auto segment_end = split_id.find(',', segment_start);
        if (segment_end == string::npos) {
            segment_end = split_id.size();
        }
        if (segment_end == segment_start ||
            (segment_end - segment_start > 1 && split_id[segment_start] == '0')) {
            return false;
        }
        idx_t value = 0;
        for (idx_t offset = segment_start; offset < segment_end; offset++) {
            auto character = split_id[offset];
            if (character < '0' || character > '9') {
                return false;
            }
            auto digit = static_cast<idx_t>(character - '0');
            if (value > (NumericLimits<idx_t>::Maximum() - digit) / 10) {
                return false;
            }
            value = value * 10 + digit;
        }
        if (previous.IsValid() && previous.GetIndex() >= value) {
            return false;
        }
        previous = optional_idx(value);
        if (segment_end == split_id.size()) {
            return true;
        }
        segment_start = segment_end + 1;
    }
    return false;
}

static string CanonicalVortexSplitId(const vector<idx_t> &file_indexes) {
    string result;
    for (auto file_index : file_indexes) {
        if (!result.empty()) {
            result += ',';
        }
        result += std::to_string(file_index);
    }
    return result;
}

static idx_t SaturatingVortexSplitEstimate(idx_t left, idx_t right) {
    // idx_t(-1) is reserved by optional_idx as its invalid sentinel.
    const auto maximum = NumericLimits<idx_t>::Maximum() - 1;
    return right > maximum - left ? maximum : left + right;
}

static idx_t ProportionalVortexSplitEstimate(idx_t total, idx_t numerator, idx_t denominator) {
    D_ASSERT(denominator > 0 && numerator <= denominator);
    if (numerator == 0) {
        return 0;
    }
    if (numerator == denominator) {
        return total;
    }
    const auto scaled = static_cast<long double>(total) * static_cast<long double>(numerator) /
                        static_cast<long double>(denominator);
    // On platforms where long double is IEEE double, UINT64_MAX - 1 rounds to
    // 2^64. Clamp in floating point before the integer conversion so an
    // extreme estimate cannot invoke an out-of-range conversion.
    if (scaled >= static_cast<long double>(total)) {
        return total;
    }
    return static_cast<idx_t>(scaled);
}

static bool SameDistributedFile(const VortexBindData::DistributedFile &left,
                                const VortexBindData::DistributedFile &right) {
    return left.source_url == right.source_url && left.path == right.path && left.size == right.size;
}

static void ValidatePortableVortexBind(const string &portable_bind,
                                       const vector<VortexBindData::DistributedFile> &files,
                                       bool aggregate_scan) {
    duckdb_vx_error error_out = nullptr;
    auto decoded_data = duckdb_table_function_distributed_bind_deserialize(
        reinterpret_cast<const uint8_t *>(portable_bind.data()),
        portable_bind.size(),
        &error_out);
    if (error_out) {
        throw SerializationException(IntoErrString(error_out));
    }
    if (!decoded_data) {
        throw SerializationException("Vortex failed to decode distributed bind state");
    }
    auto decoded = unique_ptr<CData>(reinterpret_cast<CData *>(decoded_data));
    if (duckdb_table_function_distributed_is_aggregate(decoded->DataPtr()) != aggregate_scan ||
        duckdb_table_function_distributed_file_count(decoded->DataPtr()) != files.size()) {
        throw SerializationException("Serialized Vortex bind metadata does not match its portable state");
    }
    for (idx_t file_index = 0; file_index < files.size(); file_index++) {
        VortexDistributedFileView view {};
        if (!duckdb_table_function_distributed_file_at(decoded->DataPtr(), file_index, &view) ||
            !view.source_url || !view.path || view.size == DConstants::INVALID_INDEX) {
            throw SerializationException("Portable Vortex bind contains an invalid file at index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
        VortexBindData::DistributedFile portable_file;
        portable_file.source_url.assign(reinterpret_cast<const char *>(view.source_url), view.source_url_len);
        portable_file.path.assign(reinterpret_cast<const char *>(view.path), view.path_len);
        portable_file.size = view.size;
        if (!SameDistributedFile(portable_file, files[file_index])) {
            throw SerializationException("Serialized Vortex file metadata differs from its portable bind");
        }
    }
}

static void VortexScanSerialize(Serializer &serializer,
                                const optional_ptr<FunctionData> bind_data,
                                const TableFunction &) {
    if (!bind_data) {
        throw SerializationException("Distributed Vortex scan requires bind data");
    }
    auto &data = bind_data->Cast<VortexBindData>();
    if (!IsCanonicalVortexScanId(data.scan_split_set_id)) {
        throw SerializationException("Distributed Vortex bind has an invalid scan identity");
    }
    auto snapshot = data.CreatePortableSnapshot();
    vector<string> source_urls;
    vector<string> paths;
    vector<uint64_t> sizes;
    source_urls.reserve(snapshot.distributed_files.size());
    paths.reserve(snapshot.distributed_files.size());
    sizes.reserve(snapshot.distributed_files.size());
    for (const auto &file : snapshot.distributed_files) {
        if (file.source_url.empty() || file.source_url.find('\0') != string::npos ||
            !IsCanonicalVortexFilePath(file.path) || file.size == DConstants::INVALID_INDEX) {
            throw SerializationException("Distributed Vortex bind contains an invalid file identity");
        }
        source_urls.push_back(file.source_url);
        paths.push_back(file.path);
        sizes.push_back(file.size);
    }
    serializer.WriteProperty(99, "format_version", VORTEX_BIND_SERDE_VERSION);
    serializer.WriteProperty(100, "types", data.types);
    serializer.WriteProperty(101, "names", data.names);
    serializer.WriteProperty(102, "portable_bind", snapshot.portable_bind);
    serializer.WriteProperty(103, "source_urls", source_urls);
    serializer.WriteProperty(104, "paths", paths);
    serializer.WriteProperty(105, "sizes", sizes);
    serializer.WriteProperty(106, "scan_split_set_id", data.scan_split_set_id);
    serializer.WriteProperty(107, "explicit_split_mode", data.explicit_split_mode);
    serializer.WriteProperty(108, "splits_applied", data.splits_applied);
    serializer.WriteProperty(109, "assigned_file_indexes", data.assigned_file_indexes);
    serializer.WriteProperty(110, "aggregate_scan", snapshot.aggregate_scan);
    serializer.WriteProperty(111, "eligible_file_indexes", data.eligible_file_indexes);
}

static unique_ptr<FunctionData> VortexScanDeserialize(Deserializer &deserializer, TableFunction &) {
    auto format_version = deserializer.ReadProperty<uint8_t>(99, "format_version");
    if (format_version != VORTEX_BIND_SERDE_VERSION) {
        throw SerializationException("Unsupported distributed Vortex bind format version %u", format_version);
    }
    auto types = deserializer.ReadProperty<vector<LogicalType>>(100, "types");
    auto names = deserializer.ReadProperty<vector<string>>(101, "names");
    auto portable_bind = deserializer.ReadProperty<string>(102, "portable_bind");
    auto source_urls = deserializer.ReadProperty<vector<string>>(103, "source_urls");
    auto paths = deserializer.ReadProperty<vector<string>>(104, "paths");
    auto sizes = deserializer.ReadProperty<vector<uint64_t>>(105, "sizes");
    auto scan_split_set_id = deserializer.ReadProperty<string>(106, "scan_split_set_id");
    auto explicit_split_mode = deserializer.ReadProperty<bool>(107, "explicit_split_mode");
    auto splits_applied = deserializer.ReadProperty<bool>(108, "splits_applied");
    auto assigned_file_indexes = deserializer.ReadProperty<vector<idx_t>>(109, "assigned_file_indexes");
    auto aggregate_scan = deserializer.ReadProperty<bool>(110, "aggregate_scan");
    auto eligible_file_indexes = deserializer.ReadProperty<vector<idx_t>>(111, "eligible_file_indexes");
    if (types.size() != names.size() || portable_bind.empty() ||
        !IsCanonicalVortexScanId(scan_split_set_id) || source_urls.size() != paths.size() ||
        source_urls.size() != sizes.size()) {
        throw SerializationException("Invalid serialized Vortex bind state");
    }
    vector<VortexBindData::DistributedFile> files;
    files.reserve(paths.size());
    for (idx_t file_index = 0; file_index < paths.size(); file_index++) {
        if (source_urls[file_index].empty() || source_urls[file_index].find('\0') != string::npos ||
            !IsCanonicalVortexFilePath(paths[file_index]) || sizes[file_index] == DConstants::INVALID_INDEX) {
            throw SerializationException("Invalid serialized Vortex file at index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
        VortexBindData::DistributedFile file;
        file.source_url = std::move(source_urls[file_index]);
        file.path = std::move(paths[file_index]);
        file.size = sizes[file_index];
        files.push_back(std::move(file));
    }
    ValidatePortableVortexBind(portable_bind, files, aggregate_scan);
    unordered_set<idx_t> eligible;
    optional_idx previous_eligible;
    for (auto file_index : eligible_file_indexes) {
        if (file_index >= files.size() || !eligible.insert(file_index).second ||
            (previous_eligible.IsValid() && previous_eligible.GetIndex() >= file_index)) {
            throw SerializationException("Invalid eligible Vortex file index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
        previous_eligible = optional_idx(file_index);
    }
    unordered_set<idx_t> assigned;
    optional_idx previous_assigned;
    for (auto file_index : assigned_file_indexes) {
        if (file_index >= files.size() || !assigned.insert(file_index).second ||
            !eligible.count(file_index) ||
            (previous_assigned.IsValid() && previous_assigned.GetIndex() >= file_index)) {
            throw SerializationException("Invalid assigned Vortex file index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
        previous_assigned = optional_idx(file_index);
    }
    if (!explicit_split_mode &&
        (splits_applied || !eligible_file_indexes.empty() || !assigned_file_indexes.empty())) {
        throw SerializationException("Native Vortex bind contains distributed split state");
    }
    if (!splits_applied && !assigned_file_indexes.empty()) {
        throw SerializationException(
            "Detached Vortex bind contains assigned files without an applied split batch");
    }
    if (aggregate_scan && splits_applied && !assigned_file_indexes.empty() &&
        assigned_file_indexes != eligible_file_indexes) {
        throw SerializationException(
            "Distributed aggregate Vortex bind contains an incomplete file assignment");
    }

    auto result = make_uniq<VortexBindData>(nullptr, types, names);
    result->scan_split_set_id = std::move(scan_split_set_id);
    result->portable_bind = std::move(portable_bind);
    result->distributed_files = std::move(files);
    result->aggregate_scan = aggregate_scan;
    result->explicit_split_mode = explicit_split_mode;
    result->splits_applied = splits_applied;
    result->eligible_file_indexes = std::move(eligible_file_indexes);
    result->assigned_file_indexes = std::move(assigned_file_indexes);
    return result;
}

static vector<idx_t> SelectDistributedVortexFiles(const TableFunctionDistributedScanInput &input,
                                                  idx_t file_count) {
    vector<idx_t> column_ids;
    column_ids.reserve(input.column_ids.size());
    for (const auto &column_id : input.column_ids) {
        column_ids.push_back(column_id.GetPrimaryIndex());
    }
    auto filters =
        reinterpret_cast<duckdb_vx_table_filter_set>(const_cast<TableFilterSet *>(input.table_filters.get()));
    vector<idx_t> selected_file_indexes;
    selected_file_indexes.reserve(file_count);
    for (idx_t file_index = 0; file_index < file_count; file_index++) {
        duckdb_vx_error error_out = nullptr;
        auto selected = duckdb_table_function_distributed_file_is_selected(filters,
                                                                           column_ids.data(),
                                                                           column_ids.size(),
                                                                           file_index,
                                                                           &error_out);
        if (error_out) {
            throw InvalidInputException(IntoErrString(error_out));
        }
        if (selected) {
            selected_file_indexes.push_back(file_index);
        }
    }
    return selected_file_indexes;
}

static const VortexBindData &
RequireDistributedVortexBindData(const TableFunctionDistributedScanInput &input) {
    if (!input.bind_data) {
        throw InvalidInputException("Distributed Vortex scan requires table-function bind data");
    }
    return input.bind_data->Cast<VortexBindData>();
}

static vector<DistributedScanSplit>
VortexPlanDistributedScanSplits(const TableFunctionDistributedScanPlanningInput &input) {
    auto &bind_data = RequireDistributedVortexBindData(input);
    if (bind_data.explicit_split_mode) {
        throw InvalidInputException("Distributed Vortex splits cannot be planned from a worker bind");
    }
    // Vane may serialize a logical coordinator plan before generating its
    // physical scan. Such a bind is intentionally detached from the original
    // connection, but its owned portable state and immutable file identities
    // are sufficient for deterministic split planning.
    // target_split_count is a granularity hint. The current stable Vortex
    // reader API exposes complete files, but not independently reopenable
    // fragments, so this callback deliberately keeps one split per file.
    auto snapshot = bind_data.CreatePortableSnapshot();
    auto selected_file_indexes = SelectDistributedVortexFiles(input, snapshot.distributed_files.size());
    vector<DistributedScanSplit> result;
    if (selected_file_indexes.empty()) {
        return result;
    }
    result.reserve(snapshot.aggregate_scan ? 1 : selected_file_indexes.size());
    idx_t total_bytes = 0;
    for (auto file_index : selected_file_indexes) {
        const auto &file = snapshot.distributed_files[file_index];
        total_bytes = SaturatingVortexSplitEstimate(total_bytes, file.size);
    }
    const auto has_estimated_rows = input.estimated_cardinality != DConstants::INVALID_INDEX;
    const auto estimated_rows = has_estimated_rows ? input.estimated_cardinality : 0;
    if (snapshot.aggregate_scan) {
        DistributedScanSplit split;
        split.split_id = CanonicalVortexSplitId(selected_file_indexes);
        split.payload =
            EncodeVortexSplit(selected_file_indexes, snapshot.distributed_files, bind_data.scan_split_set_id);
        split.estimated_bytes = optional_idx(total_bytes);
        split.estimated_cardinality = optional_idx(1);
        result.push_back(std::move(split));
        return result;
    }
    idx_t previous_cumulative_rows = 0;
    uint64_t cumulative_bytes = 0;
    for (idx_t selected_index = 0; selected_index < selected_file_indexes.size(); selected_index++) {
        const auto file_index = selected_file_indexes[selected_index];
        const auto &file = snapshot.distributed_files[file_index];
        vector<idx_t> split_files {file_index};
        DistributedScanSplit split;
        split.split_id = CanonicalVortexSplitId(split_files);
        split.payload =
            EncodeVortexSplit(split_files, snapshot.distributed_files, bind_data.scan_split_set_id);
        split.estimated_bytes = optional_idx(file.size);
        if (has_estimated_rows) {
            idx_t cumulative_rows;
            if (total_bytes > 0) {
                cumulative_bytes = SaturatingVortexSplitEstimate(cumulative_bytes, file.size);
                cumulative_rows =
                    ProportionalVortexSplitEstimate(estimated_rows, cumulative_bytes, total_bytes);
            } else {
                cumulative_rows = ProportionalVortexSplitEstimate(estimated_rows,
                                                                  selected_index + 1,
                                                                  selected_file_indexes.size());
            }
            split.estimated_cardinality = optional_idx(cumulative_rows - previous_cumulative_rows);
            previous_cumulative_rows = cumulative_rows;
        }
        result.push_back(std::move(split));
    }
    return result;
}

static unique_ptr<FunctionData>
VortexCreateDistributedWorkerBind(const TableFunctionDistributedScanInput &input) {
    auto &coordinator_bind = RequireDistributedVortexBindData(input);
    if (coordinator_bind.explicit_split_mode) {
        throw InvalidInputException(
            "Distributed Vortex worker bind cannot be created from another worker bind");
    }
    auto snapshot = coordinator_bind.CreatePortableSnapshot();
    auto eligible_file_indexes = SelectDistributedVortexFiles(input, snapshot.distributed_files.size());
    auto worker_bind = make_uniq<VortexBindData>(nullptr, coordinator_bind.types, coordinator_bind.names);
    worker_bind->scan_split_set_id = coordinator_bind.scan_split_set_id;
    worker_bind->portable_bind = std::move(snapshot.portable_bind);
    worker_bind->distributed_files = std::move(snapshot.distributed_files);
    worker_bind->aggregate_scan = snapshot.aggregate_scan;
    worker_bind->explicit_split_mode = true;
    worker_bind->splits_applied = false;
    worker_bind->eligible_file_indexes = std::move(eligible_file_indexes);
    return worker_bind;
}

static void VortexApplyDistributedSplits(optional_ptr<FunctionData> worker_bind_data,
                                         const vector<DistributedScanSplit> &splits) {
    if (!worker_bind_data) {
        throw InvalidInputException("Distributed Vortex scan requires worker bind data");
    }
    auto &bind_data = worker_bind_data->Cast<VortexBindData>();
    if (bind_data.ffi_data || !bind_data.explicit_split_mode) {
        throw InvalidInputException("Vortex distributed splits require a detached worker bind");
    }
    if (bind_data.aggregate_scan && splits.size() > 1) {
        throw InvalidInputException("Distributed aggregate Vortex scans require one complete file-set split");
    }
    unordered_set<string> split_ids;
    unordered_set<idx_t> file_indexes;
    unordered_set<idx_t> eligible_file_indexes(bind_data.eligible_file_indexes.begin(),
                                               bind_data.eligible_file_indexes.end());
    vector<idx_t> assigned;
    assigned.reserve(bind_data.distributed_files.size());
    for (const auto &split : splits) {
        split.Validate();
        if (!IsCanonicalVortexSplitId(split.split_id)) {
            throw InvalidInputException("Invalid distributed Vortex split id '%s'", split.split_id);
        }
        if (split.payload.empty()) {
            throw InvalidInputException("Distributed Vortex split '%s' has an empty payload", split.split_id);
        }
        if (!split_ids.insert(split.split_id).second) {
            throw InvalidInputException("Duplicate distributed Vortex split id '%s'", split.split_id);
        }
        auto decoded = DecodeVortexSplit(split.payload);
        if (decoded.scan_split_set_id != bind_data.scan_split_set_id) {
            throw InvalidInputException("Distributed Vortex split '%s' belongs to a different scan identity",
                                        split.split_id);
        }
        if (!bind_data.aggregate_scan && decoded.files.size() != 1) {
            throw InvalidInputException(
                "Non-aggregate distributed Vortex splits must reference exactly one file");
        }
        vector<idx_t> decoded_file_indexes;
        decoded_file_indexes.reserve(decoded.files.size());
        for (const auto &decoded_file : decoded.files) {
            if (decoded_file.file_index >= bind_data.distributed_files.size()) {
                throw InvalidInputException("Distributed Vortex split '%s' references an unknown file index",
                                            split.split_id);
            }
            if (!eligible_file_indexes.count(decoded_file.file_index)) {
                throw InvalidInputException(
                    "Distributed Vortex split '%s' references file index %llu outside the planned file set",
                    split.split_id,
                    static_cast<unsigned long long>(decoded_file.file_index));
            }
            if (!SameDistributedFile(decoded_file.file,
                                     bind_data.distributed_files[decoded_file.file_index])) {
                throw InvalidInputException(
                    "Distributed Vortex split '%s' does not match the bound file identity",
                    split.split_id);
            }
            if (!file_indexes.insert(decoded_file.file_index).second) {
                throw InvalidInputException(
                    "Distributed Vortex splits reference file index %llu more than once",
                    static_cast<unsigned long long>(decoded_file.file_index));
            }
            decoded_file_indexes.push_back(decoded_file.file_index);
            assigned.push_back(decoded_file.file_index);
        }
        if (split.split_id != CanonicalVortexSplitId(decoded_file_indexes)) {
            throw InvalidInputException(
                "Distributed Vortex split id '%s' does not match its payload file indexes",
                split.split_id);
        }
    }
    // A batch is a set of elementary scan splits. Canonicalize its file
    // assignment so transport or retry code may reorder those splits without
    // changing scan meaning or defeating idempotent re-application.
    std::sort(assigned.begin(), assigned.end());
    if (bind_data.aggregate_scan && !splits.empty() && assigned != bind_data.eligible_file_indexes) {
        throw InvalidInputException(
            "Distributed aggregate Vortex split does not contain the complete planned file set");
    }
    if (bind_data.splits_applied) {
        if (assigned != bind_data.assigned_file_indexes) {
            throw InvalidInputException(
                "Distributed Vortex bind already has a different explicit split assignment");
        }
        return;
    }
    bind_data.assigned_file_indexes = std::move(assigned);
    bind_data.splits_applied = true;
}

static TableFunctionDistributedScanCallbacks VortexDistributedScanCallbacks() {
    TableFunctionDistributedScanCallbacks callbacks;
    callbacks.protocol_version = 1;
    callbacks.split_codec = {VORTEX_SPLIT_CODEC, 1};
    callbacks.bind_data_mode = TableFunctionDistributedBindDataMode::REQUIRED;
    callbacks.plan_splits = VortexPlanDistributedScanSplits;
    callbacks.create_worker_bind = VortexCreateDistributedWorkerBind;
    callbacks.apply_splits = VortexApplyDistributedSplits;
    return callbacks;
}

} // namespace
#endif

static TableFunction CreateVortexTableFunction(LogicalType parameter, const std::string &name) {
    TableFunction tf(name, {}, function, duckdb_vx_table_function_bind, init_global, init_local);

    tf.projection_pushdown = true;
    tf.filter_pushdown = true;
    tf.filter_prune = true;
    tf.sampling_pushdown = false;
#ifdef VORTEX_VANE_DISTRIBUTED
    tf.supports_pushdown_type = [](const FunctionData &, idx_t column_id) {
        // file_index is constant per partition and is evaluated exactly before
        // readers are created. Other virtual-column filters stay in a DuckDB
        // PhysicalFilter so unsupported predicates cannot be silently ignored.
        return !IsVirtualColumn(column_id) || column_id == COLUMN_IDENTIFIER_FILE_INDEX;
    };
#endif

    tf.pushdown_expression = [](auto &, const auto &, Expression &expression) {
        return duckdb_table_function_pushdown_expression(reinterpret_cast<duckdb_vx_expr>(&expression));
    };
    tf.pushdown_complex_filter = [](auto &, auto &, FunctionData *bind_data, FilterVec &filters) {
        pushdown_complex_filter(*bind_data, filters);
    };
    tf.cardinality = cardinality;
    tf.get_partition_info = get_partition_info;
    tf.get_partition_data = get_partition_data;
    tf.to_string = to_string;
    tf.table_scan_progress = table_scan_progress;
    tf.statistics = statistics;

#ifdef VORTEX_VANE_DISTRIBUTED
    tf.serialize = VortexScanSerialize;
    tf.deserialize = VortexScanDeserialize;
    tf.SetDistributedScanCallbacks(VortexDistributedScanCallbacks());

    // DuckDB's late-materialization rewrite duplicates the table scan and
    // joins the copies by their virtual row identifiers. Vane assigns explicit
    // splits to every physical scan independently, so the two sides are not a
    // co-partitioned unit and can otherwise observe disjoint file assignments.
    // Keep the distributed-capable function as one explicit scan until Vane
    // has a grouped multi-scan split contract.
    tf.late_materialization = false;
#else
    tf.late_materialization = true;
#endif
    // Columns that uniquely identify a row for deferred re-fetch in a multi
    // file scan: (file index, row number in file).
    tf.get_row_id_columns = [](auto &, auto) -> vector<column_t> {
        return {COLUMN_IDENTIFIER_FILE_INDEX, COLUMN_IDENTIFIER_FILE_ROW_NUMBER};
    };

    tf.get_virtual_columns = [](auto &, auto) -> virtual_column_map_t {
        return {
            {COLUMN_IDENTIFIER_EMPTY, {"", LogicalTypeId::BOOLEAN}},
            {COLUMN_IDENTIFIER_FILE_INDEX, {"file_index", LogicalType::UBIGINT}},
            // MultiFileReader's file_row_number column is BIGINT.
            // row_idx() is UBIGINT. Use UBIGINT since there's no difference to
            // Duckdb what to compare.
            {COLUMN_IDENTIFIER_FILE_ROW_NUMBER, {"file_row_number", LogicalType::UBIGINT}},
        };
    };

    tf.arguments.resize(1);
    tf.arguments[0] = parameter;

    return tf;
}

duckdb_state register_table_function(DatabaseInstance &db, LogicalType parameter, const std::string &name) {
    auto tf = CreateVortexTableFunction(std::move(parameter), name);

    try {
        auto &system_catalog = Catalog::GetSystemCatalog(db);
        auto data = CatalogTransaction::GetSystemTransaction(db);
        CreateTableFunctionInfo tf_info(tf);
        tf_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        system_catalog.CreateFunction(data, tf_info);
    } catch (const std::exception &e) {
        ErrorData data(e);
        DUCKDB_LOG_ERROR(db, "Failed to create Vortex table function:\t" + data.Message());
        return DuckDBError;
    }
    return DuckDBSuccess;
}

#ifdef VORTEX_VANE_DISTRIBUTED
void RegisterVortexTableFunctions(ExtensionLoader &loader) {
    for (const std::string &name : {"read_vortex"s, "vortex_scan"s}) {
        TableFunctionSet functions(name);
        functions.AddFunction(CreateVortexTableFunction(LogicalType::VARCHAR, name));
        functions.AddFunction(CreateVortexTableFunction(LogicalType::LIST(LogicalType::VARCHAR), name));
        loader.RegisterFunction(std::move(functions));
    }
}
#endif

extern "C" duckdb_state duckdb_vx_register_table_functions(duckdb_database ffi_db) {
    D_ASSERT(ffi_db);
    const DatabaseWrapper &wrapper = *reinterpret_cast<DatabaseWrapper *>(ffi_db);
    DatabaseInstance &db = *wrapper.database->instance;

    for (LogicalType type : {LogicalType(LogicalType::VARCHAR), LogicalType::LIST(LogicalType::VARCHAR)}) {
        for (const std::string &name : {"read_vortex"s, "vortex_scan"s}) {
            if (register_table_function(db, type, name) == DuckDBError) {
                return DuckDBError;
            }
        }
    }
    return DuckDBSuccess;
}

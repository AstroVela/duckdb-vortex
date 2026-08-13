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

#ifdef VORTEX_DISTRIBUTED_SCAN
#include <algorithm>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/distributed_table_function.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#endif

using namespace std::string_literals;
constexpr column_t COLUMN_IDENTIFIER_FILE_INDEX = MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX;
constexpr column_t COLUMN_IDENTIFIER_FILE_ROW_NUMBER = MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;

unique_ptr<FunctionData> VortexBindData::Copy() const {
#ifdef VORTEX_DISTRIBUTED_SCAN
    unique_ptr<CData> ffi_data_copy;
    if (ffi_data) {
        const auto copied_ffi_data = duckdb_table_function_bind_data_clone(ffi_data->DataPtr());
        ffi_data_copy = unique_ptr<CData>(reinterpret_cast<CData *>(copied_ffi_data));
    }
    auto result = make_uniq<VortexBindData>(std::move(ffi_data_copy), types, names);
    result->portable_bind = portable_bind;
    result->distributed_files = distributed_files;
    result->aggregate_scan = aggregate_scan;
    result->explicit_task_mode = explicit_task_mode;
    result->tasks_applied = tasks_applied;
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
#ifdef VORTEX_DISTRIBUTED_SCAN
    if (ffi_data || other.ffi_data) {
        // Runtime bind equality retains the upstream pointer-identity semantics.
        return ffi_data.get() == other.ffi_data.get();
    }
    return types == other.types && names == other.names && portable_bind == other.portable_bind &&
           distributed_files == other.distributed_files && aggregate_scan == other.aggregate_scan &&
           explicit_task_mode == other.explicit_task_mode && tasks_applied == other.tasks_applied &&
           eligible_file_indexes == other.eligible_file_indexes &&
           assigned_file_indexes == other.assigned_file_indexes;
#else
    // if "types" are different, "ffi_data" would also be different as it
    // contains types inside, so omit "types" from comparison.
    return ffi_data.get() == other.ffi_data.get();
#endif
}

#ifdef VORTEX_DISTRIBUTED_SCAN
void VortexBindData::RefreshPortableBind() const {
    if (!ffi_data) {
        if (portable_bind.empty()) {
            throw SerializationException("Deserialized Vortex bind has no portable state");
        }
        return;
    }

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
    portable_bind.assign(reinterpret_cast<const char *>(portable_bytes), portable_size);

    auto file_count = duckdb_table_function_distributed_file_count(portable->DataPtr());
    aggregate_scan = duckdb_table_function_distributed_is_aggregate(portable->DataPtr());
    vector<DistributedFile> files;
    files.reserve(file_count);
    for (idx_t file_index = 0; file_index < file_count; file_index++) {
        VortexDistributedFileView view {};
        if (!duckdb_table_function_distributed_file_at(portable->DataPtr(), file_index, &view) ||
            !view.source_url || view.source_url_len == 0 || !view.path || view.path_len == 0) {
            throw SerializationException("Vortex produced an invalid distributed file at index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
        DistributedFile file;
        file.source_url.assign(reinterpret_cast<const char *>(view.source_url), view.source_url_len);
        file.path.assign(reinterpret_cast<const char *>(view.path), view.path_len);
        if (view.has_size) {
            if (view.size == DConstants::INVALID_INDEX) {
                throw SerializationException(
                    "Vortex file size at index %llu collides with DuckDB's invalid-size sentinel",
                    static_cast<unsigned long long>(file_index));
            }
            file.size = optional_idx(view.size);
        }
        files.push_back(std::move(file));
    }
    distributed_files = std::move(files);
}
#endif

// This is a flaw of Duckdb API which doesn't allow passing non-const
// expressions. We never modify the value on Rust side.
static duckdb_vx_expr get_ffi_expr(const Expression &expr) {
    return reinterpret_cast<duckdb_vx_expr>(const_cast<Expression *>(&expr));
}

static void *get_ffi_bind(const FunctionData *bind_data) {
#ifdef VORTEX_DISTRIBUTED_SCAN
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
#ifdef VORTEX_DISTRIBUTED_SCAN
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
#ifdef VORTEX_DISTRIBUTED_SCAN
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
#ifdef VORTEX_DISTRIBUTED_SCAN
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
#ifdef VORTEX_DISTRIBUTED_SCAN
    // A logical-plan round trip reconstructs the bind from portable owned data.
    // There is deliberately no live Rust reader until a worker receives tasks
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
#ifdef VORTEX_DISTRIBUTED_SCAN
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
    VortexBindResults result = {return_types, names};

    duckdb_vx_error error_out = nullptr;
    duckdb_vx_tfunc_bind_input bind_input = reinterpret_cast<duckdb_vx_tfunc_bind_input>(&input);
    duckdb_vx_tfunc_bind_result bind_result = reinterpret_cast<duckdb_vx_tfunc_bind_result>(&result);
    duckdb_vx_data ffi_bind_data = duckdb_table_function_bind(bind_input, bind_result, &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }

    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_bind_data));
#ifdef VORTEX_DISTRIBUTED_SCAN
    return make_uniq<VortexBindData>(std::move(cdata), return_types, names);
#else
    return make_uniq<VortexBindData>(std::move(cdata), return_types);
#endif
}

unique_ptr<GlobalTableFunctionState> init_global(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<VortexBindData>();

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
#ifdef VORTEX_DISTRIBUTED_SCAN
    bool distributed = false;
    if (!bind_data.ffi_data) {
        if (bind_data.explicit_task_mode && !bind_data.tasks_applied) {
            throw InvalidInputException(
                "Detached distributed Vortex scan requires an explicit task assignment before execution");
        }
        bind_data.RefreshPortableBind();
        vector<idx_t> native_file_indexes;
        const vector<idx_t> *runtime_file_indexes = &bind_data.assigned_file_indexes;
        if (!bind_data.explicit_task_mode) {
            native_file_indexes.reserve(bind_data.distributed_files.size());
            for (idx_t file_index = 0; file_index < bind_data.distributed_files.size(); file_index++) {
                native_file_indexes.push_back(file_index);
            }
            runtime_file_indexes = &native_file_indexes;
        }
        // Optional filters (for example TopN's dynamic bound) are maintained
        // by an upstream operator that is absent from Vane's detached scan
        // plan. They are pruning hints, so ignoring them is correctness-safe;
        // required table filters still pass through unchanged.
        ffi_global_data = duckdb_table_function_init_global_distributed(
            reinterpret_cast<const uint8_t *>(bind_data.portable_bind.data()),
            bind_data.portable_bind.size(),
            runtime_file_indexes->data(),
            runtime_file_indexes->size(),
            bind_data.explicit_task_mode,
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
#ifdef VORTEX_DISTRIBUTED_SCAN
    bool force_empty_output = false;
    force_empty_output = distributed && bind_data.explicit_task_mode && bind_data.tasks_applied &&
                         bind_data.assigned_file_indexes.empty();
    return make_uniq<VortexGlobalData>(std::move(cdata), distributed, force_empty_output);
#else
    return make_uniq<VortexGlobalData>(std::move(cdata));
#endif
}

unique_ptr<LocalTableFunctionState>
init_local(ExecutionContext &, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
    const void *ffi_bind;
#ifdef VORTEX_DISTRIBUTED_SCAN
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
#ifdef VORTEX_DISTRIBUTED_SCAN
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
#ifdef VORTEX_DISTRIBUTED_SCAN
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
#ifdef VORTEX_DISTRIBUTED_SCAN
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
#ifdef VORTEX_DISTRIBUTED_SCAN
    auto &bind_data = input.bind_data->Cast<VortexBindData>();
    if (!bind_data.ffi_data) {
        result.insert("Function", "Vortex Scan");
        const auto file_count = bind_data.explicit_task_mode ? bind_data.assigned_file_indexes.size()
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

#ifdef VORTEX_DISTRIBUTED_SCAN
namespace {

static constexpr uint8_t VORTEX_TASK_PAYLOAD_VERSION = 1;
static constexpr const char *VORTEX_TASK_CODEC = "vane.vortex-file-task";

static void AppendTaskByte(string &result, uint8_t value) {
    result.push_back(static_cast<char>(value));
}

static void AppendTaskU64(string &result, uint64_t value) {
    for (idx_t byte_index = 0; byte_index < sizeof(value); byte_index++) {
        AppendTaskByte(result, static_cast<uint8_t>((value >> (byte_index * 8U)) & 0xffU));
    }
}

static void AppendTaskString(string &result, const string &value) {
    AppendTaskU64(result, value.size());
    result.append(value);
}

// Binary payload v1:
//   "VXTK" | u8 version | u64 file_count |
//   repeated(u64 stable_file_index | string source_url | string path |
//            u8 has_size | u64 size)
// Normal scans encode one file. Aggregate-pushed scans encode their complete
// pruned file set so a single worker computes the final aggregate exactly once.
static string EncodeVortexTask(const vector<idx_t> &file_indexes, const VortexBindData &bind_data) {
    if (file_indexes.empty()) {
        throw InternalException("Cannot encode an empty distributed Vortex task");
    }
    string result("VXTK", 4);
    AppendTaskByte(result, VORTEX_TASK_PAYLOAD_VERSION);
    AppendTaskU64(result, file_indexes.size());
    for (auto file_index : file_indexes) {
        if (file_index >= bind_data.distributed_files.size()) {
            throw InternalException("Cannot encode unknown distributed Vortex file index %llu",
                                    static_cast<unsigned long long>(file_index));
        }
        const auto &file = bind_data.distributed_files[file_index];
        AppendTaskU64(result, file_index);
        AppendTaskString(result, file.source_url);
        AppendTaskString(result, file.path);
        AppendTaskByte(result, file.size.IsValid() ? 1 : 0);
        AppendTaskU64(result, file.size.IsValid() ? file.size.GetIndex() : 0);
    }
    return result;
}

class VortexTaskDecoder {
public:
    explicit VortexTaskDecoder(const string &payload_p) : payload(payload_p) {
    }

    uint8_t ReadByte() {
        if (offset >= payload.size()) {
            throw InvalidInputException("Truncated distributed Vortex task payload");
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
            throw InvalidInputException("Invalid string length in distributed Vortex task payload");
        }
        auto result = payload.substr(offset, size);
        offset += size;
        return result;
    }

    void Finish() const {
        if (offset != payload.size()) {
            throw InvalidInputException("Distributed Vortex task payload contains trailing bytes");
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

struct DecodedVortexTask {
    vector<DecodedVortexFile> files;
};

static DecodedVortexTask DecodeVortexTask(const string &payload) {
    if (payload.size() < 5 || payload.compare(0, 4, "VXTK") != 0) {
        throw InvalidInputException("Invalid distributed Vortex task payload magic");
    }
    VortexTaskDecoder decoder(payload);
    for (idx_t magic_index = 0; magic_index < 4; magic_index++) {
        decoder.ReadByte();
    }
    auto version = decoder.ReadByte();
    if (version != VORTEX_TASK_PAYLOAD_VERSION) {
        throw InvalidInputException("Unsupported distributed Vortex task payload version %u", version);
    }
    auto file_count = decoder.ReadU64();
    if (file_count == 0 || file_count > payload.size()) {
        throw InvalidInputException("Invalid file count in distributed Vortex task payload");
    }
    DecodedVortexTask result;
    result.files.reserve(file_count);
    for (idx_t file_offset = 0; file_offset < file_count; file_offset++) {
        DecodedVortexFile decoded;
        decoded.file_index = decoder.ReadU64();
        decoded.file.source_url = decoder.ReadString();
        decoded.file.path = decoder.ReadString();
        auto has_size = decoder.ReadByte();
        if (has_size > 1) {
            throw InvalidInputException("Invalid size flag in distributed Vortex task payload");
        }
        auto size = decoder.ReadU64();
        if (has_size) {
            if (size == DConstants::INVALID_INDEX) {
                throw InvalidInputException("Distributed Vortex task contains an invalid file size");
            }
            decoded.file.size = optional_idx(size);
        } else if (size != 0) {
            throw InvalidInputException("Distributed Vortex task has a size without a size flag");
        }
        if (decoded.file.source_url.empty() || decoded.file.path.empty()) {
            throw InvalidInputException("Distributed Vortex task contains an empty file identity");
        }
        result.files.push_back(std::move(decoded));
    }
    decoder.Finish();
    return result;
}

static bool IsCanonicalVortexTaskId(const string &task_id) {
    if (task_id.empty()) {
        return false;
    }
    idx_t segment_start = 0;
    optional_idx previous;
    while (segment_start < task_id.size()) {
        auto segment_end = task_id.find(',', segment_start);
        if (segment_end == string::npos) {
            segment_end = task_id.size();
        }
        if (segment_end == segment_start ||
            (segment_end - segment_start > 1 && task_id[segment_start] == '0')) {
            return false;
        }
        idx_t value = 0;
        for (idx_t offset = segment_start; offset < segment_end; offset++) {
            auto character = task_id[offset];
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
        if (segment_end == task_id.size()) {
            return true;
        }
        segment_start = segment_end + 1;
    }
    return false;
}

static string CanonicalVortexTaskId(const vector<idx_t> &file_indexes) {
    string result;
    for (auto file_index : file_indexes) {
        if (!result.empty()) {
            result += ',';
        }
        result += std::to_string(file_index);
    }
    return result;
}

static idx_t SaturatingVortexTaskEstimate(idx_t left, idx_t right) {
    // idx_t(-1) is reserved by optional_idx as its invalid sentinel.
    const auto maximum = NumericLimits<idx_t>::Maximum() - 1;
    return right > maximum - left ? maximum : left + right;
}

static idx_t ProportionalVortexTaskEstimate(idx_t total, idx_t numerator, idx_t denominator) {
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

static void VortexScanSerialize(Serializer &serializer,
                                const optional_ptr<FunctionData> bind_data,
                                const TableFunction &) {
    auto &data = bind_data->Cast<VortexBindData>();
    data.RefreshPortableBind();
    vector<string> source_urls;
    vector<string> paths;
    vector<uint64_t> sizes;
    vector<uint8_t> has_sizes;
    source_urls.reserve(data.distributed_files.size());
    paths.reserve(data.distributed_files.size());
    sizes.reserve(data.distributed_files.size());
    has_sizes.reserve(data.distributed_files.size());
    for (const auto &file : data.distributed_files) {
        source_urls.push_back(file.source_url);
        paths.push_back(file.path);
        sizes.push_back(file.size.IsValid() ? file.size.GetIndex() : 0);
        has_sizes.push_back(file.size.IsValid() ? 1 : 0);
    }
    serializer.WriteProperty(100, "types", data.types);
    serializer.WriteProperty(101, "names", data.names);
    serializer.WriteProperty(102, "portable_bind", data.portable_bind);
    serializer.WriteProperty(103, "source_urls", source_urls);
    serializer.WriteProperty(104, "paths", paths);
    serializer.WriteProperty(105, "sizes", sizes);
    serializer.WriteProperty(106, "has_sizes", has_sizes);
    serializer.WriteProperty(107, "explicit_task_mode", data.explicit_task_mode);
    serializer.WriteProperty(108, "tasks_applied", data.tasks_applied);
    serializer.WriteProperty(109, "assigned_file_indexes", data.assigned_file_indexes);
    serializer.WriteProperty(110, "aggregate_scan", data.aggregate_scan);
    serializer.WriteProperty(111, "eligible_file_indexes", data.eligible_file_indexes);
}

static unique_ptr<FunctionData> VortexScanDeserialize(Deserializer &deserializer, TableFunction &) {
    auto types = deserializer.ReadProperty<vector<LogicalType>>(100, "types");
    auto names = deserializer.ReadProperty<vector<string>>(101, "names");
    auto portable_bind = deserializer.ReadProperty<string>(102, "portable_bind");
    auto source_urls = deserializer.ReadProperty<vector<string>>(103, "source_urls");
    auto paths = deserializer.ReadProperty<vector<string>>(104, "paths");
    auto sizes = deserializer.ReadProperty<vector<uint64_t>>(105, "sizes");
    auto has_sizes = deserializer.ReadProperty<vector<uint8_t>>(106, "has_sizes");
    auto explicit_task_mode = deserializer.ReadProperty<bool>(107, "explicit_task_mode");
    auto tasks_applied = deserializer.ReadProperty<bool>(108, "tasks_applied");
    auto assigned_file_indexes = deserializer.ReadProperty<vector<idx_t>>(109, "assigned_file_indexes");
    auto aggregate_scan = deserializer.ReadProperty<bool>(110, "aggregate_scan");
    auto eligible_file_indexes = deserializer.ReadProperty<vector<idx_t>>(111, "eligible_file_indexes");
    if (types.size() != names.size() || portable_bind.empty() || source_urls.size() != paths.size() ||
        source_urls.size() != sizes.size() || source_urls.size() != has_sizes.size()) {
        throw SerializationException("Invalid serialized Vortex bind state");
    }
    vector<VortexBindData::DistributedFile> files;
    files.reserve(paths.size());
    for (idx_t file_index = 0; file_index < paths.size(); file_index++) {
        if (source_urls[file_index].empty() || paths[file_index].empty() || has_sizes[file_index] > 1) {
            throw SerializationException("Invalid serialized Vortex file at index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
        VortexBindData::DistributedFile file;
        file.source_url = std::move(source_urls[file_index]);
        file.path = std::move(paths[file_index]);
        if (has_sizes[file_index]) {
            if (sizes[file_index] == DConstants::INVALID_INDEX) {
                throw SerializationException("Serialized Vortex file has an invalid size");
            }
            file.size = optional_idx(sizes[file_index]);
        } else if (sizes[file_index] != 0) {
            throw SerializationException("Serialized Vortex file has a size without a size flag");
        }
        files.push_back(std::move(file));
    }
    unordered_set<idx_t> eligible;
    for (auto file_index : eligible_file_indexes) {
        if (file_index >= files.size() || !eligible.insert(file_index).second) {
            throw SerializationException("Invalid eligible Vortex file index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
    }
    unordered_set<idx_t> assigned;
    for (auto file_index : assigned_file_indexes) {
        if (file_index >= files.size() || !assigned.insert(file_index).second ||
            !eligible.count(file_index)) {
            throw SerializationException("Invalid assigned Vortex file index %llu",
                                         static_cast<unsigned long long>(file_index));
        }
    }
    if (!explicit_task_mode &&
        (tasks_applied || !eligible_file_indexes.empty() || !assigned_file_indexes.empty())) {
        throw SerializationException("Native Vortex bind contains distributed task state");
    }
    if (!tasks_applied && !assigned_file_indexes.empty()) {
        throw SerializationException(
            "Detached Vortex bind contains assigned files without an applied descriptor");
    }
    if (aggregate_scan && tasks_applied && !assigned_file_indexes.empty() &&
        assigned_file_indexes != eligible_file_indexes) {
        throw SerializationException(
            "Distributed aggregate Vortex bind contains an incomplete file assignment");
    }

    auto result = make_uniq<VortexBindData>(nullptr, types, names);
    result->portable_bind = std::move(portable_bind);
    result->distributed_files = std::move(files);
    result->aggregate_scan = aggregate_scan;
    result->explicit_task_mode = explicit_task_mode;
    result->tasks_applied = tasks_applied;
    result->eligible_file_indexes = std::move(eligible_file_indexes);
    result->assigned_file_indexes = std::move(assigned_file_indexes);
    return result;
}

static vector<idx_t> SelectDistributedVortexFiles(const TableFunctionDistributedScanInput &input) {
    auto &bind_data = input.bind_data.Cast<VortexBindData>();
    bind_data.RefreshPortableBind();
    vector<idx_t> column_ids;
    column_ids.reserve(input.column_ids.size());
    for (const auto &column_id : input.column_ids) {
        column_ids.push_back(column_id.GetPrimaryIndex());
    }
    auto filters =
        reinterpret_cast<duckdb_vx_table_filter_set>(const_cast<TableFilterSet *>(input.table_filters.get()));
    vector<idx_t> selected_file_indexes;
    selected_file_indexes.reserve(bind_data.distributed_files.size());
    for (idx_t file_index = 0; file_index < bind_data.distributed_files.size(); file_index++) {
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

static vector<DistributedScanTask> VortexPlanDistributedScan(const TableFunctionDistributedScanInput &input) {
    auto &bind_data = input.bind_data.Cast<VortexBindData>();
    if (bind_data.explicit_task_mode) {
        throw InvalidInputException("Distributed Vortex tasks cannot be planned from a worker bind");
    }
    // Vane may serialize a logical coordinator plan before generating its
    // physical scan. Such a bind is intentionally detached from the original
    // connection, but its owned portable state and immutable file identities
    // are sufficient for deterministic task planning.
    auto selected_file_indexes = SelectDistributedVortexFiles(input);
    vector<DistributedScanTask> result;
    if (selected_file_indexes.empty()) {
        return result;
    }
    result.reserve(bind_data.aggregate_scan ? 1 : selected_file_indexes.size());
    idx_t total_bytes = 0;
    bool all_file_sizes_known = true;
    for (auto file_index : selected_file_indexes) {
        const auto &file = bind_data.distributed_files[file_index];
        if (file.size.IsValid()) {
            total_bytes = SaturatingVortexTaskEstimate(total_bytes, file.size.GetIndex());
        } else {
            all_file_sizes_known = false;
        }
    }
    const auto has_estimated_rows = input.estimated_cardinality != DConstants::INVALID_INDEX;
    const auto estimated_rows = has_estimated_rows ? input.estimated_cardinality : 0;
    if (bind_data.aggregate_scan) {
        DistributedScanTask task;
        task.task_id = CanonicalVortexTaskId(selected_file_indexes);
        task.payload = EncodeVortexTask(selected_file_indexes, bind_data);
        if (all_file_sizes_known) {
            task.estimated_bytes = optional_idx(total_bytes);
        }
        task.estimated_cardinality = optional_idx(1);
        result.push_back(std::move(task));
        return result;
    }
    idx_t previous_cumulative_rows = 0;
    uint64_t cumulative_bytes = 0;
    for (idx_t selected_index = 0; selected_index < selected_file_indexes.size(); selected_index++) {
        const auto file_index = selected_file_indexes[selected_index];
        const auto &file = bind_data.distributed_files[file_index];
        vector<idx_t> task_files {file_index};
        DistributedScanTask task;
        task.task_id = CanonicalVortexTaskId(task_files);
        task.payload = EncodeVortexTask(task_files, bind_data);
        if (file.size.IsValid()) {
            task.estimated_bytes = file.size;
        }
        if (has_estimated_rows) {
            idx_t cumulative_rows;
            if (all_file_sizes_known && total_bytes > 0) {
                cumulative_bytes = SaturatingVortexTaskEstimate(cumulative_bytes, file.size.GetIndex());
                cumulative_rows =
                    ProportionalVortexTaskEstimate(estimated_rows, cumulative_bytes, total_bytes);
            } else {
                cumulative_rows = ProportionalVortexTaskEstimate(estimated_rows,
                                                                 selected_index + 1,
                                                                 selected_file_indexes.size());
            }
            task.estimated_cardinality = optional_idx(cumulative_rows - previous_cumulative_rows);
            previous_cumulative_rows = cumulative_rows;
        }
        result.push_back(std::move(task));
    }
    return result;
}

static void VortexPrepareDistributedBind(const TableFunctionDistributedScanInput &input,
                                         FunctionData &worker_bind_data) {
    auto &bind_data = worker_bind_data.Cast<VortexBindData>();
    if (bind_data.ffi_data) {
        throw InternalException("Distributed Vortex worker bind unexpectedly retained runtime reader state");
    }
    if (bind_data.explicit_task_mode || bind_data.tasks_applied || !bind_data.eligible_file_indexes.empty() ||
        !bind_data.assigned_file_indexes.empty()) {
        throw SerializationException("Distributed Vortex worker bind contains stale explicit task state");
    }
    auto &coordinator_bind = input.bind_data.Cast<VortexBindData>();
    if (coordinator_bind.explicit_task_mode) {
        throw InvalidInputException(
            "Distributed Vortex worker bind cannot be prepared from another worker bind");
    }
    auto eligible_file_indexes = SelectDistributedVortexFiles(input);
    if (bind_data.types != coordinator_bind.types || bind_data.names != coordinator_bind.names ||
        bind_data.portable_bind != coordinator_bind.portable_bind ||
        bind_data.aggregate_scan != coordinator_bind.aggregate_scan) {
        throw SerializationException("Distributed Vortex worker bind changed during physical-plan transport");
    }
    if (bind_data.distributed_files.size() != coordinator_bind.distributed_files.size()) {
        throw SerializationException("Distributed Vortex worker bind changed the bound file count");
    }
    for (idx_t file_index = 0; file_index < bind_data.distributed_files.size(); file_index++) {
        if (!SameDistributedFile(bind_data.distributed_files[file_index],
                                 coordinator_bind.distributed_files[file_index])) {
            throw SerializationException("Distributed Vortex worker bind changed file identity %llu",
                                         static_cast<unsigned long long>(file_index));
        }
    }
    bind_data.explicit_task_mode = true;
    bind_data.tasks_applied = false;
    bind_data.eligible_file_indexes = std::move(eligible_file_indexes);
    bind_data.assigned_file_indexes.clear();
}

static void VortexApplyDistributedTasks(FunctionData &worker_bind_data,
                                        const vector<DistributedScanTask> &tasks) {
    auto &bind_data = worker_bind_data.Cast<VortexBindData>();
    if (bind_data.ffi_data || !bind_data.explicit_task_mode) {
        throw InvalidInputException("Vortex distributed tasks require a detached worker bind");
    }
    if (bind_data.aggregate_scan && tasks.size() > 1) {
        throw InvalidInputException("Distributed aggregate Vortex scans require one complete file-set task");
    }
    unordered_set<string> task_ids;
    unordered_set<idx_t> file_indexes;
    unordered_set<idx_t> eligible_file_indexes(bind_data.eligible_file_indexes.begin(),
                                               bind_data.eligible_file_indexes.end());
    vector<idx_t> assigned;
    assigned.reserve(bind_data.distributed_files.size());
    for (const auto &task : tasks) {
        if (!IsCanonicalVortexTaskId(task.task_id)) {
            throw InvalidInputException("Invalid distributed Vortex task id '%s'", task.task_id);
        }
        if (task.payload.empty()) {
            throw InvalidInputException("Distributed Vortex task '%s' has an empty payload", task.task_id);
        }
        if (!task_ids.insert(task.task_id).second) {
            throw InvalidInputException("Duplicate distributed Vortex task id '%s'", task.task_id);
        }
        auto decoded = DecodeVortexTask(task.payload);
        if (!bind_data.aggregate_scan && decoded.files.size() != 1) {
            throw InvalidInputException(
                "Non-aggregate distributed Vortex tasks must reference exactly one file");
        }
        vector<idx_t> decoded_file_indexes;
        decoded_file_indexes.reserve(decoded.files.size());
        for (const auto &decoded_file : decoded.files) {
            if (decoded_file.file_index >= bind_data.distributed_files.size()) {
                throw InvalidInputException("Distributed Vortex task '%s' references an unknown file index",
                                            task.task_id);
            }
            if (!eligible_file_indexes.count(decoded_file.file_index)) {
                throw InvalidInputException(
                    "Distributed Vortex task '%s' references file index %llu outside the planned file set",
                    task.task_id,
                    static_cast<unsigned long long>(decoded_file.file_index));
            }
            if (!SameDistributedFile(decoded_file.file,
                                     bind_data.distributed_files[decoded_file.file_index])) {
                throw InvalidInputException(
                    "Distributed Vortex task '%s' does not match the bound file identity",
                    task.task_id);
            }
            if (!file_indexes.insert(decoded_file.file_index).second) {
                throw InvalidInputException(
                    "Distributed Vortex tasks reference file index %llu more than once",
                    static_cast<unsigned long long>(decoded_file.file_index));
            }
            decoded_file_indexes.push_back(decoded_file.file_index);
            assigned.push_back(decoded_file.file_index);
        }
        if (task.task_id != CanonicalVortexTaskId(decoded_file_indexes)) {
            throw InvalidInputException(
                "Distributed Vortex task id '%s' does not match its payload file indexes",
                task.task_id);
        }
    }
    // A descriptor is a set of elementary scan tasks. Canonicalize its file
    // assignment so transport or retry code may reorder those tasks without
    // changing scan meaning or defeating idempotent re-application.
    std::sort(assigned.begin(), assigned.end());
    if (bind_data.aggregate_scan && !tasks.empty() && assigned != bind_data.eligible_file_indexes) {
        throw InvalidInputException(
            "Distributed aggregate Vortex task does not contain the complete planned file set");
    }
    if (bind_data.tasks_applied) {
        if (assigned != bind_data.assigned_file_indexes) {
            throw InvalidInputException(
                "Distributed Vortex bind already has a different explicit task assignment");
        }
        return;
    }
    bind_data.assigned_file_indexes = std::move(assigned);
    bind_data.tasks_applied = true;
}

static TableFunctionDistributedScanCallbacks VortexDistributedScanCallbacks() {
    TableFunctionDistributedScanCallbacks callbacks;
    callbacks.protocol_version = 1;
    callbacks.task_codec = {VORTEX_TASK_CODEC, 1};
    callbacks.plan = VortexPlanDistributedScan;
    callbacks.prepare_bind = VortexPrepareDistributedBind;
    callbacks.apply_tasks = VortexApplyDistributedTasks;
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
#ifdef VORTEX_DISTRIBUTED_SCAN
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

#ifdef VORTEX_DISTRIBUTED_SCAN
    tf.serialize = VortexScanSerialize;
    tf.deserialize = VortexScanDeserialize;
    tf.SetDistributedScanCallbacks(VortexDistributedScanCallbacks());

    // DuckDB's late-materialization rewrite duplicates the table scan and
    // joins the copies by their virtual row identifiers. Vane assigns explicit
    // tasks to every physical scan independently, so the two sides are not a
    // co-partitioned unit and can otherwise observe disjoint file assignments.
    // Keep the distributed-capable function as one explicit scan until Vane
    // has a grouped multi-scan task contract.
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

#ifdef VORTEX_DISTRIBUTED_SCAN
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

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
#include "data.hpp"
#include "error.hpp"
#include "vortex_duckdb.h"
#include "table_function.h"
#include "vortex.h"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

#ifdef VORTEX_DISTRIBUTED_SCAN
#include "duckdb/main/extension/extension_loader.hpp"
#endif

using namespace duckdb;

struct CopyBindData final : TableFunctionData {
    CopyBindData(unique_ptr<CData> ffi_data, vector<string> column_names, vector<LogicalType> column_types)
        : ffi_data(std::move(ffi_data)), column_names(std::move(column_names)),
          column_types(std::move(column_types)) {
    }

    unique_ptr<FunctionData> Copy() const override;

    unique_ptr<CData> ffi_data;
    vector<string> column_names;
    vector<LogicalType> column_types;
};

struct CopyGlobalData final : GlobalFunctionData {
    CopyGlobalData(unique_ptr<CData> ffi_data, string file_path)
        : ffi_data(std::move(ffi_data)), file_path(std::move(file_path)) {
    }

    unique_ptr<CData> ffi_data;
    string file_path;
    idx_t row_count = 0;
    optional_ptr<CopyFunctionFileStatistics> written_statistics;
};

static unique_ptr<CopyBindData> CreateCopyBindData(const vector<string> &column_names,
                                                   const vector<LogicalType> &column_types) {
    if (column_names.size() != column_types.size()) {
        throw InvalidInputException("Vortex COPY bind has %llu column names but %llu column types",
                                    static_cast<unsigned long long>(column_names.size()),
                                    static_cast<unsigned long long>(column_types.size()));
    }

    vector<const char *> ffi_column_names(column_names.size());
    for (size_t i = 0; i < column_names.size(); ++i) {
        ffi_column_names[i] = column_names[i].c_str();
    }

    vector<duckdb_logical_type> ffi_column_types(column_types.size());
    for (size_t i = 0; i < column_types.size(); ++i) {
        // duckdb C api doesn't allow passing const LogicalTypes. We never
        // modify input in copy function.
        ffi_column_types[i] =
            reinterpret_cast<duckdb_logical_type>(const_cast<LogicalType *>(&column_types[i]));
    }

    duckdb_vx_error error_out = nullptr;
    const duckdb_vx_data ffi_bind_data = duckdb_copy_function_copy_to_bind(ffi_column_names.data(),
                                                                           ffi_column_names.size(),
                                                                           ffi_column_types.data(),
                                                                           ffi_column_types.size(),
                                                                           &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }
    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_bind_data));
    return make_uniq<CopyBindData>(std::move(cdata), column_names, column_types);
}

unique_ptr<FunctionData> CopyBindData::Copy() const {
    return CreateCopyBindData(column_names, column_types);
}

unique_ptr<FunctionData> copy_to_bind(ClientContext &,
                                      CopyFunctionBindInput &,
                                      const vector<string> &column_names,
                                      const vector<LogicalType> &column_types) {
    return CreateCopyBindData(column_names, column_types);
}

unique_ptr<GlobalFunctionData>
copy_to_initialize_global(ClientContext &, FunctionData &bind_data, const string &file_path) {
    void *const ffi_bind = bind_data.Cast<CopyBindData>().ffi_data->DataPtr();

    duckdb_vx_error error_out = nullptr;
    const duckdb_vx_data ffi_global =
        duckdb_copy_function_copy_to_initialize_global(ffi_bind, file_path.c_str(), &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }

    auto cdata = unique_ptr<CData>(reinterpret_cast<CData *>(ffi_global));
    return make_uniq<CopyGlobalData>(std::move(cdata), file_path);
}

void copy_to_sink(ExecutionContext &,
                  FunctionData &bind_data,
                  GlobalFunctionData &gstate,
                  LocalFunctionData &,
                  DataChunk &input) {
    void *const ffi_bind = bind_data.Cast<CopyBindData>().ffi_data->DataPtr();
    void *const ffi_global = gstate.Cast<CopyGlobalData>().ffi_data->DataPtr();
    auto ffi_chunk = reinterpret_cast<duckdb_data_chunk>(&input);
    duckdb_vx_error error_out = nullptr;
    duckdb_copy_function_copy_to_sink(ffi_bind, ffi_global, ffi_chunk, &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }
    auto &global_data = gstate.Cast<CopyGlobalData>();
    global_data.row_count += input.size();
    if (global_data.written_statistics) {
        global_data.written_statistics->row_count = global_data.row_count;
    }
}

void copy_to_finalize(ClientContext &context, FunctionData &, GlobalFunctionData &gstate) {
    auto &global_data = gstate.Cast<CopyGlobalData>();
    void *const ffi_global = global_data.ffi_data->DataPtr();
    duckdb_vx_error error_out = nullptr;
    duckdb_copy_function_copy_to_finalize(ffi_global, &error_out);
    if (error_out) {
        throw ExecutorException(IntoErrString(error_out));
    }
    if (global_data.written_statistics) {
        auto &statistics = *global_data.written_statistics;
        statistics.row_count = global_data.row_count;
        auto &file_system = FileSystem::GetFileSystem(context);
        auto handle = file_system.OpenFile(global_data.file_path, FileFlags::FILE_FLAGS_READ);
        statistics.file_size_bytes = handle->GetFileSize();
    }
}

void copy_to_get_written_statistics(ClientContext &,
                                    FunctionData &,
                                    GlobalFunctionData &gstate,
                                    CopyFunctionFileStatistics &statistics) {
    auto &global_data = gstate.Cast<CopyGlobalData>();
    global_data.written_statistics = statistics;
    statistics.row_count = global_data.row_count;
}

void copy_to_serialize(Serializer &serializer, const FunctionData &bind_data, const CopyFunction &) {
    const auto &copy_bind = bind_data.Cast<CopyBindData>();
    serializer.WriteProperty(100, "column_names", copy_bind.column_names);
    serializer.WriteProperty(101, "column_types", copy_bind.column_types);
}

unique_ptr<FunctionData> copy_to_deserialize(Deserializer &deserializer, CopyFunction &) {
    auto column_names = deserializer.ReadProperty<vector<string>>(100, "column_names");
    auto column_types = deserializer.ReadProperty<vector<LogicalType>>(101, "column_types");
    return CreateCopyBindData(column_names, column_types);
}

static CopyFunction CreateVortexCopyFunction() {
    CopyFunction fn("vortex");
    fn.copy_to_bind = copy_to_bind;
    fn.copy_to_initialize_global = copy_to_initialize_global;
    fn.copy_to_initialize_local = [](auto &, auto &) {
        return make_uniq<LocalFunctionData>();
    };
    fn.copy_to_get_written_statistics = copy_to_get_written_statistics;
    fn.copy_to_sink = copy_to_sink;
    fn.copy_to_finalize = copy_to_finalize;
    fn.serialize = copy_to_serialize;
    fn.deserialize = copy_to_deserialize;
    fn.extension = "vortex";

    // TODO(joe): expose this via c our api
    fn.execution_mode = [](bool, bool) {
        return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
    };
    // TODO(joe): handle parameters as in table_function

    return fn;
}

#ifdef VORTEX_DISTRIBUTED_SCAN
void RegisterVortexCopyFunction(ExtensionLoader &loader) {
    loader.RegisterFunction(CreateVortexCopyFunction());
}
#endif

extern "C" duckdb_state duckdb_vx_register_copy_function(duckdb_database ffi_db) {
    D_ASSERT(ffi_db);
    const DatabaseWrapper &wrapper = *reinterpret_cast<DatabaseWrapper *>(ffi_db);
    DatabaseInstance &db = *wrapper.database->instance;
    auto fn = CreateVortexCopyFunction();

    try {
        Catalog &system_catalog = Catalog::GetSystemCatalog(db);
        CatalogTransaction data = CatalogTransaction::GetSystemTransaction(db);
        CreateCopyFunctionInfo copy_info(std::move(fn));
        system_catalog.CreateCopyFunction(data, copy_info);
    } catch (const std::exception &e) {
        ErrorData data(e);
        DUCKDB_LOG_ERROR(db, "Failed to create Vortex copy function:\t" + data.Message());
        return DuckDBError;
    }
    return DuckDBSuccess;
}

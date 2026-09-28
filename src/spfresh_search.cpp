// SPDX-License-Identifier: MIT
#include "spfresh_search.hpp"
#include "sptag_spfresh_ffi.h"

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "yyjson.hpp"

#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>

namespace duckdb {
namespace {
using namespace duckdb_yyjson;
using JsonDocument = std::unique_ptr<yyjson_doc, decltype(&yyjson_doc_free)>;

JsonDocument ParseJson(const string &text) {
	auto doc = JsonDocument(yyjson_read(text.data(), text.size(), 0), yyjson_doc_free);
	if (!doc) {
		throw InvalidInputException("Invalid SPFresh JSON");
	}
	return doc;
}

string StringField(yyjson_val *object, const char *name) {
	auto value = yyjson_obj_get(object, name);
	if (!yyjson_is_str(value)) {
		throw InvalidInputException("SPFresh metadata requires string field '%s'", name);
	}
	string result(yyjson_get_str(value), yyjson_get_len(value));
	if (result.empty() || result.find('\0') != string::npos) {
		throw InvalidInputException("Invalid SPFresh field '%s'", name);
	}
	return result;
}

uint32_t PositiveInteger(yyjson_val *value, const char *name) {
	if (!yyjson_is_uint(value) || yyjson_get_uint(value) == 0 ||
	    yyjson_get_uint(value) > std::numeric_limits<uint32_t>::max()) {
		throw InvalidInputException("SPFresh %s must be a positive uint32", name);
	}
	return static_cast<uint32_t>(yyjson_get_uint(value));
}

uint32_t PositiveInteger(const Value &value, const char *name) {
	if (value.IsNull()) {
		throw InvalidInputException("SPFresh %s cannot be NULL", name);
	}
	auto number = value.GetValue<int64_t>();
	if (number <= 0 || uint64_t(number) > std::numeric_limits<uint32_t>::max()) {
		throw InvalidInputException("SPFresh %s must be a positive uint32", name);
	}
	return static_cast<uint32_t>(number);
}

string ReadMetadata(const std::filesystem::path &path) {
	std::ifstream file(path, std::ios::binary | std::ios::ate);
	auto length = file.tellg();
	if (!file || length <= 0 || length > 16 * 1024 * 1024) {
		throw IOException("Cannot read SPFresh metadata (maximum 16 MiB): %s", path.string());
	}
	string text(static_cast<size_t>(length), '\0');
	file.seekg(0);
	if (!file.read(&text[0], length)) {
		throw IOException("Truncated SPFresh metadata: %s", path.string());
	}
	return text;
}

void RequireExternalAccess(ClientContext &context) {
	Value enabled;
	if (!context.TryGetCurrentSetting("enable_external_access", enabled) || !enabled.GetValue<bool>()) {
		throw PermissionException("SPFresh native stores require enable_external_access=true");
	}
}

struct StoreInfo {
	string config_path;
	string storage;
	string identity;
	uint32_t dim;
};

StoreInfo ReadStore(ClientContext &context, const string &table_path, const string &index_name) {
	RequireExternalAccess(context);
	if (table_path.find('\0') != string::npos || table_path.find("://") != string::npos ||
	    !std::filesystem::path(table_path).is_absolute()) {
		throw InvalidInputException("SPFresh requires an absolute local table path");
	}
	if (index_name.empty() || index_name.find_first_not_of(
	                              "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != string::npos) {
		throw InvalidInputException("Invalid SPFresh index name");
	}
	auto base = std::filesystem::path(table_path);
	auto catalog_text = ReadMetadata(base / "indexes" / "_index_catalog.json");
	auto catalog = ParseJson(catalog_text);
	auto entries = yyjson_doc_get_root(catalog.get());
	if (!yyjson_is_arr(entries)) {
		throw InvalidInputException("SPFresh catalog must be a JSON array");
	}
	yyjson_val *entry = nullptr;
	for (size_t i = 0; i < yyjson_arr_size(entries); ++i) {
		auto candidate = yyjson_arr_get(entries, i);
		if (StringField(candidate, "name") == index_name) {
			if (entry) {
				throw InvalidInputException("Duplicate SPFresh catalog entry: %s", index_name);
			}
			entry = candidate;
		}
	}
	if (!entry || StringField(entry, "index_type") != "sptag_spfresh") {
		throw InvalidInputException("Registered SPFresh index not found: %s", index_name);
	}
	auto relative = std::filesystem::path("indexes") / index_name / "sptag_spfresh" / "manifest.json";
	auto external = yyjson_obj_get(entry, "external_manifest");
	if (external && !yyjson_is_null(external)) {
		relative = StringField(entry, "external_manifest");
	}
	if (relative.is_absolute()) {
		throw InvalidInputException("SPFresh manifest must be relative to the table directory");
	}
	for (const auto &part : relative) {
		if (part == "..") {
			throw InvalidInputException("SPFresh manifest cannot escape the table directory");
		}
	}
	auto manifest_text = ReadMetadata(base / relative);
	auto manifest = ParseJson(manifest_text);
	auto root = yyjson_doc_get_root(manifest.get());
	if (StringField(root, "format") != "vortex-sptag-spfresh-store" ||
	    PositiveInteger(yyjson_obj_get(root, "version"), "version") != 1 ||
	    StringField(root, "backend") != "sptag_spfresh" || StringField(root, "distance_type") != "l2") {
		throw InvalidInputException("Unsupported SPFresh manifest format, version, backend, or metric");
	}
	auto dim = PositiveInteger(yyjson_obj_get(root, "dim"), "dim");
	if (dim != PositiveInteger(yyjson_obj_get(entry, "dim"), "catalog dim") ||
	    StringField(entry, "distance_type") != "l2" ||
	    PositiveInteger(yyjson_obj_get(root, "num_vectors"), "num_vectors") !=
	        PositiveInteger(yyjson_obj_get(entry, "num_vectors"), "catalog num_vectors")) {
		throw InvalidInputException("SPFresh manifest does not match the catalog");
	}
	auto value_type = StringField(root, "value_type");
	if ((value_type != "Float" && value_type != "Int8" && value_type != "UInt8") ||
	    StringField(root, "row_id_encoding") != "sptag-metadata") {
		throw InvalidInputException("Unsupported SPFresh value type or row ID encoding");
	}
	auto storage = StringField(root, "storage");
	auto kind = StringField(root, "store_kind");
	if (!((storage == "STATIC" && kind == "static") || (storage == "FILEIO" && kind == "dynamic"))) {
		throw InvalidInputException("SPFresh supports static STATIC and dynamic FILEIO stores");
	}
	auto config_path = StringField(root, "config_path");
	if (!std::filesystem::path(config_path).is_absolute() || config_path.find("://") != string::npos) {
		throw InvalidInputException("SPFresh config_path must be an absolute local path");
	}
	// Include the configuration contents so a prepared query cannot reuse a stale handle.
	auto config = ReadMetadata(config_path);
	return {config_path, storage, catalog_text + manifest_text + config, dim};
}

struct SearchData : public TableFunctionData {
	string table_path;
	string index_name;
	vector<float> queries;
	uint32_t query_count = 0;
	uint32_t dim = 0;
	uint32_t k = 10;
	uint32_t max_check = 4096;
	uint32_t internal_result_num = 64;
	uint32_t posting_page_limit = 16;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SearchData>(*this);
	}
	bool Equals(const FunctionData &other) const override {
		auto &rhs = other.Cast<SearchData>();
		return table_path == rhs.table_path && index_name == rhs.index_name && queries == rhs.queries &&
		       query_count == rhs.query_count && dim == rhs.dim && k == rhs.k && max_check == rhs.max_check &&
		       internal_result_num == rhs.internal_result_num && posting_page_limit == rhs.posting_page_limit;
	}
};

unique_ptr<FunctionData> BindSearch(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &types,
                                    vector<string> &names) {
	for (const auto &value : input.inputs) {
		if (value.IsNull()) {
			throw InvalidInputException("SPFresh arguments cannot be NULL");
		}
	}
	auto data = make_uniq<SearchData>();
	data->table_path = input.inputs[0].GetValue<string>();
	data->index_name = input.inputs[1].GetValue<string>();
	auto store = ReadStore(context, data->table_path, data->index_name);
	data->dim = store.dim;
	const bool blob = input.inputs.size() == 6;
	data->k = PositiveInteger(input.inputs[blob ? 5 : 3], "k");
	for (auto &parameter : input.named_parameters) {
		auto name = StringUtil::Lower(parameter.first);
		if (name == "spfresh_backend") {
			if (parameter.second.IsNull() || parameter.second.GetValue<string>() != "ssdserving_lib") {
				throw InvalidInputException("This SPFresh adapter requires spfresh_backend='ssdserving_lib'");
			}
		} else {
			auto value = PositiveInteger(parameter.second, parameter.first.c_str());
			if (name == "spfresh_max_check") {
				data->max_check = value;
			}
			if (name == "spfresh_internal_result_num") {
				data->internal_result_num = value;
			}
			if (name == "spfresh_posting_page_limit") {
				data->posting_page_limit = value;
			}
		}
	}
	if (blob) {
		data->query_count = PositiveInteger(input.inputs[3], "query_count");
		if (PositiveInteger(input.inputs[4], "dim") != data->dim) {
			throw InvalidInputException("SPFresh query dimension does not match the manifest");
		}
		const auto &bytes = StringValue::Get(input.inputs[2]);
		uint64_t count = uint64_t(data->query_count) * data->dim;
		if (count > std::numeric_limits<size_t>::max() / sizeof(float) || bytes.size() != count * sizeof(float)) {
			throw InvalidInputException("SPFresh BLOB length must equal query_count * dim * sizeof(float)");
		}
		data->queries.resize(count);
		std::memcpy(data->queries.data(), bytes.data(), bytes.size());
	} else {
		auto text = input.inputs[2].GetValue<string>();
		auto json = ParseJson(text);
		auto rows = yyjson_doc_get_root(json.get());
		if (!yyjson_is_arr(rows) || yyjson_arr_size(rows) == 0 || yyjson_arr_size(rows) > UINT32_MAX) {
			throw InvalidInputException("SPFresh queries must be a nonempty JSON array of vectors");
		}
		data->query_count = static_cast<uint32_t>(yyjson_arr_size(rows));
		for (uint32_t i = 0; i < data->query_count; ++i) {
			auto row = yyjson_arr_get(rows, i);
			if (!yyjson_is_arr(row) || yyjson_arr_size(row) != data->dim) {
				throw InvalidInputException("SPFresh query dimension does not match the manifest");
			}
			for (uint32_t j = 0; j < data->dim; ++j) {
				auto value = yyjson_arr_get(row, j);
				if (!yyjson_is_num(value)) {
					throw InvalidInputException("SPFresh vectors must contain numbers");
				}
				data->queries.push_back(static_cast<float>(yyjson_get_num(value)));
			}
		}
	}
	for (auto value : data->queries) {
		if (!std::isfinite(value)) {
			throw InvalidInputException("SPFresh query values must be finite");
		}
	}
	if (uint64_t(data->query_count) * data->k > 16000000) {
		throw InvalidInputException("SPFresh batch may produce at most 16000000 results");
	}
	types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::FLOAT};
	names = {"query_id", "id", "distance"};
	return data;
}

void CheckNative(int status, char *error) {
	string message = error ? error : "SPFresh native operation failed";
	vx_spfresh_free_error(error);
	if (status != 0) {
		throw IOException("%s", message);
	}
}

struct NativeIndex {
	VxSpfreshIndex handle = nullptr;
	~NativeIndex() {
		if (handle)
			vx_spfresh_static_close(handle);
	}
};

struct SearchCache : public ClientContextState {
	std::mutex mutex;
	string identity;
	unique_ptr<NativeIndex> index;
};

struct SearchState : public GlobalTableFunctionState {
	vector<uint64_t> ids;
	vector<float> distances;
	vector<uint32_t> lengths;
	idx_t query = 0;
	idx_t rank = 0;
};

unique_ptr<GlobalTableFunctionState> InitSearch(ClientContext &context, TableFunctionInitInput &input) {
	auto &data = input.bind_data->Cast<SearchData>();
	auto store = ReadStore(context, data.table_path, data.index_name);
	if (store.dim != data.dim) {
		throw InvalidInputException("SPFresh index dimension changed; rebind this query");
	}
	auto state = make_uniq<SearchState>();
	auto count = uint64_t(data.query_count) * data.k;
	state->ids.resize(count);
	state->distances.resize(count);
	state->lengths.resize(data.query_count);
	auto cache = context.registered_state->GetOrCreate<SearchCache>("vortex_spfresh_search");
	std::lock_guard<std::mutex> lock(cache->mutex);
	auto key = store.identity + "\n" + std::to_string(data.max_check) + "/" + std::to_string(data.internal_result_num) +
	           "/" + std::to_string(data.posting_page_limit);
	if (!cache->index || cache->identity != key) {
		auto index = make_uniq<NativeIndex>();
		char *error = nullptr;
		int status;
		if (store.storage == "STATIC") {
			status = vx_spfresh_static_open_config(store.config_path.c_str(), data.max_check, data.internal_result_num,
			                                       data.posting_page_limit, &index->handle, &error);
		} else {
			status = vx_spfresh_dynamic_open_config(store.config_path.c_str(), store.storage.c_str(), data.max_check,
			                                        data.internal_result_num, data.posting_page_limit, &index->handle,
			                                        &error);
		}
		CheckNative(status, error);
		if (!index->handle)
			throw IOException("SPFresh returned a null index");
		cache->index = std::move(index);
		cache->identity = std::move(key);
	}
	char *error = nullptr;
	auto status =
	    vx_spfresh_static_search_batch(cache->index->handle, data.queries.data(), data.query_count, data.dim, data.k,
	                                   data.max_check, data.internal_result_num, data.posting_page_limit,
	                                   state->ids.data(), state->distances.data(), state->lengths.data(), &error);
	CheckNative(status, error);
	for (uint32_t query = 0; query < data.query_count; ++query) {
		if (state->lengths[query] > data.k) {
			throw IOException("SPFresh returned more than k results");
		}
		for (uint32_t rank = 0; rank < state->lengths[query]; ++rank) {
			auto offset = uint64_t(query) * data.k + rank;
			if (state->ids[offset] > uint64_t(std::numeric_limits<int64_t>::max()) ||
			    !std::isfinite(state->distances[offset])) {
				throw IOException("SPFresh returned an invalid ID or distance");
			}
		}
	}
	return state;
}

void ScanSearch(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<SearchData>();
	auto &state = input.global_state->Cast<SearchState>();
	idx_t rows = 0;
	while (rows < STANDARD_VECTOR_SIZE && state.query < data.query_count) {
		if (state.rank == state.lengths[state.query]) {
			++state.query;
			state.rank = 0;
			continue;
		}
		auto offset = state.query * data.k + state.rank++;
		FlatVector::GetData<int64_t>(output.data[0])[rows] = static_cast<int64_t>(state.query);
		FlatVector::GetData<int64_t>(output.data[1])[rows] = static_cast<int64_t>(state.ids[offset]);
		FlatVector::GetData<float>(output.data[2])[rows] = state.distances[offset];
		++rows;
	}
	output.SetCardinality(rows);
}
} // namespace

void RegisterSpfreshSearch(ExtensionLoader &loader) {
	for (bool blob : {false, true}) {
		vector<LogicalType> arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR};
		arguments.push_back(blob ? LogicalType::BLOB : LogicalType::VARCHAR);
		if (blob) {
			arguments.push_back(LogicalType::BIGINT);
			arguments.push_back(LogicalType::BIGINT);
		}
		arguments.push_back(LogicalType::BIGINT);
		TableFunction function(blob ? "vortex_spfresh_search_batch_blob" : "vortex_spfresh_search_batch", arguments,
		                       ScanSearch, BindSearch, InitSearch);
		function.named_parameters["spfresh_backend"] = LogicalType::VARCHAR;
		function.named_parameters["spfresh_max_check"] = LogicalType::BIGINT;
		function.named_parameters["spfresh_internal_result_num"] = LogicalType::BIGINT;
		function.named_parameters["spfresh_posting_page_limit"] = LogicalType::BIGINT;
		loader.RegisterFunction(function);
	}
}
} // namespace duckdb

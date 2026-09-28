// SPDX-License-Identifier: Apache-2.0

#include "sptag_spfresh_ffi.h"

#include "inc/Core/SearchQuery.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/VectorSet.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/main.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

struct SpfreshWrapper {
    std::shared_ptr<SPTAG::VectorIndex> index;
    SPTAG::VectorValueType value_type = SPTAG::VectorValueType::Undefined;
    uint32_t dim = 0;
};

static_assert(sizeof(SPTAG::SizeType) == 4, "SPTAG SizeType must match AE trace format");
static_assert(sizeof(SPTAG::DimensionType) == 4, "SPTAG DimensionType must match DEFAULT vector header");

struct UpdateTrace {
    std::vector<std::uint32_t> delete_ids;
    std::vector<std::uint32_t> insert_ids;
};

void set_error(char** error, const std::string& message) {
    if (error == nullptr) {
        return;
    }
    char* buffer = new (std::nothrow) char[message.size() + 1];
    if (buffer == nullptr) {
        *error = nullptr;
        return;
    }
    std::memcpy(buffer, message.c_str(), message.size() + 1);
    *error = buffer;
}

bool is_success(SPTAG::ErrorCode code) {
    return code == SPTAG::ErrorCode::Success;
}

double elapsed_ms(std::chrono::steady_clock::time_point start) {
    const auto elapsed = std::chrono::steady_clock::now() - start;
    return std::chrono::duration<double, std::milli>(elapsed).count();
}

void reset_profile(VxSpfreshApplyTraceProfile* profile) {
    if (profile != nullptr) {
        *profile = {};
    }
}

void reset_profile(VxSpfreshSaveProfile* profile) {
    if (profile != nullptr) {
        *profile = {};
    }
}

std::string error_code_message(const std::string& prefix, SPTAG::ErrorCode code) {
    return prefix + " failed with code " + std::to_string(static_cast<int>(code));
}

bool key_equals(const std::string& left, const char* right) {
    return SPTAG::Helper::StrUtils::StrEqualIgnoreCase(left.c_str(), right);
}

void set_config_parameter(
    std::map<std::string, std::map<std::string, std::string>>& config,
    const std::string& section,
    const char* key,
    const std::string& value
) {
    auto& values = config[section];
    for (auto it = values.begin(); it != values.end();) {
        if (key_equals(it->first, key)) {
            it = values.erase(it);
        } else {
            ++it;
        }
    }
    values[key] = value;
}

std::string get_config_parameter(
    const std::map<std::string, std::map<std::string, std::string>>& config,
    const std::string& section,
    const char* key
) {
    const auto section_iter = config.find(section);
    if (section_iter == config.end()) {
        return "";
    }
    for (const auto& entry : section_iter->second) {
        if (key_equals(entry.first, key)) {
            return entry.second;
        }
    }
    return "";
}

std::map<std::string, std::map<std::string, std::string>> load_search_config(
    const char* config_path,
    SPTAG::VectorValueType& value_type,
    const char* storage
) {
    if (storage == nullptr || storage[0] == '\0') {
        throw std::runtime_error("SPTAG SPFresh storage must be non-empty");
    }

    SPTAG::Helper::IniReader ini_reader;
    const auto ret = ini_reader.LoadIniFile(config_path);
    if (!is_success(ret)) {
        throw std::runtime_error(
            "SPTAG failed to read config '" + std::string(config_path) +
            "' with code " + std::to_string(static_cast<int>(ret))
        );
    }

    std::map<std::string, std::map<std::string, std::string>> config;
    config[SPTAG::SSDServing::SEC_BASE] = ini_reader.GetParameters(SPTAG::SSDServing::SEC_BASE);
    config[SPTAG::SSDServing::SEC_SELECT_HEAD] =
        ini_reader.GetParameters(SPTAG::SSDServing::SEC_SELECT_HEAD);
    config[SPTAG::SSDServing::SEC_BUILD_HEAD] =
        ini_reader.GetParameters(SPTAG::SSDServing::SEC_BUILD_HEAD);
    config[SPTAG::SSDServing::SEC_BUILD_SSD_INDEX] =
        ini_reader.GetParameters(SPTAG::SSDServing::SEC_BUILD_SSD_INDEX);

    value_type = ini_reader.GetParameter(
        SPTAG::SSDServing::SEC_BASE,
        "ValueType",
        SPTAG::VectorValueType::Undefined
    );
    if (value_type == SPTAG::VectorValueType::Undefined) {
        throw std::runtime_error("SPTAG SPFresh config has no valid Base.ValueType");
    }

    const bool build_ssd = ini_reader.GetParameter(
        SPTAG::SSDServing::SEC_BUILD_SSD_INDEX,
        "isExecute",
        false
    );
    for (auto entry : ini_reader.GetParameters(SPTAG::SSDServing::SEC_SEARCH_SSD_INDEX)) {
        std::string param = entry.first;
        if (build_ssd && key_equals(param, "BuildSsdIndex")) {
            continue;
        }
        if (build_ssd && key_equals(param, "isExecute")) {
            continue;
        }
        if (key_equals(param, "PostingPageLimit")) {
            param = "SearchPostingPageLimit";
        }
        if (key_equals(param, "InternalResultNum")) {
            param = "SearchInternalResultNum";
        }
        set_config_parameter(config, SPTAG::SSDServing::SEC_BUILD_SSD_INDEX, param.c_str(), entry.second);
    }

    // The SQL adapter opens an existing store. Even if the registered config is
    // the build config emitted by the AE helper, force SPTAG into load-only mode.
    set_config_parameter(config, SPTAG::SSDServing::SEC_BASE, "VectorPath", "");
    set_config_parameter(config, SPTAG::SSDServing::SEC_SELECT_HEAD, "isExecute", "false");
    set_config_parameter(config, SPTAG::SSDServing::SEC_BUILD_HEAD, "isExecute", "false");
    set_config_parameter(config, SPTAG::SSDServing::SEC_BUILD_SSD_INDEX, "isExecute", "true");
    set_config_parameter(config, SPTAG::SSDServing::SEC_BUILD_SSD_INDEX, "BuildSsdIndex", "false");
    set_config_parameter(config, SPTAG::SSDServing::SEC_BUILD_SSD_INDEX, "Storage", storage);

    return config;
}

std::shared_ptr<SPTAG::VectorIndex> open_index_from_config(
    const char* config_path,
    const char* storage,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    uint32_t& dim
) {
    SPTAG::VectorValueType value_type = SPTAG::VectorValueType::Undefined;
    auto config = load_search_config(config_path, value_type, storage);
    if (max_check > 0) {
        set_config_parameter(
            config,
            SPTAG::SSDServing::SEC_BUILD_SSD_INDEX,
            "MaxCheck",
            std::to_string(max_check)
        );
    }
    if (internal_result_num > 0) {
        set_config_parameter(
            config,
            SPTAG::SSDServing::SEC_BUILD_SSD_INDEX,
            "SearchInternalResultNum",
            std::to_string(internal_result_num)
        );
    }
    if (posting_page_limit > 0) {
        set_config_parameter(
            config,
            SPTAG::SSDServing::SEC_BUILD_SSD_INDEX,
            "SearchPostingPageLimit",
            std::to_string(posting_page_limit)
        );
    }

    auto index = SPTAG::VectorIndex::CreateInstance(SPTAG::IndexAlgoType::SPANN, value_type);
    if (index == nullptr) {
        throw std::runtime_error("failed to create SPTAG SPANN index for SPFresh store");
    }

    const std::string quantizer_path =
        get_config_parameter(config, SPTAG::SSDServing::SEC_BASE, "QuantizerFilePath");
    if (!quantizer_path.empty() && index->LoadQuantizer(quantizer_path) != SPTAG::ErrorCode::Success) {
        throw std::runtime_error("failed to load SPTAG quantizer: " + quantizer_path);
    }

    for (auto& section : config) {
        for (auto& entry : section.second) {
            index->SetParameter(entry.first, entry.second, section.first);
        }
    }

    const auto ret = index->BuildIndex();
    if (!is_success(ret)) {
        throw std::runtime_error(
            "SPTAG SPFresh store load failed with code " +
            std::to_string(static_cast<int>(ret))
        );
    }
    dim = static_cast<uint32_t>(index->GetFeatureDim());
    return index;
}

int open_spfresh_config(
    const char* config_path,
    const char* storage,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    VxSpfreshIndex* out,
    char** error
) {
    if (out != nullptr) {
        *out = nullptr;
    }
    if (error != nullptr) {
        *error = nullptr;
    }

    try {
        if (config_path == nullptr || config_path[0] == '\0' || out == nullptr) {
            set_error(error, "config_path and out must be non-null");
            return 1;
        }
        if (storage == nullptr || storage[0] == '\0') {
            set_error(error, "storage must be non-empty");
            return 1;
        }

        auto wrapper = std::make_unique<SpfreshWrapper>();
        wrapper->index = open_index_from_config(
            config_path,
            storage,
            max_check,
            internal_result_num,
            posting_page_limit,
            wrapper->dim
        );
        wrapper->value_type = wrapper->index->GetVectorValueType();
        *out = static_cast<VxSpfreshIndex>(wrapper.release());
        return 0;
    } catch (const std::exception& ex) {
        set_error(error, ex.what());
        return 1;
    } catch (...) {
        set_error(error, "unknown SPTAG SPFresh open exception");
        return 1;
    }
}

template <typename T>
T clamp_round(float value, float min_value, float max_value) {
    const float clamped = std::max(min_value, std::min(max_value, value));
    return static_cast<T>(std::lround(clamped));
}

std::vector<std::int8_t> convert_query_i8(const float* query, uint32_t dim) {
    std::vector<std::int8_t> converted;
    converted.reserve(dim);
    for (uint32_t i = 0; i < dim; ++i) {
        converted.push_back(clamp_round<std::int8_t>(query[i], -128.0f, 127.0f));
    }
    return converted;
}

std::vector<std::uint8_t> convert_query_u8(const float* query, uint32_t dim) {
    std::vector<std::uint8_t> converted;
    converted.reserve(dim);
    for (uint32_t i = 0; i < dim; ++i) {
        converted.push_back(clamp_round<std::uint8_t>(query[i], 0.0f, 255.0f));
    }
    return converted;
}

void convert_query_i8_into(const float* query, uint32_t dim, std::vector<std::int8_t>& converted) {
    converted.resize(dim);
    for (uint32_t i = 0; i < dim; ++i) {
        converted[i] = clamp_round<std::int8_t>(query[i], -128.0f, 127.0f);
    }
}

void convert_query_u8_into(const float* query, uint32_t dim, std::vector<std::uint8_t>& converted) {
    converted.resize(dim);
    for (uint32_t i = 0; i < dim; ++i) {
        converted[i] = clamp_round<std::uint8_t>(query[i], 0.0f, 255.0f);
    }
}

uint32_t write_deduplicated_vids(
    SPTAG::QueryResult& result,
    uint32_t k,
    uint64_t* out_row_ids,
    float* out_distances
) {
    std::unordered_map<uint64_t, float> best_by_vid;
    best_by_vid.reserve(k);
    for (uint32_t i = 0; i < k; ++i) {
        auto* item = result.GetResult(static_cast<int>(i));
        if (item == nullptr || item->VID < 0) {
            break;
        }
        const auto vid = static_cast<uint64_t>(item->VID);
        const auto existing = best_by_vid.find(vid);
        if (existing == best_by_vid.end() || item->Dist < existing->second) {
            best_by_vid[vid] = item->Dist;
        }
    }

    std::vector<std::pair<uint64_t, float>> deduplicated;
    deduplicated.reserve(best_by_vid.size());
    for (const auto& entry : best_by_vid) {
        deduplicated.emplace_back(entry.first, entry.second);
    }
    std::sort(deduplicated.begin(), deduplicated.end(), [](const auto& left, const auto& right) {
        if (left.second == right.second) {
            return left.first < right.first;
        }
        return left.second < right.second;
    });

    uint32_t written = 0;
    for (const auto& entry : deduplicated) {
        if (written >= k) {
            break;
        }
        out_row_ids[written] = entry.first;
        out_distances[written] = entry.second;
        ++written;
    }
    return written;
}

void set_search_parameters(
    SpfreshWrapper* wrapper,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit
) {
    if (max_check > 0) {
        wrapper->index->SetParameter("MaxCheck", std::to_string(max_check), SPTAG::SSDServing::SEC_BUILD_SSD_INDEX);
    }
    if (internal_result_num > 0) {
        wrapper->index->SetParameter(
            "SearchInternalResultNum",
            std::to_string(internal_result_num),
            SPTAG::SSDServing::SEC_BUILD_SSD_INDEX
        );
    }
    if (posting_page_limit > 0) {
        wrapper->index->SetParameter(
            "SearchPostingPageLimit",
            std::to_string(posting_page_limit),
            SPTAG::SSDServing::SEC_BUILD_SSD_INDEX
        );
    }
}

int search_raw_preconfigured(
    SpfreshWrapper* wrapper,
    const void* typed_query,
    uint32_t k,
    uint64_t* out_row_ids,
    float* out_distances,
    uint32_t* out_len,
    char** error
) {
    SPTAG::QueryResult result(typed_query, static_cast<int>(k), false);
    const auto ret = wrapper->index->SearchIndex(result);
    if (!is_success(ret)) {
        set_error(error, "SPTAG SPFresh SearchIndex failed with code " + std::to_string(static_cast<int>(ret)));
        return 1;
    }
    *out_len = write_deduplicated_vids(result, k, out_row_ids, out_distances);
    return 0;
}

int search_raw(
    SpfreshWrapper* wrapper,
    const void* typed_query,
    uint32_t k,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    uint64_t* out_row_ids,
    float* out_distances,
    uint32_t* out_len,
    char** error
) {
    set_search_parameters(wrapper, max_check, internal_result_num, posting_page_limit);
    return search_raw_preconfigured(wrapper, typed_query, k, out_row_ids, out_distances, out_len, error);
}

int search_batch_float_preconfigured(
    SpfreshWrapper* wrapper,
    const float* queries,
    uint32_t query_count,
    uint32_t dim,
    uint32_t k,
    uint64_t* out_row_ids,
    float* out_distances,
    uint32_t* out_lens,
    char** error
) {
    for (uint32_t query_id = 0; query_id < query_count; ++query_id) {
        const auto query_offset = static_cast<std::size_t>(query_id) * dim;
        const auto output_offset = static_cast<std::size_t>(query_id) * k;
        const int code = search_raw_preconfigured(
            wrapper,
            queries + query_offset,
            k,
            out_row_ids + output_offset,
            out_distances + output_offset,
            out_lens + query_id,
            error
        );
        if (code != 0) {
            return code;
        }
    }
    return 0;
}

int search_batch_i8_preconfigured(
    SpfreshWrapper* wrapper,
    const float* queries,
    uint32_t query_count,
    uint32_t dim,
    uint32_t k,
    uint64_t* out_row_ids,
    float* out_distances,
    uint32_t* out_lens,
    char** error
) {
    std::vector<std::int8_t> converted;
    converted.reserve(dim);
    for (uint32_t query_id = 0; query_id < query_count; ++query_id) {
        const auto query_offset = static_cast<std::size_t>(query_id) * dim;
        const auto output_offset = static_cast<std::size_t>(query_id) * k;
        convert_query_i8_into(queries + query_offset, dim, converted);
        const int code = search_raw_preconfigured(
            wrapper,
            converted.data(),
            k,
            out_row_ids + output_offset,
            out_distances + output_offset,
            out_lens + query_id,
            error
        );
        if (code != 0) {
            return code;
        }
    }
    return 0;
}

int search_batch_u8_preconfigured(
    SpfreshWrapper* wrapper,
    const float* queries,
    uint32_t query_count,
    uint32_t dim,
    uint32_t k,
    uint64_t* out_row_ids,
    float* out_distances,
    uint32_t* out_lens,
    char** error
) {
    std::vector<std::uint8_t> converted;
    converted.reserve(dim);
    for (uint32_t query_id = 0; query_id < query_count; ++query_id) {
        const auto query_offset = static_cast<std::size_t>(query_id) * dim;
        const auto output_offset = static_cast<std::size_t>(query_id) * k;
        convert_query_u8_into(queries + query_offset, dim, converted);
        const int code = search_raw_preconfigured(
            wrapper,
            converted.data(),
            k,
            out_row_ids + output_offset,
            out_distances + output_offset,
            out_lens + query_id,
            error
        );
        if (code != 0) {
            return code;
        }
    }
    return 0;
}

std::shared_ptr<SPTAG::VectorSet> load_default_vector_file(
    const char* vector_path,
    SPTAG::VectorValueType value_type,
    uint32_t dim
) {
    auto options = std::make_shared<SPTAG::Helper::ReaderOptions>(
        value_type,
        static_cast<SPTAG::DimensionType>(dim),
        SPTAG::VectorFileType::DEFAULT
    );
    auto reader = SPTAG::Helper::VectorSetReader::CreateInstance(options);
    if (reader == nullptr) {
        throw std::runtime_error("failed to create SPTAG vector reader");
    }
    const auto ret = reader->LoadFile(vector_path);
    if (!is_success(ret)) {
        throw std::runtime_error(error_code_message("SPTAG vector file load", ret));
    }
    auto vector_set = reader->GetVectorSet();
    if (vector_set == nullptr || !vector_set->Available()) {
        throw std::runtime_error("SPTAG vector file reader returned no vectors");
    }
    if (vector_set->GetValueType() != value_type) {
        throw std::runtime_error("SPTAG vector file value type does not match index value type");
    }
    if (vector_set->Dimension() != static_cast<SPTAG::DimensionType>(dim)) {
        throw std::runtime_error("SPTAG vector file dimension does not match index dimension");
    }
    if (vector_set->Count() == 0) {
        throw std::runtime_error("SPTAG vector file contains zero vectors");
    }
    return vector_set;
}

template <typename T>
SPTAG::SPANN::Index<T>* as_spann_index(SpfreshWrapper* wrapper) {
    auto* typed = dynamic_cast<SPTAG::SPANN::Index<T>*>(wrapper->index.get());
    if (typed == nullptr) {
        throw std::runtime_error("SPTAG handle is not the expected SPANN index type");
    }
    return typed;
}

template <typename T>
int insert_default_file_typed(
    SpfreshWrapper* wrapper,
    const char* vector_path,
    uint64_t* out_inserted,
    uint64_t* out_first_vid,
    char** error
) {
    auto* typed = as_spann_index<T>(wrapper);
    auto vector_set = load_default_vector_file(vector_path, wrapper->value_type, wrapper->dim);
    std::vector<SPTAG::SizeType> vids(vector_set->Count());
    const auto ret = typed->AddIndexSPFresh(
        vector_set->GetData(),
        vector_set->Count(),
        vector_set->Dimension(),
        vids.data()
    );
    if (!is_success(ret)) {
        set_error(error, error_code_message("SPTAG SPFresh AddIndexSPFresh", ret));
        return 1;
    }
    *out_inserted = static_cast<uint64_t>(vector_set->Count());
    *out_first_vid = vids.empty() ? 0 : static_cast<uint64_t>(vids.front());
    return 0;
}

template <typename T>
void wait_all_finished_typed(SpfreshWrapper* wrapper) {
    auto* typed = as_spann_index<T>(wrapper);
    while (!typed->AllFinished()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

void wait_all_finished(SpfreshWrapper* wrapper) {
    switch (wrapper->value_type) {
    case SPTAG::VectorValueType::Float:
        wait_all_finished_typed<float>(wrapper);
        break;
    case SPTAG::VectorValueType::Int8:
        wait_all_finished_typed<std::int8_t>(wrapper);
        break;
    case SPTAG::VectorValueType::UInt8:
        wait_all_finished_typed<std::uint8_t>(wrapper);
        break;
    default:
        throw std::runtime_error("SPFresh mutation supports only Float, Int8, and UInt8 value types");
    }
}

std::vector<uint64_t> read_delete_ids_file(const char* delete_path) {
    std::ifstream input(delete_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open SPFresh delete ID file");
    }
    uint32_t count = 0;
    input.read(reinterpret_cast<char*>(&count), sizeof(count));
    if (!input) {
        throw std::runtime_error("SPFresh delete ID file has a truncated count header");
    }
    std::vector<uint64_t> ids(count);
    if (count > 0) {
        input.read(reinterpret_cast<char*>(ids.data()), static_cast<std::streamsize>(count * sizeof(uint64_t)));
        if (!input) {
            throw std::runtime_error("SPFresh delete ID file has a truncated VID payload");
        }
    }
    char trailing = 0;
    if (input.read(&trailing, 1)) {
        throw std::runtime_error("SPFresh delete ID file has trailing bytes after VID payload");
    }
    return ids;
}

void read_exact(std::ifstream& input, char* data, std::streamsize bytes, const std::string& description) {
    if (bytes == 0) {
        return;
    }
    input.read(data, bytes);
    if (!input) {
        throw std::runtime_error(description + " is truncated");
    }
}

void ensure_no_trailing_bytes(std::ifstream& input, const std::string& description) {
    char trailing = 0;
    if (input.read(&trailing, 1)) {
        throw std::runtime_error(description + " has trailing bytes");
    }
}

SPTAG::SizeType size_type_from_u32(std::uint32_t value, const std::string& description) {
    if (value > static_cast<std::uint32_t>(std::numeric_limits<SPTAG::SizeType>::max())) {
        throw std::runtime_error(description + " exceeds SPTAG SizeType range");
    }
    return static_cast<SPTAG::SizeType>(value);
}

SPTAG::SizeType size_type_from_size(std::size_t value, const std::string& description) {
    if (value > static_cast<std::size_t>(std::numeric_limits<SPTAG::SizeType>::max())) {
        throw std::runtime_error(description + " exceeds SPTAG SizeType range");
    }
    return static_cast<SPTAG::SizeType>(value);
}

SPTAG::DimensionType dim_type_from_u32(std::uint32_t value, const std::string& description) {
    if (value > static_cast<std::uint32_t>(std::numeric_limits<SPTAG::DimensionType>::max())) {
        throw std::runtime_error(description + " exceeds SPTAG DimensionType range");
    }
    return static_cast<SPTAG::DimensionType>(value);
}

std::streamsize streamsize_from_u64(std::uint64_t value, const std::string& description) {
    if (value > static_cast<std::uint64_t>(std::numeric_limits<std::streamsize>::max())) {
        throw std::runtime_error(description + " exceeds streamsize range");
    }
    return static_cast<std::streamsize>(value);
}

std::streamoff streamoff_from_u64(std::uint64_t value, const std::string& description) {
    if (value > static_cast<std::uint64_t>(std::numeric_limits<std::streamoff>::max())) {
        throw std::runtime_error(description + " exceeds stream offset range");
    }
    return static_cast<std::streamoff>(value);
}

UpdateTrace read_update_trace_file(const char* trace_path) {
    std::ifstream input(trace_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open SPFresh update trace file");
    }

    std::uint32_t count = 0;
    read_exact(
        input,
        reinterpret_cast<char*>(&count),
        static_cast<std::streamsize>(sizeof(count)),
        "SPFresh update trace count"
    );
    size_type_from_u32(count, "SPFresh update trace count");

    UpdateTrace trace;
    trace.delete_ids.resize(count);
    trace.insert_ids.resize(count);
    const auto payload_bytes = streamsize_from_u64(
        static_cast<std::uint64_t>(count) * sizeof(std::uint32_t),
        "SPFresh update trace payload"
    );
    read_exact(
        input,
        reinterpret_cast<char*>(trace.delete_ids.data()),
        payload_bytes,
        "SPFresh update trace delete set"
    );
    read_exact(
        input,
        reinterpret_cast<char*>(trace.insert_ids.data()),
        payload_bytes,
        "SPFresh update trace insert set"
    );
    ensure_no_trailing_bytes(input, "SPFresh update trace");
    return trace;
}

std::vector<SPTAG::SizeType> read_update_mapping_file(const char* mapping_path) {
    std::ifstream input(mapping_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open SPFresh update mapping file");
    }

    std::int32_t count = 0;
    read_exact(
        input,
        reinterpret_cast<char*>(&count),
        static_cast<std::streamsize>(sizeof(count)),
        "SPFresh update mapping count"
    );
    if (count < 0) {
        throw std::runtime_error("SPFresh update mapping count is negative");
    }

    std::vector<SPTAG::SizeType> mapping(static_cast<std::size_t>(count));
    const auto payload_bytes = streamsize_from_u64(
        static_cast<std::uint64_t>(mapping.size()) * sizeof(SPTAG::SizeType),
        "SPFresh update mapping payload"
    );
    read_exact(
        input,
        reinterpret_cast<char*>(mapping.data()),
        payload_bytes,
        "SPFresh update mapping payload"
    );
    ensure_no_trailing_bytes(input, "SPFresh update mapping");
    return mapping;
}

void write_update_mapping_file(const char* mapping_path, const std::vector<SPTAG::SizeType>& mapping) {
    const auto count = size_type_from_size(mapping.size(), "SPFresh update mapping count");
    std::ofstream output(mapping_path, std::ios::binary | std::ios::trunc);
    if (!output) {
        throw std::runtime_error("failed to write SPFresh update mapping file");
    }
    output.write(reinterpret_cast<const char*>(&count), sizeof(count));
    if (!output) {
        throw std::runtime_error("failed to write SPFresh update mapping count");
    }
    if (!mapping.empty()) {
        output.write(
            reinterpret_cast<const char*>(mapping.data()),
            static_cast<std::streamsize>(mapping.size() * sizeof(SPTAG::SizeType))
        );
        if (!output) {
            throw std::runtime_error("failed to write SPFresh update mapping payload");
        }
    }
}

SPTAG::SizeType mapped_vid_for_row(
    std::uint32_t row_id,
    const std::vector<SPTAG::SizeType>* mapping
) {
    if (mapping == nullptr) {
        return size_type_from_u32(row_id, "SPFresh update trace row ID");
    }
    if (row_id >= mapping->size()) {
        throw std::runtime_error("SPFresh update mapping does not cover a trace row ID");
    }
    const auto vid = (*mapping)[row_id];
    if (vid < 0) {
        throw std::runtime_error("SPFresh update mapping contains a negative VID");
    }
    return vid;
}

template <typename T>
std::shared_ptr<SPTAG::VectorSet> load_update_vectors_from_default_file(
    const char* vector_path,
    const std::vector<std::uint32_t>& insert_ids,
    SPTAG::VectorValueType value_type,
    std::uint32_t dim
) {
    std::ifstream input(vector_path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open SPFresh update vector source");
    }

    std::int32_t row_count = 0;
    std::int32_t file_dim = 0;
    read_exact(
        input,
        reinterpret_cast<char*>(&row_count),
        static_cast<std::streamsize>(sizeof(row_count)),
        "SPFresh update vector source row count"
    );
    read_exact(
        input,
        reinterpret_cast<char*>(&file_dim),
        static_cast<std::streamsize>(sizeof(file_dim)),
        "SPFresh update vector source dimension"
    );
    if (row_count < 0) {
        throw std::runtime_error("SPFresh update vector source row count is negative");
    }
    if (file_dim != static_cast<std::int32_t>(dim_type_from_u32(dim, "SPFresh index dimension"))) {
        throw std::runtime_error("SPFresh update vector source dimension does not match index dimension");
    }

    const auto row_count_u32 = static_cast<std::uint32_t>(row_count);
    const auto per_vector_bytes = static_cast<std::uint64_t>(sizeof(T)) * dim;
    if (insert_ids.size() != 0 &&
        per_vector_bytes > std::numeric_limits<std::uint64_t>::max() / insert_ids.size()) {
        throw std::runtime_error("SPFresh update vector buffer size overflow");
    }
    const auto total_bytes = per_vector_bytes * insert_ids.size();
    if (total_bytes > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        throw std::runtime_error("SPFresh update vector buffer exceeds addressable memory");
    }

    auto vector_bytes = SPTAG::ByteArray::Alloc(static_cast<std::size_t>(total_bytes));
    char* output = reinterpret_cast<char*>(vector_bytes.Data());
    const auto read_size = streamsize_from_u64(per_vector_bytes, "SPFresh update vector row");

    for (std::size_t i = 0; i < insert_ids.size(); ++i) {
        const auto row_id = insert_ids[i];
        if (row_id >= row_count_u32) {
            throw std::runtime_error("SPFresh update trace insert row ID exceeds vector source row count");
        }
        const auto offset = static_cast<std::uint64_t>(sizeof(SPTAG::SizeType) + sizeof(SPTAG::DimensionType)) +
            static_cast<std::uint64_t>(row_id) * per_vector_bytes;
        input.clear();
        input.seekg(streamoff_from_u64(offset, "SPFresh update vector row offset"), std::ios::beg);
        if (!input) {
            throw std::runtime_error("failed to seek SPFresh update vector source");
        }
        read_exact(
            input,
            output + i * static_cast<std::size_t>(per_vector_bytes),
            read_size,
            "SPFresh update vector row"
        );
    }

    return std::make_shared<SPTAG::BasicVectorSet>(
        vector_bytes,
        value_type,
        dim_type_from_u32(dim, "SPFresh update vector dimension"),
        size_type_from_size(insert_ids.size(), "SPFresh update vector count")
    );
}

template <typename T>
int apply_trace_typed(
    SpfreshWrapper* wrapper,
    const char* trace_path,
    const char* vector_path,
    const char* update_mapping_path,
    uint64_t* out_inserted,
    uint64_t* out_deleted,
    uint64_t* out_first_vid,
    VxSpfreshApplyTraceProfile* profile,
    char** error
) {
    const auto total_start = std::chrono::steady_clock::now();
    auto* typed = as_spann_index<T>(wrapper);
    auto stage_start = std::chrono::steady_clock::now();
    const auto trace = read_update_trace_file(trace_path);
    if (profile != nullptr) {
        profile->read_trace_ms = elapsed_ms(stage_start);
    }

    const bool has_mapping = update_mapping_path != nullptr && update_mapping_path[0] != '\0';
    std::vector<SPTAG::SizeType> mapping;
    std::vector<SPTAG::SizeType>* mapping_ptr = nullptr;
    if (has_mapping) {
        stage_start = std::chrono::steady_clock::now();
        mapping = read_update_mapping_file(update_mapping_path);
        if (profile != nullptr) {
            profile->read_mapping_ms = elapsed_ms(stage_start);
        }
        mapping_ptr = &mapping;
    }

    uint64_t deleted = 0;
    stage_start = std::chrono::steady_clock::now();
    for (const auto row_id : trace.delete_ids) {
        const auto vid = mapped_vid_for_row(row_id, mapping_ptr);
        const auto ret = wrapper->index->DeleteIndex(vid);
        if (!is_success(ret)) {
            set_error(error, error_code_message("SPTAG SPFresh DeleteIndex", ret));
            return 1;
        }
        ++deleted;
    }
    if (profile != nullptr) {
        profile->delete_ms = elapsed_ms(stage_start);
    }

    uint64_t inserted = 0;
    uint64_t first_vid = 0;
    if (!trace.insert_ids.empty()) {
        stage_start = std::chrono::steady_clock::now();
        auto vector_set = load_update_vectors_from_default_file<T>(
            vector_path,
            trace.insert_ids,
            wrapper->value_type,
            wrapper->dim
        );
        if (profile != nullptr) {
            profile->load_vectors_ms = elapsed_ms(stage_start);
        }
        std::vector<SPTAG::SizeType> vids(trace.insert_ids.size());
        stage_start = std::chrono::steady_clock::now();
        const auto ret = typed->AddIndexSPFresh(
            vector_set->GetData(),
            vector_set->Count(),
            vector_set->Dimension(),
            vids.data()
        );
        if (!is_success(ret)) {
            set_error(error, error_code_message("SPTAG SPFresh AddIndexSPFresh", ret));
            return 1;
        }
        if (profile != nullptr) {
            profile->add_index_ms = elapsed_ms(stage_start);
        }
        inserted = static_cast<uint64_t>(vids.size());
        first_vid = vids.empty() ? 0 : static_cast<uint64_t>(vids.front());
        if (mapping_ptr != nullptr) {
            for (std::size_t i = 0; i < trace.insert_ids.size(); ++i) {
                const auto row_id = trace.insert_ids[i];
                if (row_id >= mapping_ptr->size()) {
                    set_error(error, "SPFresh update mapping does not cover a trace insert row ID");
                    return 1;
                }
                (*mapping_ptr)[row_id] = vids[i];
            }
            stage_start = std::chrono::steady_clock::now();
            write_update_mapping_file(update_mapping_path, *mapping_ptr);
            if (profile != nullptr) {
                profile->write_mapping_ms = elapsed_ms(stage_start);
            }
        }
    }

    *out_inserted = inserted;
    *out_deleted = deleted;
    *out_first_vid = first_vid;
    if (profile != nullptr) {
        profile->total_ms = elapsed_ms(total_start);
    }
    return 0;
}

} // namespace

extern "C" {

int vx_spfresh_ssdserving_is_available(char** error) {
    if (error != nullptr) {
        *error = nullptr;
    }
    return 0;
}

int vx_spfresh_ssdserving_boot_config(const char* config_path, char** error) {
    if (error != nullptr) {
        *error = nullptr;
    }

    try {
        if (config_path == nullptr || config_path[0] == '\0') {
            set_error(error, "config_path must be non-empty");
            return 1;
        }

        std::map<std::string, std::map<std::string, std::string>> config;
        const int code = SPTAG::SSDServing::BootProgram(false, &config, config_path);
        if (code != 0) {
            set_error(error, "SPTAG SSDServing::BootProgram failed with code " + std::to_string(code));
            return code;
        }
        return 0;
    } catch (const std::exception& ex) {
        set_error(error, ex.what());
        return 1;
    } catch (...) {
        set_error(error, "unknown SPTAG SSDServing exception");
        return 1;
    }
}

int vx_spfresh_static_open_config(
    const char* config_path,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    VxSpfreshIndex* out,
    char** error
) {
    return open_spfresh_config(
        config_path,
        "STATIC",
        max_check,
        internal_result_num,
        posting_page_limit,
        out,
        error
    );
}

int vx_spfresh_dynamic_open_config(
    const char* config_path,
    const char* storage,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    VxSpfreshIndex* out,
    char** error
) {
    return open_spfresh_config(
        config_path,
        storage,
        max_check,
        internal_result_num,
        posting_page_limit,
        out,
        error
    );
}

int vx_spfresh_static_search(
    VxSpfreshIndex handle,
    const float* query,
    uint32_t dim,
    uint32_t k,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    uint64_t* out_row_ids,
    float* out_distances,
    uint32_t* out_len,
    char** error
) {
    if (out_len != nullptr) {
        *out_len = 0;
    }
    if (error != nullptr) {
        *error = nullptr;
    }

    try {
        if (handle == nullptr || query == nullptr || out_row_ids == nullptr || out_distances == nullptr ||
            out_len == nullptr) {
            set_error(error, "index, query, output buffers, and out_len must be non-null");
            return 1;
        }
        auto* wrapper = static_cast<SpfreshWrapper*>(handle);
        if (dim != wrapper->dim) {
            set_error(error, "query dimension does not match SPFresh index dimension");
            return 1;
        }
        if (k == 0 || k > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
            set_error(error, "k must fit in positive int range");
            return 1;
        }

        switch (wrapper->value_type) {
        case SPTAG::VectorValueType::Float:
            return search_raw(
                wrapper,
                query,
                k,
                max_check,
                internal_result_num,
                posting_page_limit,
                out_row_ids,
                out_distances,
                out_len,
                error
            );
        case SPTAG::VectorValueType::Int8: {
            auto converted = convert_query_i8(query, dim);
            return search_raw(
                wrapper,
                converted.data(),
                k,
                max_check,
                internal_result_num,
                posting_page_limit,
                out_row_ids,
                out_distances,
                out_len,
                error
            );
        }
        case SPTAG::VectorValueType::UInt8: {
            auto converted = convert_query_u8(query, dim);
            return search_raw(
                wrapper,
                converted.data(),
                k,
                max_check,
                internal_result_num,
                posting_page_limit,
                out_row_ids,
                out_distances,
                out_len,
                error
            );
        }
        default:
            set_error(error, "SPFresh static search supports only Float, Int8, and UInt8 value types");
            return 1;
        }
    } catch (const std::exception& ex) {
        set_error(error, ex.what());
        return 1;
    } catch (...) {
        set_error(error, "unknown SPTAG SPFresh static search exception");
        return 1;
    }
}

int vx_spfresh_static_search_batch(
    VxSpfreshIndex handle,
    const float* queries,
    uint32_t query_count,
    uint32_t dim,
    uint32_t k,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    uint64_t* out_row_ids,
    float* out_distances,
    uint32_t* out_lens,
    char** error
) {
    if (error != nullptr) {
        *error = nullptr;
    }

    try {
        if (handle == nullptr || queries == nullptr || out_row_ids == nullptr || out_distances == nullptr ||
            out_lens == nullptr) {
            set_error(error, "index, queries, output buffers, and out_lens must be non-null");
            return 1;
        }
        for (uint32_t query_id = 0; query_id < query_count; ++query_id) {
            out_lens[query_id] = 0;
        }

        auto* wrapper = static_cast<SpfreshWrapper*>(handle);
        if (query_count == 0) {
            set_error(error, "query_count must be positive");
            return 1;
        }
        if (dim != wrapper->dim) {
            set_error(error, "query dimension does not match SPFresh index dimension");
            return 1;
        }
        if (k == 0 || k > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
            set_error(error, "k must fit in positive int range");
            return 1;
        }
        const auto max_size = static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max());
        if (static_cast<std::uint64_t>(query_count) * dim > max_size ||
            static_cast<std::uint64_t>(query_count) * k > max_size) {
            set_error(error, "SPFresh batch search buffer size exceeds addressable memory");
            return 1;
        }

        set_search_parameters(wrapper, max_check, internal_result_num, posting_page_limit);
        switch (wrapper->value_type) {
        case SPTAG::VectorValueType::Float:
            return search_batch_float_preconfigured(
                wrapper,
                queries,
                query_count,
                dim,
                k,
                out_row_ids,
                out_distances,
                out_lens,
                error
            );
        case SPTAG::VectorValueType::Int8:
            return search_batch_i8_preconfigured(
                wrapper,
                queries,
                query_count,
                dim,
                k,
                out_row_ids,
                out_distances,
                out_lens,
                error
            );
        case SPTAG::VectorValueType::UInt8:
            return search_batch_u8_preconfigured(
                wrapper,
                queries,
                query_count,
                dim,
                k,
                out_row_ids,
                out_distances,
                out_lens,
                error
            );
        default:
            set_error(error, "SPFresh batch search supports only Float, Int8, and UInt8 value types");
            return 1;
        }
    } catch (const std::exception& ex) {
        set_error(error, ex.what());
        return 1;
    } catch (...) {
        set_error(error, "unknown SPTAG SPFresh batch search exception");
        return 1;
    }
}

int vx_spfresh_mutate_insert_default_file(
    VxSpfreshIndex handle,
    const char* vector_path,
    uint64_t* out_inserted,
    uint64_t* out_first_vid,
    char** error
) {
    if (out_inserted != nullptr) {
        *out_inserted = 0;
    }
    if (out_first_vid != nullptr) {
        *out_first_vid = 0;
    }
    if (error != nullptr) {
        *error = nullptr;
    }

    try {
        if (handle == nullptr || vector_path == nullptr || vector_path[0] == '\0' ||
            out_inserted == nullptr || out_first_vid == nullptr) {
            set_error(error, "index, vector_path, and output counters must be non-null");
            return 1;
        }
        auto* wrapper = static_cast<SpfreshWrapper*>(handle);
        switch (wrapper->value_type) {
        case SPTAG::VectorValueType::Float:
            return insert_default_file_typed<float>(wrapper, vector_path, out_inserted, out_first_vid, error);
        case SPTAG::VectorValueType::Int8:
            return insert_default_file_typed<std::int8_t>(wrapper, vector_path, out_inserted, out_first_vid, error);
        case SPTAG::VectorValueType::UInt8:
            return insert_default_file_typed<std::uint8_t>(wrapper, vector_path, out_inserted, out_first_vid, error);
        default:
            set_error(error, "SPFresh insert supports only Float, Int8, and UInt8 value types");
            return 1;
        }
    } catch (const std::exception& ex) {
        set_error(error, ex.what());
        return 1;
    } catch (...) {
        set_error(error, "unknown SPTAG SPFresh insert exception");
        return 1;
    }
}

int vx_spfresh_mutate_delete_ids_file(
    VxSpfreshIndex handle,
    const char* delete_path,
    uint64_t* out_deleted,
    char** error
) {
    if (out_deleted != nullptr) {
        *out_deleted = 0;
    }
    if (error != nullptr) {
        *error = nullptr;
    }

    try {
        if (handle == nullptr || delete_path == nullptr || delete_path[0] == '\0' || out_deleted == nullptr) {
            set_error(error, "index, delete_path, and out_deleted must be non-null");
            return 1;
        }
        auto ids = read_delete_ids_file(delete_path);
        auto* wrapper = static_cast<SpfreshWrapper*>(handle);
        uint64_t deleted = 0;
        for (uint64_t id : ids) {
            if (id > static_cast<uint64_t>(std::numeric_limits<SPTAG::SizeType>::max())) {
                set_error(error, "SPFresh delete VID exceeds native SizeType range");
                return 1;
            }
            const auto ret = wrapper->index->DeleteIndex(static_cast<SPTAG::SizeType>(id));
            if (!is_success(ret)) {
                set_error(error, error_code_message("SPTAG SPFresh DeleteIndex", ret));
                return 1;
            }
            ++deleted;
        }
        *out_deleted = deleted;
        return 0;
    } catch (const std::exception& ex) {
        set_error(error, ex.what());
        return 1;
    } catch (...) {
        set_error(error, "unknown SPTAG SPFresh delete exception");
        return 1;
    }
}

int vx_spfresh_mutate_apply_trace(
    VxSpfreshIndex handle,
    const char* trace_path,
    const char* vector_path,
    const char* update_mapping_path,
    uint64_t* out_inserted,
    uint64_t* out_deleted,
    uint64_t* out_first_vid,
    char** error
) {
    return vx_spfresh_mutate_apply_trace_profiled(
        handle,
        trace_path,
        vector_path,
        update_mapping_path,
        out_inserted,
        out_deleted,
        out_first_vid,
        nullptr,
        error
    );
}

int vx_spfresh_mutate_apply_trace_profiled(
    VxSpfreshIndex handle,
    const char* trace_path,
    const char* vector_path,
    const char* update_mapping_path,
    uint64_t* out_inserted,
    uint64_t* out_deleted,
    uint64_t* out_first_vid,
    VxSpfreshApplyTraceProfile* profile,
    char** error
) {
    if (out_inserted != nullptr) {
        *out_inserted = 0;
    }
    if (out_deleted != nullptr) {
        *out_deleted = 0;
    }
    if (out_first_vid != nullptr) {
        *out_first_vid = 0;
    }
    if (error != nullptr) {
        *error = nullptr;
    }
    reset_profile(profile);

    try {
        if (handle == nullptr || trace_path == nullptr || trace_path[0] == '\0' ||
            vector_path == nullptr || vector_path[0] == '\0' ||
            out_inserted == nullptr || out_deleted == nullptr || out_first_vid == nullptr) {
            set_error(error, "index, trace_path, vector_path, and output counters must be non-null");
            return 1;
        }
        auto* wrapper = static_cast<SpfreshWrapper*>(handle);
        switch (wrapper->value_type) {
        case SPTAG::VectorValueType::Float:
            return apply_trace_typed<float>(
                wrapper,
                trace_path,
                vector_path,
                update_mapping_path,
                out_inserted,
                out_deleted,
                out_first_vid,
                profile,
                error
            );
        case SPTAG::VectorValueType::Int8:
            return apply_trace_typed<std::int8_t>(
                wrapper,
                trace_path,
                vector_path,
                update_mapping_path,
                out_inserted,
                out_deleted,
                out_first_vid,
                profile,
                error
            );
        case SPTAG::VectorValueType::UInt8:
            return apply_trace_typed<std::uint8_t>(
                wrapper,
                trace_path,
                vector_path,
                update_mapping_path,
                out_inserted,
                out_deleted,
                out_first_vid,
                profile,
                error
            );
        default:
            set_error(error, "SPFresh apply_trace supports only Float, Int8, and UInt8 value types");
            return 1;
        }
    } catch (const std::exception& ex) {
        set_error(error, ex.what());
        return 1;
    } catch (...) {
        set_error(error, "unknown SPTAG SPFresh apply_trace exception");
        return 1;
    }
}

int vx_spfresh_save(VxSpfreshIndex handle, const char* store_root, char** error) {
    return vx_spfresh_save_profiled(handle, store_root, nullptr, error);
}

int vx_spfresh_save_profiled(
    VxSpfreshIndex handle,
    const char* store_root,
    VxSpfreshSaveProfile* profile,
    char** error
) {
    if (error != nullptr) {
        *error = nullptr;
    }
    reset_profile(profile);

    try {
        if (handle == nullptr || store_root == nullptr || store_root[0] == '\0') {
            set_error(error, "index and store_root must be non-null");
            return 1;
        }
        auto* wrapper = static_cast<SpfreshWrapper*>(handle);
        const auto total_start = std::chrono::steady_clock::now();
        auto stage_start = std::chrono::steady_clock::now();
        wait_all_finished(wrapper);
        if (profile != nullptr) {
            profile->wait_ms = elapsed_ms(stage_start);
        }
        stage_start = std::chrono::steady_clock::now();
        const auto ret = wrapper->index->SaveIndex(store_root);
        if (profile != nullptr) {
            profile->save_index_ms = elapsed_ms(stage_start);
            profile->total_ms = elapsed_ms(total_start);
        }
        if (!is_success(ret)) {
            set_error(error, error_code_message("SPTAG SPFresh SaveIndex", ret));
            return 1;
        }
        return 0;
    } catch (const std::exception& ex) {
        set_error(error, ex.what());
        return 1;
    } catch (...) {
        set_error(error, "unknown SPTAG SPFresh save exception");
        return 1;
    }
}

void vx_spfresh_static_close(VxSpfreshIndex handle) {
    delete static_cast<SpfreshWrapper*>(handle);
}

void vx_spfresh_free_error(char* error) {
    delete[] error;
}

} // extern "C"

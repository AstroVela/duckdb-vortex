// SPDX-License-Identifier: Apache-2.0

#ifndef VORTEX_SPTAG_SPFRESH_FFI_H
#define VORTEX_SPTAG_SPFRESH_FFI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* VxSpfreshIndex;

typedef struct VxSpfreshApplyTraceProfile {
    double read_trace_ms;
    double read_mapping_ms;
    double delete_ms;
    double load_vectors_ms;
    double add_index_ms;
    double write_mapping_ms;
    double total_ms;
} VxSpfreshApplyTraceProfile;

typedef struct VxSpfreshSaveProfile {
    double wait_ms;
    double save_index_ms;
    double total_ms;
} VxSpfreshSaveProfile;

int vx_spfresh_ssdserving_is_available(char** error);

int vx_spfresh_ssdserving_boot_config(const char* config_path, char** error);

int vx_spfresh_static_open_config(
    const char* config_path,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    VxSpfreshIndex* out,
    char** error
);

int vx_spfresh_dynamic_open_config(
    const char* config_path,
    const char* storage,
    uint32_t max_check,
    uint32_t internal_result_num,
    uint32_t posting_page_limit,
    VxSpfreshIndex* out,
    char** error
);

int vx_spfresh_static_search(
    VxSpfreshIndex index,
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
);

int vx_spfresh_static_search_batch(
    VxSpfreshIndex index,
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
);

int vx_spfresh_mutate_insert_default_file(
    VxSpfreshIndex index,
    const char* vector_path,
    uint64_t* out_inserted,
    uint64_t* out_first_vid,
    char** error
);

int vx_spfresh_mutate_delete_ids_file(
    VxSpfreshIndex index,
    const char* delete_path,
    uint64_t* out_deleted,
    char** error
);

int vx_spfresh_mutate_apply_trace(
    VxSpfreshIndex index,
    const char* trace_path,
    const char* vector_path,
    const char* update_mapping_path,
    uint64_t* out_inserted,
    uint64_t* out_deleted,
    uint64_t* out_first_vid,
    char** error
);

int vx_spfresh_mutate_apply_trace_profiled(
    VxSpfreshIndex index,
    const char* trace_path,
    const char* vector_path,
    const char* update_mapping_path,
    uint64_t* out_inserted,
    uint64_t* out_deleted,
    uint64_t* out_first_vid,
    VxSpfreshApplyTraceProfile* profile,
    char** error
);

int vx_spfresh_save(VxSpfreshIndex index, const char* store_root, char** error);

int vx_spfresh_save_profiled(
    VxSpfreshIndex index,
    const char* store_root,
    VxSpfreshSaveProfile* profile,
    char** error
);

void vx_spfresh_static_close(VxSpfreshIndex index);

void vx_spfresh_free_error(char* error);

#ifdef __cplusplus
}
#endif

#endif

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ffi::CStr;
use std::ffi::c_char;
use std::ffi::c_void;
use std::ptr;

use num_traits::AsPrimitive;
use vortex::error::VortexExpect;

use crate::convert::can_push_expression;
use crate::copy::CopyFunctionBind;
use crate::copy::CopyFunctionGlobal;
use crate::copy::copy_to_bind;
use crate::copy::copy_to_finalize;
use crate::copy::copy_to_initialize_global;
use crate::copy::copy_to_sink;
use crate::cpp;
#[cfg(feature = "vane")]
use crate::distributed::DistributedRuntimeGlobal;
#[cfg(feature = "vane")]
use crate::distributed::PortableDistributedBind;
#[cfg(feature = "vane")]
use crate::distributed::deserialize_runtime_bind;
#[cfg(feature = "vane")]
use crate::distributed::serialize_bind;
use crate::duckdb::AggregatePushdownInput;
use crate::duckdb::BindInput;
use crate::duckdb::BindResult;
use crate::duckdb::Data;
use crate::duckdb::DataChunk;
use crate::duckdb::DuckdbStringMap;
use crate::duckdb::Expression;
use crate::duckdb::LogicalType;
use crate::duckdb::LogicalTypeRef;
#[cfg(feature = "vane")]
use crate::duckdb::TableFilterSet;
use crate::duckdb::TableInitInput;
use crate::duckdb::try_or;
use crate::duckdb::try_or_null;
#[cfg(feature = "vane")]
use crate::projection::distributed_file_index_is_selected;
use crate::table_function::Cardinality;
use crate::table_function::TableFunctionBind;
use crate::table_function::TableFunctionGlobal;
use crate::table_function::TableFunctionLocal;
use crate::table_function::bind;
use crate::table_function::cardinality;
use crate::table_function::get_partition_data;
use crate::table_function::init_global;
use crate::table_function::init_local;
use crate::table_function::pushdown_complex_filter;
use crate::table_function::pushdown_projection_aggregates;
use crate::table_function::pushdown_projection_expression;
use crate::table_function::scan;
use crate::table_function::statistics;
use crate::table_function::table_scan_progress;
use crate::table_function::to_string;

#[repr(C)]
#[cfg(feature = "vane")]
pub struct VortexDistributedFileView {
    pub source_url: *const u8,
    pub source_url_len: usize,
    pub path: *const u8,
    pub path_len: usize,
    pub size: u64,
    pub has_size: bool,
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_to_string(
    bind_data: *const c_void,
    map: cpp::duckdb_vx_string_map,
) {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let map = unsafe { DuckdbStringMap::borrow_mut(map) };
    to_string(bind_data, map);
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_statistics(
    bind_data: *const c_void,
    column_index: usize,
    stats_out: *mut cpp::duckdb_column_statistics,
) -> bool {
    let stats_out = unsafe { &mut *stats_out };
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let Some(stats) = statistics(bind_data, column_index) else {
        return false;
    };
    stats_out.min = stats.min.map_or(ptr::null_mut(), |v| v.into_ptr());
    stats_out.max = stats.max.map_or(ptr::null_mut(), |v| v.into_ptr());
    stats_out.max_string_length = stats.max_string_length;
    stats_out.has_null = stats.has_null;
    true
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_scan_progress(global_state: *mut c_void) -> f64 {
    let global_state = unsafe { global_state.cast::<TableFunctionGlobal>().as_ref() }
        .vortex_expect("global_init_data null pointer");
    table_scan_progress(global_state)
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_get_partition_data(
    global_init_data: *mut c_void,
    local_init_data: *mut c_void,
    partition_data_out: *mut cpp::duckdb_vx_partition_data,
) {
    let global_init_data = unsafe { global_init_data.cast::<TableFunctionGlobal>().as_ref() }
        .vortex_expect("global_init_data null pointer");
    let local_init_data = unsafe { local_init_data.cast::<TableFunctionLocal>().as_mut() }
        .vortex_expect("local_init_data null pointer");
    let data = get_partition_data(global_init_data, local_init_data);
    let out = unsafe { &mut *partition_data_out };

    out.partition_index = data.partition_index;
    out.file_index_column_pos = data.file_index_column_pos.unwrap_or(usize::MAX);
    out.file_index = data.file_index;
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_pushdown_complex_filter(
    bind_data: *mut c_void,
    expr: cpp::duckdb_vx_expr,
    error_out: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_mut() }
        .vortex_expect("bind_data null pointer");
    let expr = unsafe { Expression::borrow(expr) };
    try_or(error_out, || pushdown_complex_filter(bind_data, expr))
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_pushdown_projection_expression(
    bind_data: *mut c_void,
    expr: cpp::duckdb_vx_expr,
    column_id: usize,
    error_out: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_mut() }
        .vortex_expect("bind_data null pointer");
    let expr = unsafe { Expression::borrow(expr) };
    try_or(error_out, || {
        pushdown_projection_expression(bind_data, expr, column_id)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_pushdown_projection_aggregates(
    bind_data: *mut c_void,
    input: cpp::duckdb_vx_agg_input,
    error_out: *mut cpp::duckdb_vx_error,
) -> bool {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_mut() }
        .vortex_expect("bind_data null pointer");
    let input = unsafe { AggregatePushdownInput::borrow(input) };
    try_or(error_out, || {
        pushdown_projection_aggregates(bind_data, input)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C-unwind" fn duckdb_table_function_scan(
    global_init_data: *mut c_void,
    local_init_data: *mut c_void,
    output: cpp::duckdb_data_chunk,
    error_out: *mut cpp::duckdb_vx_error,
) {
    let global_init_data = unsafe { global_init_data.cast::<TableFunctionGlobal>().as_ref() }
        .vortex_expect("global_init_data null pointer");
    let local_init_data = unsafe { local_init_data.cast::<TableFunctionLocal>().as_mut() }
        .vortex_expect("local_init_data null pointer");
    let data_chunk = unsafe { DataChunk::borrow_mut(output) };

    match scan(local_init_data, global_init_data, data_chunk) {
        Ok(()) => {
            // The data chunk is already filled by the function.
            // No need to do anything here.
        }
        Err(e) => unsafe {
            error_out.write(cpp::duckdb_vx_error_create(
                e.to_string().as_ptr().cast(),
                e.to_string().len(),
            ));
        },
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_pushdown_expression(
    expr: cpp::duckdb_vx_expr,
) -> bool {
    can_push_expression(unsafe { Expression::borrow(expr) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_cardinality(
    bind_data: *const c_void,
    node_stats_out: *mut cpp::duckdb_vx_node_statistics,
) {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let node_stats =
        unsafe { node_stats_out.as_mut() }.vortex_expect("node_stats_out null pointer");

    match cardinality(bind_data) {
        Cardinality::Unknown => {}
        Cardinality::Exact(c) => {
            node_stats.has_estimated_cardinality = true;
            node_stats.estimated_cardinality = c as _;
            node_stats.has_max_cardinality = true;
            node_stats.max_cardinality = c as _;
        }
        Cardinality::Estimate(c) => {
            node_stats.has_estimated_cardinality = true;
            node_stats.estimated_cardinality = c as _;
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_init_global(
    init_input: *const cpp::duckdb_vx_tfunc_init_input,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let init_input = TableInitInput::new(
        unsafe { init_input.as_ref() }.vortex_expect("init_input null pointer"),
    );

    match init_global(&init_input) {
        Ok(init_data) => Data::from(Box::new(init_data)).as_ptr(),
        Err(e) => {
            // Set the error in the error output.
            let msg = e.to_string();
            unsafe { error_out.write(cpp::duckdb_vx_error_create(msg.as_ptr().cast(), msg.len())) };
            ptr::null_mut::<cpp::duckdb_vx_data_>().cast()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_init_local(
    bind_data: *const c_void,
    global_init_data: *mut c_void,
) -> cpp::duckdb_vx_data {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let global_init_data = unsafe { global_init_data.cast::<TableFunctionGlobal>().as_ref() }
        .vortex_expect("global_init_data null pointer");

    let init_data = init_local(bind_data, global_init_data);
    Data::from(Box::new(init_data)).as_ptr()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_bind(
    bind_input: cpp::duckdb_vx_tfunc_bind_input,
    bind_result: cpp::duckdb_vx_tfunc_bind_result,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let bind_input = unsafe { BindInput::own(bind_input) };
    let mut bind_result = unsafe { BindResult::own(bind_result) };

    try_or_null(error_out, || {
        let bind_data = bind(&bind_input, &mut bind_result)?;
        Ok(Data::from(Box::new(bind_data)).as_ptr())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_bind_data_clone(
    bind_data: *const c_void,
) -> cpp::duckdb_vx_data {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let copied_data = bind_data.clone();
    Data::from(Box::new(copied_data)).as_ptr()
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_distributed_bind_serialize(
    bind_data: *const c_void,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let bind_data = unsafe { bind_data.cast::<TableFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    try_or_null(error_out, || {
        let portable = serialize_bind(bind_data)?;
        Ok(Data::from(Box::new(portable)).as_ptr())
    })
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_distributed_bind_bytes(
    portable_bind: *const c_void,
    size_out: *mut usize,
) -> *const u8 {
    let portable_bind = unsafe { portable_bind.cast::<PortableDistributedBind>().as_ref() }
        .vortex_expect("portable_bind null pointer");
    unsafe { size_out.write(portable_bind.encoded.len()) };
    portable_bind.encoded.as_ptr()
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_distributed_file_count(
    portable_bind: *const c_void,
) -> usize {
    let portable_bind = unsafe { portable_bind.cast::<PortableDistributedBind>().as_ref() }
        .vortex_expect("portable_bind null pointer");
    portable_bind.files.len()
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_distributed_is_aggregate(
    portable_bind: *const c_void,
) -> bool {
    let portable_bind = unsafe { portable_bind.cast::<PortableDistributedBind>().as_ref() }
        .vortex_expect("portable_bind null pointer");
    portable_bind.aggregate_scan
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_distributed_file_at(
    portable_bind: *const c_void,
    index: usize,
    file_out: *mut VortexDistributedFileView,
) -> bool {
    let portable_bind = unsafe { portable_bind.cast::<PortableDistributedBind>().as_ref() }
        .vortex_expect("portable_bind null pointer");
    let Some(file) = portable_bind.files.get(index) else {
        return false;
    };
    unsafe {
        file_out.write(VortexDistributedFileView {
            source_url: file.source_url.as_ptr(),
            source_url_len: file.source_url.len(),
            path: file.path.as_ptr(),
            path_len: file.path.len(),
            size: file.size.unwrap_or_default(),
            has_size: file.size.is_some(),
        })
    };
    true
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_distributed_file_is_selected(
    filters: cpp::duckdb_vx_table_filter_set,
    column_ids: *const u64,
    column_ids_count: usize,
    file_index: u64,
    error_out: *mut cpp::duckdb_vx_error,
) -> bool {
    try_or(error_out, || {
        if column_ids.is_null() && column_ids_count != 0 {
            return Err(vortex::error::vortex_err!(
                "Distributed Vortex column ids are null"
            ));
        }
        let column_ids = if column_ids_count == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(column_ids, column_ids_count) }
        };
        let filters = if filters.is_null() {
            None
        } else {
            Some(unsafe { TableFilterSet::borrow(filters) })
        };
        distributed_file_index_is_selected(filters, column_ids, file_index, ptr::null_mut())
    })
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_init_global_distributed(
    portable_bind: *const u8,
    portable_bind_size: usize,
    assigned_file_indexes: *const u64,
    assigned_file_count: usize,
    ignore_optional_filters: bool,
    init_input: *const cpp::duckdb_vx_tfunc_init_input,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    try_or_null(error_out, || {
        if portable_bind.is_null() && portable_bind_size != 0 {
            return Err(vortex::error::vortex_err!(
                "Distributed Vortex bind bytes are null"
            ));
        }
        if assigned_file_indexes.is_null() && assigned_file_count != 0 {
            return Err(vortex::error::vortex_err!(
                "Distributed Vortex file indexes are null"
            ));
        }
        let bytes = if portable_bind_size == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(portable_bind, portable_bind_size) }
        };
        let indexes = if assigned_file_count == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(assigned_file_indexes, assigned_file_count) }
        };
        let bind_data = deserialize_runtime_bind(bytes, indexes)?;
        let input = unsafe { init_input.as_ref() }.vortex_expect("init_input null pointer");
        let runtime_input = cpp::duckdb_vx_tfunc_init_input {
            bind_data: (&raw const bind_data).cast(),
            column_ids: input.column_ids,
            column_ids_count: input.column_ids_count,
            projection_ids: input.projection_ids,
            projection_ids_count: input.projection_ids_count,
            filters: input.filters,
            client_context: input.client_context,
        };
        let global_data = init_global(&TableInitInput::new_distributed(
            &runtime_input,
            ignore_optional_filters,
        ))?;
        Ok(Data::from(Box::new(DistributedRuntimeGlobal {
            bind_data,
            global_data,
        }))
        .as_ptr())
    })
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_distributed_bind_data(
    global_data: *mut c_void,
) -> *const c_void {
    let global_data = unsafe { global_data.cast::<DistributedRuntimeGlobal>().as_ref() }
        .vortex_expect("distributed global data null pointer");
    (&raw const global_data.bind_data).cast()
}

#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_table_function_distributed_global_data(
    global_data: *mut c_void,
) -> *mut c_void {
    let global_data = unsafe { global_data.cast::<DistributedRuntimeGlobal>().as_mut() }
        .vortex_expect("distributed global data null pointer");
    (&raw mut global_data.global_data).cast()
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_copy_to_bind(
    column_names: *const *const c_char,
    column_name_count: usize,
    column_types: *const cpp::duckdb_logical_type,
    column_type_count: usize,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let column_names: Vec<String> =
        unsafe { std::slice::from_raw_parts(column_names, column_name_count.as_()) }
            .iter()
            .map(|name| {
                unsafe { CStr::from_ptr(name.cast()) }
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();

    let column_types: Vec<&LogicalTypeRef> =
        unsafe { std::slice::from_raw_parts(column_types, column_type_count.as_()) }
            .iter()
            .map(|c| unsafe { LogicalType::borrow(*c) })
            .collect();

    try_or_null(error_out, || {
        let bind_data = copy_to_bind(&column_names, &column_types)?;
        Ok(Data::from(Box::new(bind_data)).as_ptr())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_copy_to_initialize_global(
    bind_data: *const c_void,
    file_path: *const c_char,
    error_out: *mut cpp::duckdb_vx_error,
) -> cpp::duckdb_vx_data {
    let file_path = unsafe { CStr::from_ptr(file_path) }
        .to_string_lossy()
        .into_owned();
    let bind_data = unsafe { bind_data.cast::<CopyFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    try_or_null(error_out, || {
        let bind_data = copy_to_initialize_global(bind_data, file_path)?;
        Ok(Data::from(Box::new(bind_data)).as_ptr())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_copy_to_sink(
    bind_data: *const c_void,
    global_data: *mut c_void,
    data_chunk: cpp::duckdb_data_chunk,
    error_out: *mut cpp::duckdb_vx_error,
) {
    let bind_data = unsafe { bind_data.cast::<CopyFunctionBind>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let global_data = unsafe { global_data.cast::<CopyFunctionGlobal>().as_ref() }
        .vortex_expect("bind_data null pointer");
    let data_chunk = unsafe { DataChunk::borrow_mut(data_chunk) };
    try_or(error_out, || {
        copy_to_sink(bind_data, global_data, data_chunk)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn duckdb_copy_function_copy_to_finalize(
    global_data: *mut c_void,
    error_out: *mut cpp::duckdb_vx_error,
) {
    let global_data = unsafe { global_data.cast::<CopyFunctionGlobal>().as_mut() }
        .vortex_expect("bind_data null pointer");
    try_or(error_out, || copy_to_finalize(global_data))
}

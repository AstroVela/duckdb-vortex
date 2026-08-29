// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::unwrap_used)]
// exit(1) + cargo:error= doesn't provide a double-traceback like panic!()
#![expect(clippy::exit)]

use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::exit;

use bindgen::Abi;
use bindgen::callbacks::ParseCallbacks;

const SOURCE_FILES: [&str; 11] = [
    "cpp/vortex_duckdb.cpp",
    "cpp/copy_function.cpp",
    "cpp/expr.cpp",
    "cpp/optimizer.cpp",
    "cpp/scalar_fn_pushdown.cpp",
    "cpp/spatial_overrides.cpp",
    "cpp/cast_pushdown.cpp",
    "cpp/aggregate_fn_pushdown.cpp",
    "cpp/table_filter.cpp",
    "cpp/table_function.cpp",
    "cpp/vector.cpp",
];

// Duckdb C API function we use.
// This lowers codegen'd src/cpp.rs by four times.
const DUCKDB_C_API_FUNCTIONS: [&str; 134] = [
    "duckdb_array_type_array_size",
    "duckdb_array_type_child_type",
    "duckdb_array_vector_get_child",
    "duckdb_client_context_try_get_current_setting",
    "duckdb_close",
    "duckdb_column_count",
    "duckdb_column_logical_type",
    "duckdb_column_name",
    "duckdb_column_type",
    "duckdb_config_count",
    "duckdb_connect",
    "duckdb_create_array_type",
    "duckdb_create_blob",
    "duckdb_create_bool",
    "duckdb_create_config",
    "duckdb_create_data_chunk",
    "duckdb_create_date",
    "duckdb_create_decimal",
    "duckdb_create_decimal_type",
    "duckdb_create_double",
    "duckdb_create_float",
    "duckdb_create_hugeint",
    "duckdb_create_int16",
    "duckdb_create_int32",
    "duckdb_create_int64",
    "duckdb_create_int8",
    "duckdb_create_list_type",
    "duckdb_create_logical_type",
    "duckdb_create_map_type",
    "duckdb_create_null_value",
    "duckdb_create_selection_vector",
    "duckdb_create_struct_type",
    "duckdb_create_time",
    "duckdb_create_timestamp",
    "duckdb_create_timestamp_ms",
    "duckdb_create_timestamp_ns",
    "duckdb_create_timestamp_s",
    "duckdb_create_timestamp_tz",
    "duckdb_create_uint16",
    "duckdb_create_uint32",
    "duckdb_create_uint64",
    "duckdb_create_uint8",
    "duckdb_create_union_type",
    "duckdb_create_varchar_length",
    "duckdb_create_vector",
    "duckdb_data_chunk_get_column_count",
    "duckdb_data_chunk_get_size",
    "duckdb_data_chunk_get_vector",
    "duckdb_data_chunk_reset",
    "duckdb_data_chunk_set_size",
    "duckdb_data_chunk_to_string",
    "duckdb_data_chunk_verify",
    "duckdb_decimal_scale",
    "duckdb_decimal_width",
    "duckdb_destroy_client_context",
    "duckdb_destroy_config",
    "duckdb_destroy_data_chunk",
    "duckdb_destroy_logical_type",
    "duckdb_destroy_result",
    "duckdb_destroy_selection_vector",
    "duckdb_destroy_value",
    "duckdb_destroy_vector",
    "duckdb_disconnect",
    "duckdb_fetch_chunk",
    "duckdb_free",
    "duckdb_geometry_type_get_crs",
    "duckdb_get_blob",
    "duckdb_get_bool",
    "duckdb_get_config_flag",
    "duckdb_get_date",
    "duckdb_get_decimal",
    "duckdb_get_double",
    "duckdb_get_float",
    "duckdb_get_hugeint",
    "duckdb_get_int16",
    "duckdb_get_int32",
    "duckdb_get_int64",
    "duckdb_get_int8",
    "duckdb_get_list_child",
    "duckdb_get_list_size",
    "duckdb_get_struct_child",
    "duckdb_get_time",
    "duckdb_get_time_ns",
    "duckdb_get_timestamp",
    "duckdb_get_timestamp_ms",
    "duckdb_get_timestamp_ns",
    "duckdb_get_timestamp_s",
    "duckdb_get_timestamp_tz",
    "duckdb_get_type_id",
    "duckdb_get_uhugeint",
    "duckdb_get_uint16",
    "duckdb_get_uint32",
    "duckdb_get_uint64",
    "duckdb_get_uint8",
    "duckdb_get_value_type",
    "duckdb_get_varchar",
    "duckdb_is_null_value",
    "duckdb_library_version",
    "duckdb_list_type_child_type",
    "duckdb_list_vector_get_child",
    "duckdb_list_vector_get_size",
    "duckdb_list_vector_reserve",
    "duckdb_list_vector_set_size",
    "duckdb_malloc",
    "duckdb_map_type_key_type",
    "duckdb_map_type_value_type",
    "duckdb_open",
    "duckdb_open_ext",
    "duckdb_query",
    "duckdb_result_error",
    "duckdb_row_count",
    "duckdb_rows_changed",
    "duckdb_selection_vector_get_data_ptr",
    "duckdb_set_config",
    "duckdb_string_t_data",
    "duckdb_string_t_length",
    "duckdb_struct_type_child_count",
    "duckdb_struct_type_child_name",
    "duckdb_struct_type_child_type",
    "duckdb_struct_vector_get_child",
    "duckdb_union_type_member_count",
    "duckdb_union_type_member_name",
    "duckdb_union_type_member_type",
    "duckdb_value_to_string",
    "duckdb_vector_assign_string_element",
    "duckdb_vector_assign_string_element_len",
    "duckdb_vector_ensure_validity_writable",
    "duckdb_vector_flatten",
    "duckdb_vector_get_column_type",
    "duckdb_vector_get_data",
    "duckdb_vector_get_validity",
    "duckdb_vector_reference_value",
    "duckdb_vector_reference_vector",
    "duckdb_vector_size",
];

const DUCKDB_C_API_HEADERS: [&str; 7] = [
    "cpp/include/vortex_duckdb.h",
    "cpp/include/expr.h",
    "cpp/include/spatial_overrides.h",
    "cpp/include/table_filter.h",
    "cpp/include/vector.h",
    "cpp/include/copy_function.h",
    "cpp/include/table_function.h",
];

#[derive(Debug)]
struct BindgenCargoCallbacks;

impl ParseCallbacks for BindgenCargoCallbacks {
    fn read_env_var(&self, key: &str) {
        println!("cargo:rerun-if-env-changed={key}");
    }

    fn header_file(&self, filename: &str) {
        println!("cargo:rerun-if-changed={filename}");
    }

    fn include_file(&self, _filename: &str) {
        // We do not want to let bindgen add DuckDB headers from OUT_DIR to Cargo's fingerprint.
        // Those files are extracted during this build script, so their mtimes are newer than
        // Cargo's build-script output timestamp and would force one extra
        // rebuild after a clean build.
    }
}

fn distributed_scan_enabled(duckdb_include_dir: &Path) -> bool {
    assert!(
        env::var_os("CARGO_FEATURE_VANE").is_some(),
        "the vendored vortex-duckdb crate is Vane-only"
    );
    let distributed_header =
        duckdb_include_dir.join("duckdb/function/distributed_table_function.hpp");
    assert!(
        distributed_header.is_file(),
        "the `vane` feature requires {}",
        distributed_header.display()
    );
    true
}

/// Generate rust functions with bindgen from C sources.
fn bindgen_c2rust(crate_dir: &Path, duckdb_include_dir: &Path) {
    let mut builder = bindgen::Builder::default()
        .headers(DUCKDB_C_API_HEADERS)
        .override_abi(Abi::CUnwind, ".*")
        .raw_line("#![allow(dead_code)]")
        .raw_line("#![allow(non_camel_case_types)]")
        .raw_line("#![allow(non_upper_case_globals)]")
        .raw_line("#![allow(non_snake_case)]")
        .raw_line("#![allow(clippy::absolute_paths)]")
        .raw_line("#![allow(clippy::suspicious_doc_comments)]")
        .raw_line("#![allow(clippy::enum_variant_names)]")
        .allowlist_function("duckdb_vx_.*")
        .allowlist_type("duckdb_vx_.*")
        .allowlist_type("DUCKDB_VX_.*")
        .allowlist_var("DUCKDB_VX_.*")
        // Two types read from raw vector data
        .allowlist_type("duckdb_list_entry")
        .allowlist_type("duckdb_column_statistics")
        // Add the #[must_use] attribute to FFI functions that return results.
        .must_use_type("duckdb_state")
        .rustified_enum("duckdb_state")
        .rustified_enum("DUCKDB_VX_EXPR_CLASS")
        .rustified_enum("DUCKDB_VX_EXPR_TYPE")
        .rustified_enum("DUCKDB_VX_TABLE_FILTER_TYPE")
        .rustified_enum("DUCKDB_VX_TABLE_FILTER_MATCH")
        .rustified_non_exhaustive_enum("DUCKDB_TYPE")
        .size_t_is_usize(true)
        .clang_arg(format!("-I{}", duckdb_include_dir.display()))
        .clang_arg(format!("-I{}", crate_dir.join("cpp/include").display()))
        .generate_comments(true)
        .parse_callbacks(Box::new(BindgenCargoCallbacks));

    if distributed_scan_enabled(duckdb_include_dir) {
        builder = builder.clang_arg("-DVORTEX_VANE_DISTRIBUTED=1");
    }

    // Some minimal build images provide libclang without Clang's resource
    // headers. In that setup bindgen cannot even parse DuckDB's public C
    // header because <stdbool.h> is missing. Ask the selected C compiler for
    // its builtin include directory instead of baking in a host-specific GCC
    // path.
    let compiler = cc::Build::new().get_compiler();
    if let Ok(output) = Command::new(compiler.path())
        .arg("-print-file-name=include")
        .output()
    {
        if output.status.success() {
            let builtin_include = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
            if builtin_include.is_dir() {
                builder = builder
                    .clang_arg("-isystem")
                    .clang_arg(builtin_include.to_string_lossy());
            }
        }
    }

    for function in DUCKDB_C_API_FUNCTIONS {
        builder = builder.allowlist_function(function);
    }

    let bindings = builder.generate();

    let bindings = match bindings {
        Ok(b) => b,
        Err(e) => {
            println!("cargo:error=Failed to generate Rust bindings: {e}");
            exit(1);
        }
    };
    let out_path = crate_dir.join("src/cpp.rs");
    if let Err(e) = fs::write(&out_path, bindings.to_string()) {
        println!("cargo:error=Failed to write Rust bindings: {e}");
        exit(1);
    }
}

/// Generate libvortex_duckdb.*
fn compile_cpp(duckdb_include_dir: &Path) {
    let mut build = cc::Build::new();
    build
        .std("c++20")
        .flags(["-Wall", "-Wextra", "-Wpedantic", "-Werror"])
        .cpp(true)
        // Duckdb 1.5.5 uses C++11. spatial_overrides.o uses
        // duckdb::ScalarFunctionCatalogEntry::Name which is constexpr but not
        // inline. Our code uses C++20 where constexpr implies inline. GCC
        // emits this symbol with STB_GNU_UNIQUE and this conflicts on link stage
        // in duckdb-vortex where libvortex_duckdb.a is linked statically
        .flag_if_supported("-fno-gnu-unique")
        // We don't want compiler warnings inside duckdb headers, pass as flags
        .flag("-isystem")
        .flag(duckdb_include_dir)
        .include("include")
        .include("cpp/include")
        .files(SOURCE_FILES);
    if distributed_scan_enabled(duckdb_include_dir) {
        build.define("VORTEX_VANE_DISTRIBUTED", "1");
    }
    build.compile("vortex-duckdb-extras");
    for e in SOURCE_FILES {
        println!("cargo:rerun-if-changed={e}");
    }
}

/// Generate include/vortex.h from rust sources
fn cbindgen_rust2c(crate_dir: &Path) {
    let header = crate_dir.join("include/vortex.h");
    let output = cbindgen::Builder::new()
        .with_config(cbindgen::Config::from_file(crate_dir.join("cbindgen.toml")).unwrap())
        .with_crate(crate_dir)
        .with_no_includes()
        .generate();
    match output {
        Ok(bindings) => bindings.write_to_file(&header),
        Err(e) => {
            println!("cargo:error=Failed to generate cbindgen bindings for vortex.h: {e}");
            exit(1);
        }
    };

    let mut cmd = Command::new("clang-format");
    let format = cmd.arg("-i").arg("--style=file").arg(&header);
    if let Ok(status) = format.status() {
        if !status.success() {
            println!("cargo:warning=clang-format exited with status {status}");
        }
    } else {
        println!("cargo:warning=clang-format not found, skipping formatting of generated header");
    }
}

fn main() {
    println!("cargo:rerun-if-changed=cpp/include");
    println!("cargo:rustc-check-cfg=cfg(duckdb_release)");
    println!("cargo:rerun-if-env-changed=DUCKDB_SOURCE_DIR");
    println!("cargo:rerun-if-env-changed=DUCKDB_VERSION");
    assert!(
        env::var_os("CARGO_FEATURE_VANE").is_some(),
        "the vendored vortex-duckdb crate is Vane-only and requires the `vane` feature"
    );

    let source_dir = PathBuf::from(
        env::var_os("DUCKDB_SOURCE_DIR")
            .expect("the Vane-only vortex-duckdb crate requires DUCKDB_SOURCE_DIR"),
    );
    let duckdb_version = env::var("DUCKDB_VERSION")
        .expect("the Vane-only vortex-duckdb crate requires DUCKDB_VERSION");
    assert!(
        !duckdb_version.is_empty(),
        "the Vane-only vortex-duckdb crate requires a non-empty DUCKDB_VERSION"
    );

    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let duckdb_include_dir = source_dir.join("src").join("include");
    println!(
        "cargo:info=Using DuckDB {duckdb_version} source from DUCKDB_SOURCE_DIR={}",
        source_dir.display()
    );
    bindgen_c2rust(&crate_dir, &duckdb_include_dir);
    cbindgen_rust2c(&crate_dir);
    compile_cpp(&duckdb_include_dir);
}

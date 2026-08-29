// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::missing_safety_doc)]

#[cfg(not(feature = "vane"))]
compile_error!("vortex-extension-vane must be built with the `vane` feature");

use std::ffi::c_char;

/// Initialize process-wide Vortex runtime support for the Vane-specific C++
/// loader path. This crate intentionally has no legacy DuckDB registration
/// entry point; the ordinary build uses the separate native manifest.
#[cfg(feature = "vane")]
#[unsafe(no_mangle)]
pub extern "C" fn vortex_init_vane_rust() {
    vortex_duckdb::initialize_runtime();
}

/// An additional function we export to expose the version of the extension itself to C++ code.
#[unsafe(no_mangle)]
pub extern "C" fn vortex_extension_version_rust() -> *const c_char {
    vortex_duckdb::extension_version()
}

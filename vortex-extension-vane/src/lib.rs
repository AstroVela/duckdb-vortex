// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![expect(clippy::missing_safety_doc)]

#[cfg(not(vortex_vane_distributed))]
compile_error!("vortex-extension-vane requires VORTEX_VANE_DISTRIBUTED=1 at compile time");

#[cfg(vortex_vane_distributed)]
use std::ffi::c_char;
#[cfg(vortex_vane_distributed)]
use std::ffi::c_void;

/// Initialize runtime support and register Vortex through the Vane-specific
/// C++ loader path. This crate intentionally has no legacy DuckDB registration
/// entry point; the ordinary build uses the separate native manifest.
///
/// # Safety
///
/// `loader` must point to a live `duckdb::ExtensionLoader` from the exact Vane
/// DuckDB source tree used to build this crate.
#[cfg(vortex_vane_distributed)]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn vortex_init_vane_rust(loader: *mut c_void) {
    unsafe { vortex_duckdb::initialize_vane(loader) };
}

/// An additional function we export to expose the version of the extension itself to C++ code.
#[cfg(vortex_vane_distributed)]
#[unsafe(no_mangle)]
pub extern "C" fn vortex_extension_version_rust() -> *const c_char {
    vortex_duckdb::extension_version()
}

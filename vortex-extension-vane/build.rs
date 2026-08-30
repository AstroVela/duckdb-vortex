// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=VORTEX_VANE_DISTRIBUTED");
    println!("cargo:rustc-check-cfg=cfg(vortex_vane_distributed)");

    match env::var("VORTEX_VANE_DISTRIBUTED") {
        Ok(value) if matches!(value.as_str(), "1" | "true") => {
            println!("cargo:rustc-cfg=vortex_vane_distributed");
        }
        Err(env::VarError::NotPresent) => {}
        Ok(value) => {
            panic!("VORTEX_VANE_DISTRIBUTED must be `1` or `true` when set, got `{value}`");
        }
        Err(env::VarError::NotUnicode(_)) => {
            panic!("VORTEX_VANE_DISTRIBUTED must contain valid Unicode");
        }
    }
}

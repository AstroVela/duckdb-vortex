// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use vortex_index_spfresh::SPFRESH_ID;
use vortex_index_spfresh::SpFreshBuildLimits;
use vortex_index_spfresh::SpFreshIndexBuilder;
use vortex_index_spfresh::SpFreshLimits;
use vortex_index_spfresh::SpFreshProvider;

pub(crate) fn register() {
    vortex_duckdb::index::register_index_provider_factory(SPFRESH_ID, |session, scratch| {
        let builder = SpFreshIndexBuilder::try_new(
            scratch.to_path_buf(),
            session,
            SpFreshBuildLimits::default(),
        )?;
        let provider = SpFreshProvider::try_new(scratch.to_path_buf(), SpFreshLimits::default())?
            .with_builder(builder);
        Ok(Arc::new(provider))
    })
    .expect("Failed to register the static SPFresh SQL provider");
}

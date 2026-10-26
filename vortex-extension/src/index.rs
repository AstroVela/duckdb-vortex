// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

#[cfg(feature = "index-hnswlib")]
use vortex_index_hnswlib::{
    HNSWLIB_ID, HnswBuildLimits, HnswIndexBuilder, HnswLimits, HnswProvider,
};
#[cfg(feature = "index-spfresh")]
use vortex_index_spfresh::{
    SPFRESH_ID, SpFreshBuildLimits, SpFreshIndexBuilder, SpFreshLimits, SpFreshProvider,
};

pub(crate) fn register() {
    #[cfg(feature = "index-spfresh")]
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

    #[cfg(feature = "index-hnswlib")]
    vortex_duckdb::index::register_index_provider_factory(HNSWLIB_ID, |session, scratch| {
        let builder =
            HnswIndexBuilder::try_new(scratch.to_path_buf(), session, HnswBuildLimits::default())?;
        let provider = HnswProvider::try_new(scratch.to_path_buf(), HnswLimits::default())?
            .with_builder(builder);
        Ok(Arc::new(provider))
    })
    .expect("Failed to register the static hnswlib SQL provider");
}

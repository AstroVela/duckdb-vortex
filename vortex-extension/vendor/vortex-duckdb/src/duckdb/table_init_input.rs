// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;

use crate::cpp;
use crate::duckdb::TableFilterSet;
use crate::duckdb::TableFilterSetRef;

pub struct TableInitInput<'a> {
    pub input: &'a cpp::duckdb_vx_tfunc_init_input,
    #[cfg(feature = "vane")]
    ignore_optional_filters: bool,
}

impl Debug for TableInitInput<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("TableInitInput")
            .field("column_ids", &self.column_ids())
            .field("projection_ids", &self.projection_ids())
            .field("table_filter_set", &self.table_filter_set())
            .finish()
    }
}

impl<'a> TableInitInput<'a> {
    pub fn new(input: &'a cpp::duckdb_vx_tfunc_init_input) -> Self {
        Self {
            input,
            #[cfg(feature = "vane")]
            ignore_optional_filters: false,
        }
    }

    #[cfg(feature = "vane")]
    pub fn new_distributed(
        input: &'a cpp::duckdb_vx_tfunc_init_input,
        ignore_optional_filters: bool,
    ) -> Self {
        Self {
            input,
            ignore_optional_filters,
        }
    }

    pub fn column_ids(&self) -> &[u64] {
        // `std::vector::data()` may be null for an empty vector, while Rust's
        // `from_raw_parts` requires a non-null pointer even when the length is
        // zero. Count-star and fully pruned scans can legitimately have no
        // projected input columns.
        if self.input.column_ids_count == 0 {
            return &[];
        }
        assert!(!self.input.column_ids.is_null());
        unsafe { std::slice::from_raw_parts(self.input.column_ids, self.input.column_ids_count) }
    }

    pub fn projection_ids(&self) -> Option<&[u64]> {
        // Passed pointer is std::vector's .data(). However, C++ doesn't
        // guarantee an empty vector's pointer is nullptr so we need to check
        // both conditions
        if self.input.projection_ids.is_null() || self.input.projection_ids_count == 0 {
            return None;
        }
        Some(unsafe {
            std::slice::from_raw_parts(self.input.projection_ids, self.input.projection_ids_count)
        })
    }

    /// Returns the table filter set for the table function.
    pub fn table_filter_set(&self) -> Option<&TableFilterSetRef> {
        let ptr = self.input.filters;
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { TableFilterSet::borrow(ptr) })
        }
    }

    #[cfg(feature = "vane")]
    pub fn ignore_optional_filters(&self) -> bool {
        self.ignore_optional_filters
    }
}

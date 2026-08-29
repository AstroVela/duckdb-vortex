#define DUCKDB_EXTENSION_MAIN

#include "vortex_extension.hpp"
#include "vortex_rust.hpp"

#ifdef VORTEX_VANE_DISTRIBUTED
#include "duckdb/main/extension/extension_loader.hpp"
#endif

using namespace duckdb;

#ifdef VORTEX_VANE_DISTRIBUTED
static void LoadVane(ExtensionLoader &loader) {
	vortex_init_vane_rust();
	vortex_vane_init(loader);
}
#endif

extern "C" {
#ifndef VORTEX_VANE_DISTRIBUTED
DUCKDB_EXTENSION_API void vortex_init(duckdb::DatabaseInstance &db) {
	vortex_init_rust(reinterpret_cast<duckdb_database>(&db));
}

DUCKDB_EXTENSION_API void vortex_duckdb_cpp_init(duckdb::DatabaseInstance &db) {
	vortex_init_rust(reinterpret_cast<duckdb_database>(&db));
}
#else
#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
DUCKDB_CPP_EXTENSION_ENTRY(vortex, loader) {
	LoadVane(loader);
}
#endif
#endif

DUCKDB_EXTENSION_API const char *vortex_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef VORTEX_VANE_DISTRIBUTED
static void LoadInternal(DatabaseInstance &db_instance) {
	vortex_init_rust(reinterpret_cast<duckdb_database>(&db_instance));
}
#endif

/// Called when the extension is loaded by DuckDB.
/// It is responsible for registering functions and initializing state.
///
/// Specifically, the `read_vortex` table function enables reading data from
/// Vortex files in SQL queries.
void VortexExtension::Load(duckdb::ExtensionLoader &loader) {
#ifdef VORTEX_VANE_DISTRIBUTED
	LoadVane(loader);
#else
	LoadInternal(loader.GetDatabaseInstance());
#endif
}

/// Returns the name of the Vortex extension.
///
/// It is used by DuckDB to identify the extension.
///
/// Example:
/// ```
/// LOAD vortex;
/// ```
std::string VortexExtension::Name() {
	return "vortex";
}

//! Returns the version of the Vortex extension.
std::string VortexExtension::Version() const {
	return vortex_extension_version_rust();
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

#pragma once

extern "C" {

void vortex_init_rust(void *db);
const char *vortex_version_rust();
const char *vortex_extension_version_rust();
}

#ifdef VORTEX_VANE_DISTRIBUTED
extern "C" void vortex_init_vane_rust();

namespace duckdb {
class ExtensionLoader;
}

void vortex_vane_init(duckdb::ExtensionLoader &loader);
#endif

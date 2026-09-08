# Vane Vortex provider release

This repository owns the Vortex native build adapter and its top-level TestPyPI
publisher. Shared source, release-matrix, immutable-retry and index validation
comes from the committed `vane-extension-ci-tools` submodule; initialize it with:

```sh
git submodule update --init --recursive
```

## Fixed candidate

- Vane: `472df75ab51fd3eac2642f6646545075549e5921`
  (`vane-ai==0.2.0.dev612`).
- DuckDB SourceID: `edb8047859d9d5c443f46e86e6b5e507b5026944`.
- Vortex Rust fork: `8eedee91dcf630551ab6b5d8705fad3d853a7c33`.
- Rust: `1.97.1`, with the committed Vane adapter Cargo.lock.
- Extension vcpkg: `74e6536215718009aae747d86d84b78376bf9e09`.
- Shared Vane CI tools: `671717e4816c21e65aa32a32dd0def8b030baa44`.
- Provider: `vane-extension-vortex`, CPython 3.10–3.14,
  `manylinux_2_28_x86_64`, with no other provider dependencies.

The runtime requirement is exact. The provider uses Vane's descriptor-derived
package version, not the repository's native extension version or a new Vane
release. No tag or republishing of vane-ai is needed for this candidate.

## Build and qualification

`.github/workflows/VaneIntegration.yml` retains ordinary DuckDB isolation and
Vane native/static-wheel, local, two-node Ray, and ASAN/LSAN lifecycle coverage.
Ordinary DuckDB inputs and the native Rust adapter stay unchanged.

`.github/workflows/VaneExtension.yml` adds independent dynamic-provider CI.
The adapter in `scripts/build_vane_dynamic_wheel.py` selects
`VANE_LOADABLE_EXTENSIONS=vortex` and static Rust/DuckDB linkage: the base Vane
wheel does not link Vortex. Only `vortex_duckdb_cpp_init` is exported; the ELF is
stripped before its metadata/signature footer is appended. The build rejects
unresolved DuckDB APIs, non-platform runtime libraries and RPATH/RUNPATH.

License collection includes this repository's MIT license, Vane's license and
notice, the matching DuckDB engine's third-party notices, cargo-about's locked
Vane-adapter dependency closure, and the pinned Rust standard-library notices.
cargo-about 0.9.2 is downloaded with an exact SHA256. Failure to collect a
complete accepted license set fails the build.

The existing Vane wheel builder/verifier owns descriptor generation, exact
runtime dependencies, artifact/SourceID/platform/trust verification and wheel
qualification. There is no extension download at query time, compatibility
selection, unsigned runtime switch or fallback path.

Local SQL tests load the installed provider by name with automatic installation
and loading disabled, then exercise scans, filters, aggregates and COPY. The Ray
test owns a head with no execution CPUs and two one-CPU worker nodes, checks the
prepared dynamic manifest, proves Vortex fragment execution on both persistent
workers, and verifies distributed COPY and empty COPY readback. Both CI and
post-upload tests use installed wheels and isolated Python processes.

## First TestPyPI publication (after merge)

The code PR does **not** configure credentials or publish a package.

1. Create the protected GitHub environment `testpypi` in
   `AstroVela/duckdb-vortex`, allow only the branch
   `v1.5-variegata_vane`, and require an authorized human reviewer.
2. Configure the environment secret
   `VANE_TESTPYPI_EXTENSION_SIGNING_PRIVATE_KEY` with the existing private key
   for trust identity `astrovela/vane-testpypi`, matching dev612. Do not commit,
   print or rotate this key as part of provider publication.
3. In TestPyPI, configure the Trusted Publisher:
   project `vane-extension-vortex`, owner `AstroVela`,
   repository `duckdb-vortex`, workflow `VaneExtension.yml`,
   environment `testpypi`.
4. Dispatch the merged workflow and approve its environment gates:

   ```sh
   gh workflow run VaneExtension.yml --repo AstroVela/duckdb-vortex \
     --ref v1.5-variegata_vane -f operation=testpypi-dev
   ```

The workflow downloads all five exact indexed dev612 runtime wheels, builds and
signs one native artifact, qualifies the complete provider matrix, revalidates
the immutable release set, and produces checksums, SBOM, provenance and Sigstore
evidence. OIDC upload stays in the repository's top-level workflow.

The shared validator checks existing filenames and bytes before
`skip-existing` can be used. After upload it compares all indexed SHA256 hashes.
Local and two-worker Ray jobs download the exact selected versions, compare the
downloaded provider to the assembled artifact, run `pip check`, and execute the
provider tests. A successful upload alone is not a successful qualification.

The configured TestPyPI limit is 100,000,000 bytes **per wheel**, independent of
Vane's larger native artifact safety limits. The first CI build must establish
that the actual compressed Vortex wheel fits; this PR does not assume a measured
wheel size or silently relax either limit.

## Focused developer checks

After static review, use Python 3.11+ (the release tooling interpreter is
independent of the provider's CPython 3.10–3.14 wheel tags):

```sh
python -m pip install -r vane-extension-ci-tools/requirements-release.txt
python -I test/vane/test_vane_dynamic_wheel.py
python -I test/vane/test_vane_provider_release.py
```

No editable install or full local Vane test suite is needed. Native builds and
installed local/two-worker integration run in the PR workflows.

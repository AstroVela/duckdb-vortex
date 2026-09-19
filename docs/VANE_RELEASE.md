# Vane Vortex provider release

This repository owns the Vortex native build adapter and its top-level TestPyPI
and PyPI publishers. Shared source, release-matrix, immutable-retry and index validation
comes from the committed `vane-extension-ci-tools` submodule; initialize it with:

```sh
git submodule update --init --recursive
```

## Fixed Vane baseline

- Vane v0.2.0: `79049f382ba6ee79d035c09cc8b5d3538e5bbe6a`
  (`vane-ai==0.2.0`).
- Full DuckDB source tree ID: `e24da547b83d10b75697b8a40acd684f0a0a8481`;
  native runtime SourceID: `e24da547b8`.
- Vortex Rust fork: `3da8a2848b5d10d028e69471c5c97bd3dc785a03`.
- Rust: `1.97.1`, with the committed Vane adapter Cargo.lock.
- Extension vcpkg: `74e6536215718009aae747d86d84b78376bf9e09`.
- Shared Vane CI tools: `d7316f29add0cc893c5fd126a971ffb78cc39c78`.
- Provider: `vane-extension-vortex`, CPython 3.10–3.14,
  `manylinux_2_28_x86_64`, with no other provider dependencies.

The runtime requirement is exact. The provider uses Vane's descriptor-derived
package version, not the repository's native extension version or a new Vane
release. Local and PR qualification builds the exact runtime from source.
Build-only CI enables the public CI test key in its locally built runtime and
packages a matching runtime/provider set. These are test artifacts even though
the runtime reports `0.2.0`; do not mix them with the PyPI runtime or publish them.

Both manifests pin the same Vane v0.2.0 commit. Production qualification uses
its exact PyPI runtime wheels and the production signer. Updating the pin does
not publish the provider or establish production qualification.

Use `build-only` for PRs and `release` for production. With this stable pin,
`testpypi-dev` deliberately rejects `0.2.0`; a future development publication
requires a separately reviewed pin to its exact TestPyPI runtime. Ordinary
TestPyPI dev runtimes trust the dedicated TestPyPI key, not the public CI key.

The preflight requires a protected manual dispatch from this repository's
`v1.5-variegata_vane` branch, derives the canonical runtime version from full
source history without version overrides, and checks that all five non-yanked
runtime wheels are indexed. Development candidates use TestPyPI only; production
candidates use PyPI only. Production may be alpha, beta, RC, final or post-release,
but not a development, local, epoch or noncanonical version.

## Build and qualification

`.github/workflows/VaneIntegration.yml` retains ordinary DuckDB isolation and
Vane native/static-wheel, default Ray smoke, two-node Ray, and ASAN/LSAN lifecycle coverage.
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

Default Ray SQL tests load the installed provider by name with automatic installation
and loading disabled, then exercise scans, filters, aggregates and COPY. The Ray
test owns a head with no execution CPUs and two one-CPU worker nodes, checks the
prepared dynamic manifest, proves Vortex fragment execution on both persistent
workers, and verifies distributed COPY and empty COPY readback. Both CI and
post-upload tests use installed wheels and isolated Python processes.
The dynamic tests receive the exact runtime version, fork version and full
DuckDB source tree ID from the selected source, so production does not reuse
hard-coded development-version expectations or skip native identity checks.

## Signing and publication boundaries

Both publishing operations use fresh jobs at each privilege boundary:

```text
read-only preflight
  -> native prepare (no secrets or OIDC)
  -> protected native signer (stdlib + OpenSSL only)
  -> package + native verification (no secrets or OIDC)
  -> complete matrix validation + checksums/SBOM/provenance
  -> minimal TestPyPI upload -> read-only index verification
  -> installed default Ray smoke AND two-worker Ray qualification
  -> release only: protected read-only promotion verification
  -> minimal PyPI upload -> read-only PyPI verification
```

The native preparation job emits only `artifacts/vortex.duckdb_extension` and
the six reviewed notices under `licenses/vortex/`. The signer reads its exact
official Vane source pin from the committed selected manifest, never from a
build-produced script or source reference. It installs no Python dependencies,
does not load the native artifact, and receives only the selected signing key.
Its wrapper runs with `/usr/bin/python3 -I -S`, checks the public-key DER SHA256,
unsets the secret before spawning children, and erases temporary key files outside
all downloaded/uploaded paths. It snapshots the unsigned payload before signing
and requires that only the final 256-byte DuckDB signature slot changes.

A fresh packaging job independently compares the signed payload with the original
unsigned prepare artifact, then repeats ELF checks and Vane's native wheel
verification for all five indexed runtimes. Signing is not a second native build.
The full in-process builder is restricted to the public `ci-test` fixture key;
published profiles cannot use it.

Every publishing artifact consumer uses the original producer's immutable
artifact ID and fails on an archive digest mismatch. The promotion verifier has
only `contents: read`; dependencies used for validation never share a job with
publishing OIDC. Each final uploader contains only pinned artifact-download and
PyPI-publish actions, with no checkout, dependency installation or custom script.

## Future development TestPyPI publication

This channel requires a separate development runtime pin; it cannot use the
current Vane v0.2.0 pin. The code PR does **not** configure credentials or publish
a package.

1. Create the protected GitHub environment `testpypi` in
   `AstroVela/duckdb-vortex`, allow only the branch
   `v1.5-variegata_vane`, and require an authorized human reviewer.
2. Configure the environment secret
   `VANE_TESTPYPI_EXTENSION_SIGNING_PRIVATE_KEY` with the existing private key
   for trust identity `astrovela/vane-testpypi`, matching the selected dev runtime. Do not commit,
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

The workflow checks all five exact indexed development runtime wheels, builds and
signs one native artifact in separate jobs, qualifies the complete provider matrix, revalidates
the immutable release set, and produces checksums, SBOM, provenance and Sigstore
evidence. OIDC upload stays in the repository's top-level workflow.

The shared validator checks existing filenames and bytes before
`skip-existing` can be used. After upload it compares all indexed SHA256 hashes.
Default Ray smoke and two-worker Ray jobs download the exact selected versions, compare the
downloaded provider to the assembled artifact, run `pip check`, and execute the
provider tests. A successful upload alone is not a successful qualification.

The configured upload limit is 100,000,000 bytes **per wheel**, independent of
Vane's larger native artifact safety limits. Both publishing indexes must accept
that budget; neither limit is silently relaxed.

## Production PyPI setup and promotion (after merge)

Environment and publisher setup is an administrator action, separate from this
code change. No private key, tag, package or ruleset is created by this PR.

1. Keep the existing `testpypi` environment and its Trusted Publisher for staging.
2. Create `production-signing`, restricted to `v1.5-variegata_vane` with required
   human approval. Store `VANE_EXTENSION_SIGNING_PRIVATE_KEY` **only there**. It
   must match trust identity `astrovela/vane` and public DER SHA256
   `8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb`.
   The separate development identity `astrovela/vane-testpypi` has public DER
   SHA256 `53779fb8f9c97e9dec9c66ff838839eb234d1a64d4b105671304820e627b5e32`.
3. Create `pypi`, restricted to the same branch with required approval, without
   a native signing secret. Register the production PyPI Trusted Publisher for
   project `vane-extension-vortex`, owner `AstroVela`, repository `duckdb-vortex`,
   workflow `VaneExtension.yml`, environment `pypi`.
4. Confirm the reviewed Vane v0.2.0 pin and its complete PyPI runtime matrix,
   then dispatch:

   ```sh
   gh workflow run VaneExtension.yml --repo AstroVela/duckdb-vortex \
     --ref v1.5-variegata_vane -f operation=release
   ```

The production artifact is newly built and signed with the production native key,
then staged on TestPyPI. It is never a renamed or re-signed development wheel. The
staging tests install `vane-ai` from PyPI and the provider from TestPyPI, compare
the provider bytes with the original candidate, and exercise both default Ray smoke and two-worker
execution. Only after both pass does the protected promotion verifier recheck the
complete TestPyPI matrix and reject any conflicting, extra or yanked PyPI files.
An absent version or byte-identical partial upload is allowed for a retry.

Approve both the read-only `pypi` verifier and the minimal `pypi` uploader when
GitHub requests them; they are separate jobs and may require separate approvals.
The uploader promotes the exact original wheel files, without rebuilding,
re-signing, changing dependencies or relabeling versions. Finally, indexed PyPI
filenames and SHA256 hashes must match. The workflow creates no provider tag;
provider versions remain generated by Vane's descriptor-bound wheel builder.

## Focused developer checks

After static review, use Python 3.11+ (the release tooling interpreter is
independent of the provider's CPython 3.10–3.14 wheel tags):

```sh
python -m pip install -r vane-extension-ci-tools/requirements-release.txt pytest pyyaml
python -I test/vane/test_vane_dynamic_wheel.py
python -I test/vane/test_vane_provider_release.py
python -I test/vane/test_vane_runtime_identity.py
python -I -m pytest -q test/vane/test_vane_dynamic_signing.py test/vane/test_vane_production_release.py
```

No editable install or full local Vane test suite is needed. Native builds and
installed default Ray smoke/two-worker integration run in the PR workflows.

## Default Ray qualification

Both manifests pin Vane v0.2.0,
`79049f382ba6ee79d035c09cc8b5d3538e5bbe6a`, including the schema-only chunk fix
tracked in [Vane #827](https://github.com/AstroVela/vane/issues/827).
All public integration entry points require `VANE_RUNNER` to be absent and
verify the default Ray runner without calling a runner-selection API. Fixture
writes also use Ray. COPY checks inspect committed file receipts, and fixture
files are copied from those selected outputs into a flat directory for stable
glob and `file_index` expectations. Expected rows are generated independently
in Python and compared against both SQL and Relation execution.

The native and Ray object-store jobs use the same MinIO entry point. It pulls
an upstream Quay release pinned by tag and digest, waits for successful health
responses with a finite timeout, and creates the bucket through signed S3
requests before exporting the test environment.

The dynamic script's `--smoke` option selects a smaller read/write test set;
it still uses the default Ray runner. The full linked suite retains worker,
actor-loss, retry, and object-store failure coverage.

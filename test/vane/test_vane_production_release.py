# SPDX-FileCopyrightText: 2026 Vane contributors
# SPDX-License-Identifier: Apache-2.0

"""Focused release gates and job-boundary regressions; never build or publish."""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def load_script(filename):
    spec = importlib.util.spec_from_file_location(filename, ROOT / "scripts" / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


preflight = load_script("vane_release_preflight.py")
package = load_script("package_vane_dynamic_wheel.py")
builder = package.builder
release = preflight.load_release_tools(ROOT)
WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/VaneExtension.yml").read_text())
JOBS = WORKFLOW["jobs"]
CONTEXT = {
    "GITHUB_EVENT_NAME": "workflow_dispatch",
    "GITHUB_REPOSITORY": "AstroVela/duckdb-vortex",
    "GITHUB_REF": "refs/heads/v1.5-variegata_vane",
    "GITHUB_REF_PROTECTED": "true",
}
INTERPRETERS = ("cp310", "cp311", "cp312", "cp313", "cp314")


def runs(job):
    return "\n".join(step.get("run", "") for step in job["steps"])


def downloads(job):
    return [
        step["with"]
        for step in job["steps"]
        if step.get("uses", "").startswith("actions/download-artifact@")
    ]


def test_production_manifest_preserves_every_other_source_contract():
    dev = tomllib.loads((ROOT / "vane-extension.toml").read_text())
    prod = tomllib.loads((ROOT / "vane-extension-release.toml").read_text())
    assert dev["schema_version"] == prod["schema_version"] == 2
    assert dev["vane"]["revision"] == "472df75ab51fd3eac2642f6646545075549e5921"
    assert prod["vane"]["revision"] == preflight.PRODUCTION_KEY_REVISION
    prod["vane"]["revision"] = dev["vane"]["revision"]
    assert prod == dev
    assert builder.EXPECTED_RUST_RELEASE == "1.97.1"
    assert (
        builder.EXPECTED_VORTEX_REVISION == "8eedee91dcf630551ab6b5d8705fad3d853a7c33"
    )
    assert builder.SIGNING_PROFILES["production"] == ("astrovela/vane", None)


@pytest.mark.parametrize("field", list(CONTEXT))
def test_publishing_requires_official_protected_manual_dispatch(field):
    preflight.require_publish_dispatch(CONTEXT)
    with pytest.raises(ValueError, match="protected"):
        preflight.require_publish_dispatch({**CONTEXT, field: "untrusted"})
    with pytest.raises(ValueError, match="protected"):
        preflight.require_publish_dispatch(
            {key: value for key, value in CONTEXT.items() if key != field}
        )


def indexed_wheels(version):
    return [
        {
            "filename": f"vane_ai-{version}-{interpreter}-{interpreter}-manylinux_2_28_x86_64.whl",
            "packagetype": "bdist_wheel",
            "yanked": False,
            "digests": {"sha256": "a" * 64},
        }
        for interpreter in INTERPRETERS
    ]


@pytest.mark.parametrize(
    "channel,version,index",
    [("release", "0.2.0rc1", "pypi"), ("testpypi-dev", "0.2.0.dev612", "testpypi")],
)
def test_preflight_requires_all_indexed_runtimes_from_one_explicit_index(
    channel, version, index, monkeypatch
):
    query = Mock(return_value=(200, {"urls": indexed_wheels(version)}))
    monkeypatch.setattr(release, "_request_json", query)
    preflight.require_indexed_runtime(
        release, version, channel, ROOT / "vane-provider-release.toml"
    )
    query.assert_called_once_with(
        f"{release.INDEX_JSON_BASES[index]}/vane-ai/{version}/json"
    )


@pytest.mark.parametrize(
    "problem",
    [
        "missing",
        "partial",
        "yanked",
        "digest",
        "build-tag",
        "wrong-version",
        "no-files",
    ],
)
def test_runtime_index_failures_do_not_authorize_signing(problem, monkeypatch):
    version = "0.2.0"
    files = indexed_wheels(version)
    status, document = 200, {"urls": files}
    if problem == "missing":
        status, document = 404, None
    elif problem == "partial":
        files.pop()
    elif problem == "yanked":
        files[0]["yanked"] = True
    elif problem == "digest":
        files[0]["digests"]["sha256"] = "invalid"
    elif problem == "build-tag":
        files[0]["filename"] = files[0]["filename"].replace("-0.2.0-", "-0.2.0-1-")
    elif problem == "wrong-version":
        files[0]["filename"] = files[0]["filename"].replace("-0.2.0-", "-0.2.1-")
    else:
        document = {}
    monkeypatch.setattr(release, "_request_json", Mock(return_value=(status, document)))
    with pytest.raises(ValueError):
        preflight.require_indexed_runtime(
            release, version, "release", ROOT / "vane-provider-release.toml"
        )


@pytest.mark.parametrize(
    "version", ["0.2.0", "0.2.0a1", "0.2.0b1", "0.2.0rc1", "0.2.0.post1"]
)
def test_release_preflight_sanitizes_version_inputs_and_waits_for_index(
    version, tmp_path, monkeypatch
):
    for key, value in CONTEXT.items():
        monkeypatch.setenv(key, value)
    for key in (
        "SETUPTOOLS_SCM_PRETEND_VERSION",
        "SETUPTOOLS_SCM_PRETEND_VERSION_FOR_VANE_AI",
        "VANE_VERSION_BRANCH",
        "GITHUB_REF_NAME",
        "GITHUB_BASE_REF",
        "PYTHONPATH",
        "PYTHONHOME",
    ):
        monkeypatch.setenv(key, "untrusted")
    verify = Mock()
    monkeypatch.setattr(release, "verify_sources", verify)
    monkeypatch.setattr(preflight, "load_release_tools", Mock(return_value=release))
    derive = Mock(return_value=version)
    monkeypatch.setattr(preflight.subprocess, "check_output", derive)
    ancestor = Mock()
    monkeypatch.setattr(preflight.subprocess, "run", ancestor)
    indexed = Mock()
    monkeypatch.setattr(preflight, "require_indexed_runtime", indexed)
    output = tmp_path / "output"
    args = [
        "--channel",
        "release",
        "--extension-root",
        str(ROOT),
        "--manifest",
        str(ROOT / "vane-extension-release.toml"),
        "--vane-source",
        str(tmp_path),
        "--ci-tools-version",
        "a" * 40,
        "--github-output",
        str(output),
    ]
    assert preflight.main(args) == 0
    verify.assert_called_once()
    assert not any(
        name.startswith("SETUPTOOLS_SCM_PRETEND_VERSION")
        for name in derive.call_args.kwargs["env"]
    )
    assert not {
        "VANE_VERSION_BRANCH",
        "GITHUB_REF_NAME",
        "GITHUB_BASE_REF",
        "PYTHONPATH",
        "PYTHONHOME",
    }.intersection(derive.call_args.kwargs["env"])
    assert ancestor.call_args.args[0] == [
        "git",
        "merge-base",
        "--is-ancestor",
        preflight.PRODUCTION_KEY_REVISION,
        "HEAD",
    ]
    indexed.assert_called_once_with(
        release, version, "release", ROOT / "vane-provider-release.toml"
    )
    assert output.read_text() == f"vane_version={version}\n"
    output.unlink()
    indexed.side_effect = ValueError("not indexed")
    with pytest.raises(ValueError, match="not indexed"):
        preflight.main(args)
    assert not output.exists()
    indexed.side_effect = None
    indexed.reset_mock()
    derive.return_value = "0.2.0.dev612"
    with pytest.raises(release.ReleaseValidationError):
        preflight.main(args)
    indexed.assert_not_called()
    assert not output.exists()


def arguments():
    return argparse.Namespace(
        phase="prepare",
        signing_profile="production",
        signing_private_key=None,
        consume_signing_private_key=False,
        package_local_runtime=False,
        runtime_python=[],
        runtime_wheel=[],
    )


def test_prepare_refuses_keys_and_full_build_refuses_publishing_profiles():
    args = arguments()
    builder._validate_phase(args)
    for name, value in (
        ("signing_private_key", Path("private.pem")),
        ("consume_signing_private_key", True),
        ("package_local_runtime", True),
        ("runtime_python", [Path("python")]),
        ("runtime_wheel", [Path("runtime.whl")]),
    ):
        modified = argparse.Namespace(**{**vars(args), name: value})
        with pytest.raises(
            builder.QualificationError, match="no signing key or runtime"
        ):
            builder._validate_phase(modified)
    for profile in ("production", "testpypi"):
        with pytest.raises(
            builder.QualificationError, match="isolated prepare/sign/package"
        ):
            builder._validate_phase(
                argparse.Namespace(
                    **{**vars(args), "phase": "full", "signing_profile": profile}
                )
            )


def prepared_data(tmp_path):
    bundle = tmp_path / "prepared"
    unsigned = bundle / "artifacts/vortex.duckdb_extension"
    unsigned.parent.mkdir(parents=True)
    unsigned.write_bytes(b"vortex native payload" * 60000 + b"\0" * 256)
    licenses = bundle / "licenses/vortex"
    licenses.mkdir(parents=True)
    for name in package.LICENSE_NAMES:
        (licenses / name).write_text("license notice")
    signed = tmp_path / "signed/vortex.duckdb_extension"
    signed.parent.mkdir()
    signed.write_bytes(unsigned.read_bytes()[:-256] + b"s" * 256)
    return unsigned, signed


def test_packaging_rechecks_original_payload_and_exact_notices(tmp_path):
    unsigned, signed = prepared_data(tmp_path)
    assert package.require_signed_payload(unsigned, signed) == signed
    assert len(package.require_licenses(tmp_path / "prepared/licenses/vortex")) == 6
    original = signed.read_bytes()
    for invalid in (
        b"x" + original[1:],
        original + b"x",
        original[:-1],
        unsigned.read_bytes(),
    ):
        signed.write_bytes(invalid)
        with pytest.raises(builder.QualificationError):
            package.require_signed_payload(unsigned, signed)
    signed.unlink()
    signed.symlink_to(unsigned)
    with pytest.raises(builder.QualificationError, match="non-symlink"):
        package.require_signed_payload(unsigned, signed)
    notice = tmp_path / "prepared/licenses/vortex/Vane-NOTICE.txt"
    notice.unlink()
    with pytest.raises(builder.QualificationError, match="reviewed set"):
        package.require_licenses(notice.parent)
    notice.symlink_to(unsigned)
    with pytest.raises(builder.QualificationError, match="non-symlink"):
        package.require_licenses(notice.parent)


def test_packaging_rejects_symlink_parents_and_oversized_data(tmp_path):
    unsigned, signed = prepared_data(tmp_path)
    linked = tmp_path / "alias"
    linked.symlink_to(unsigned.parents[1], target_is_directory=True)
    with pytest.raises(builder.QualificationError, match="non-symlink"):
        package.require_signed_payload(
            linked / "artifacts/vortex.duckdb_extension", signed
        )
    with unsigned.open("r+b") as output:
        output.truncate(package.MAX_ARTIFACT_BYTES + 1)
    with pytest.raises(builder.QualificationError, match="bounded regular"):
        package.require_signed_payload(unsigned, signed)


@pytest.mark.parametrize(
    "profile,version",
    [
        ("production", "0.2.0.dev612"),
        ("testpypi", "0.2.0"),
        ("production", "0.2.0+local"),
        ("production", "1!0.2.0"),
        ("production", "0.2"),
        ("production", "0.2.0-rc1"),
    ],
)
def test_package_runtime_channel_cannot_be_relabeled(profile, version):
    with pytest.raises(builder.QualificationError, match="publication channel"):
        package.require_runtime_wheels([], version, profile)


def test_package_uses_one_exact_runtime_matrix():
    wheels = [Path(entry["filename"]) for entry in indexed_wheels("0.2.0")]
    package.require_runtime_wheels(wheels, "0.2.0", "production")
    for invalid in (
        wheels + [wheels[0]],
        [Path(str(wheels[0]).replace("-0.2.0-", "-0.2.0-1-"))],
        [Path(str(wheels[0]).replace("-0.2.0-", "-0.2.1-"))],
    ):
        with pytest.raises(builder.QualificationError):
            package.require_runtime_wheels(invalid, "0.2.0", "production")


@pytest.mark.parametrize("fail", [False, True])
def test_fresh_package_verifies_every_runtime_and_emits_nothing_on_failure(
    fail, tmp_path, monkeypatch
):
    prepared_data(tmp_path)
    wheels = [tmp_path / entry["filename"] for entry in indexed_wheels("0.2.0")]
    for wheel in wheels:
        wheel.touch()
    monkeypatch.setattr(builder, "_require_git_revision", Mock())
    audit = Mock()
    monkeypatch.setattr(builder, "_require_no_undefined_duckdb_symbols", audit)
    monkeypatch.setattr(
        builder, "_platform_tag", Mock(return_value=builder.PROVIDER_PLATFORM_TAG)
    )
    cleanup = Mock()
    monkeypatch.setattr(
        builder,
        "_builder_python",
        Mock(return_value=(SimpleNamespace(cleanup=cleanup), Path(sys.executable))),
    )
    commands, count = [], []

    def build(**kwargs):
        assert kwargs["trust_identity"] == "astrovela/vane"
        assert len(tuple(kwargs["license_files"])) == 6
        wheel = (
            kwargs["output_directory"]
            / f"vane_extension_vortex-0.2.0-{INTERPRETERS[len(count)]}-none-manylinux_2_28_x86_64.whl"
        )
        count.append(wheel)
        wheel.write_bytes(b"synthetic wheel")
        return wheel

    def run(command, **kwargs):
        commands.append(command)
        if fail and len(count) == len(wheels):
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(builder, "_build_provider_wheel", build)
    monkeypatch.setattr(builder, "_run", run)
    args = [
        "--extension-root",
        str(ROOT),
        "--manifest",
        str(ROOT / "vane-extension-release.toml"),
        "--vane-source",
        str(tmp_path),
        "--vane-revision",
        "a" * 40,
        "--vane-version",
        "0.2.0",
        "--unsigned-directory",
        str(tmp_path / "prepared"),
        "--signed-directory",
        str(tmp_path / "signed"),
        "--output-directory",
        str(tmp_path / "dist"),
        "--signing-profile",
        "production",
    ]
    for wheel in wheels:
        args.extend(["--runtime-python", sys.executable, "--runtime-wheel", str(wheel)])
    if fail:
        with pytest.raises(subprocess.CalledProcessError):
            package.main(args)
        assert not list((tmp_path / "dist").iterdir())
    else:
        assert package.main(args) == 0
        assert len(list((tmp_path / "dist").glob("*.whl"))) == 5
    assert cleanup.call_count == 5
    audit.assert_called_once()
    verifies = [
        command
        for command in commands
        if any(str(item).endswith("/verify_extension_wheel.py") for item in command)
    ]
    assert len(verifies) == 5
    assert [command[command.index("--base-wheel") + 1] for command in verifies] == [
        str(wheel) for wheel in wheels
    ]
    assert all(
        "cmake" not in command and "cargo" not in command for command in commands
    )


def test_workflow_credentials_and_oidc_are_isolated_from_build_and_validation():
    for name in (
        "provider-release-preflight",
        "vane-provider-prepare",
        "vane-testpypi-wheels",
        "verify-testpypi-vortex",
        "verify-pypi-vortex",
    ):
        job = JOBS[name]
        assert job["permissions"] == {"contents": "read"}
        assert "environment" not in job
        assert "secrets" not in job
        assert "secrets." not in str(job) and "secrets[" not in str(job)
    assert JOBS["vane-provider-prepare"]["needs"] == "provider-release-preflight"
    assert "--phase prepare" in runs(JOBS["vane-provider-prepare"])
    assert "--signing-private-key" not in runs(JOBS["vane-provider-prepare"])
    signer = JOBS["vane-provider-sign"]
    assert signer["permissions"] == {"contents": "read"}
    assert (
        "production-signing" in signer["environment"]
        and "testpypi" in signer["environment"]
    )
    assert signer["needs"] == "vane-provider-prepare"
    assert "pip" not in runs(signer) and "setuptools" not in runs(signer)
    assert "/usr/bin/python3 -I -S" in runs(signer)
    assert "needs." not in runs(signer)
    vane_checkout = [
        step
        for step in signer["steps"]
        if step.get("with", {}).get("repository") == "AstroVela/vane"
    ]
    assert len(vane_checkout) == 1
    assert (
        vane_checkout[0]["with"]["ref"] == "${{ steps.manifest.outputs.vane_revision }}"
    )
    for name, endpoint in (
        ("publish-testpypi-vortex", "https://test.pypi.org/legacy/"),
        ("publish-pypi-vortex", "https://upload.pypi.org/legacy/"),
    ):
        job = JOBS[name]
        assert job["permissions"] == {"contents": "read", "id-token": "write"}
        assert not runs(job).strip()
        assert len(job["steps"]) == 2
        assert job["steps"][0]["uses"].startswith("actions/download-artifact@")
        assert job["steps"][1]["uses"].startswith("pypa/gh-action-pypi-publish@")
        assert job["steps"][1]["with"]["repository-url"] == endpoint
        assert job["steps"][1]["with"]["skip-existing"] is True


def test_release_promotion_requires_both_smokes_and_read_only_revalidation():
    pre = JOBS["provider-release-preflight"]
    assert "workflow_dispatch" in runs(pre)
    assert "GITHUB_REF_PROTECTED" in runs(pre)
    assert "source_id" in pre["outputs"] and "fork_version" in pre["outputs"]
    verify = JOBS["verify-pypi-promotion"]
    assert set(verify["needs"]) == {
        "assemble-testpypi-vortex",
        "testpypi-local-vortex-integration",
        "testpypi-ray-vortex-integration",
    }
    assert verify["permissions"] == {"contents": "read"}
    assert verify["environment"]["name"] == "pypi"
    assert "verify-promotion" in runs(verify)
    assert "--vane-version" in runs(verify)
    assert JOBS["publish-pypi-vortex"]["needs"] == [
        "assemble-testpypi-vortex",
        "verify-pypi-promotion",
    ]
    for name in ("verify-pypi-promotion", "publish-pypi-vortex"):
        assert JOBS[name]["if"] == "inputs.operation == 'release'"
    assert (
        "inputs.operation != 'release'" in WORKFLOW["concurrency"]["cancel-in-progress"]
    )


def test_all_release_consumers_download_original_immutable_ids():
    original = "${{ needs.assemble-testpypi-vortex.outputs.artifact_id }}"
    expected = {
        "vane-provider-sign": [
            "${{ needs.vane-provider-prepare.outputs.artifact_id }}"
        ],
        "vane-testpypi-wheels": [
            "${{ needs.vane-provider-prepare.outputs.artifact_id }}",
            "${{ needs.vane-provider-sign.outputs.artifact_id }}",
        ],
        "assemble-testpypi-vortex": [
            "${{ needs.vane-testpypi-wheels.outputs.artifact_id }}"
        ],
        **{
            name: [original]
            for name in (
                "publish-testpypi-vortex",
                "verify-testpypi-vortex",
                "testpypi-local-vortex-integration",
                "testpypi-ray-vortex-integration",
                "verify-pypi-promotion",
                "publish-pypi-vortex",
                "verify-pypi-vortex",
            )
        },
    }
    for name, artifacts in expected.items():
        assert [step["artifact-ids"] for step in downloads(JOBS[name])] == artifacts
        for step in downloads(JOBS[name]):
            assert step["digest-mismatch"] == "error"
            assert step["merge-multiple"] is True
            assert "name" not in step and "pattern" not in step
    for name, step in (
        ("vane-provider-prepare", "bundle"),
        ("vane-provider-sign", "signed"),
        ("vane-testpypi-wheels", "candidate"),
        ("assemble-testpypi-vortex", "distributions"),
    ):
        assert (
            JOBS[name]["outputs"]["artifact_id"]
            == "${{ steps." + step + ".outputs.artifact-id }}"
        )


@pytest.mark.parametrize("runner", ["local", "ray"])
def test_staged_smokes_check_bytes_and_source_derived_runtime_identities(runner):
    job = JOBS[f"testpypi-{runner}-vortex-integration"]
    assert job["permissions"] == {"contents": "read"}
    assert "environment" not in job
    environment = job["env"]
    assert (
        "inputs.operation == 'release'"
        in environment["VANE_EXPECTED_EXTENSION_TRUST_IDENTITY"]
    )
    assert "'astrovela/vane'" in environment["VANE_EXPECTED_EXTENSION_TRUST_IDENTITY"]
    assert "'https://pypi.org/simple/'" in environment["VANE_RUNTIME_INDEX_URL"]
    for name in (
        "VANE_EXPECTED_PACKAGE_VERSION",
        "VANE_EXPECTED_FORK_VERSION",
        "VANE_EXPECTED_DUCKDB_SOURCE_ID",
    ):
        assert "needs.assemble-testpypi-vortex.outputs." in environment[name]
    assert (
        'INDEX_URL="$VANE_RUNTIME_INDEX_URL" download_exact "vane-ai==$VANE_VERSION"'
        in runs(job)
    )
    assert 'cmp "${expected_vortex[0]}" "${vortex_wheels[0]}"' in runs(job)
    assert "unset PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_FIND_LINKS" in runs(job)
    assert "test/vane/test_vane_dynamic_vortex.py" in runs(job)


def test_prepare_bundle_contains_only_native_and_reviewed_license_data(tmp_path):
    unsigned, _signed = prepared_data(tmp_path)
    destination = tmp_path / "output"
    destination.mkdir()
    notices = package.require_licenses(tmp_path / "prepared/licenses/vortex")
    builder._emit_unsigned_bundle(unsigned, notices, destination)
    expected = {
        "artifacts/vortex.duckdb_extension",
        *[f"licenses/vortex/{name}" for name in package.LICENSE_NAMES],
    }
    assert {
        path.relative_to(destination).as_posix()
        for path in destination.rglob("*")
        if path.is_file()
    } == expected
    with pytest.raises(builder.QualificationError, match="empty output"):
        builder._emit_unsigned_bundle(unsigned, notices, destination)


def test_prepare_main_never_reads_a_key_or_packages_a_provider(tmp_path, monkeypatch):
    unsigned, _signed = prepared_data(tmp_path)
    notices = package.require_licenses(tmp_path / "prepared/licenses/vortex")
    args = arguments()
    vars(args).update(
        jobs=8,
        extension_root=ROOT,
        manifest=ROOT / "vane-extension-release.toml",
        vane_source=tmp_path,
        vane_revision="a" * 40,
        vane_vcpkg_installed=tmp_path,
        vcpkg_toolchain=tmp_path / "toolchain.cmake",
        cargo_about=Path(sys.executable),
        build_directory=tmp_path / "build",
        output_directory=tmp_path / "output",
    )
    monkeypatch.setattr(builder, "_parse_arguments", Mock(return_value=args))
    monkeypatch.setattr(builder, "_require_rust_contract", Mock())
    monkeypatch.setattr(builder, "_require_git_revision", Mock())
    monkeypatch.setattr(
        builder, "_require_vcpkg_toolchain", Mock(return_value=args.vcpkg_toolchain)
    )
    monkeypatch.setattr(builder, "_capture", Mock(return_value="b" * 40))
    monkeypatch.setattr(builder, "_build_environment", Mock(return_value={}))
    monkeypatch.setattr(
        builder, "_platform_tag", Mock(return_value=builder.PROVIDER_PLATFORM_TAG)
    )
    monkeypatch.setattr(builder, "_require_no_undefined_duckdb_symbols", Mock())
    monkeypatch.setattr(builder, "_stage_license_files", Mock(return_value=notices))
    forbidden = []
    for name in (
        "_read_signing_private_key",
        "_builder_python",
        "_build_provider_wheel",
    ):
        action = Mock(side_effect=AssertionError(f"prepare invoked {name}"))
        monkeypatch.setattr(builder, name, action)
        forbidden.append(action)
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        if "--outdir" in command:
            destination = Path(command[command.index("--outdir") + 1])
            (destination / "vane_ai-0.2.0-cp312-cp312-linux_x86_64.whl").touch()
        if "vane_loadable_extensions" in command:
            artifact = args.build_directory / "vane_extensions/vortex.duckdb_extension"
            artifact.parent.mkdir()
            artifact.write_bytes(unsigned.read_bytes())

    monkeypatch.setattr(builder, "_run", run)
    assert builder.main() == 0
    assert (
        args.output_directory / "artifacts/vortex.duckdb_extension"
    ).read_bytes() == unsigned.read_bytes()
    assert not list(args.output_directory.glob("*.whl"))
    assert len(list((args.output_directory / "licenses/vortex").iterdir())) == 6
    for action in forbidden:
        action.assert_not_called()
    assert not any(
        "sign_test_dynamic_extension.py" in str(command) for command in commands
    )

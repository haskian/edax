# Releasing the SDKs

The release workflow builds Python wheels and a source distribution from a version tag.
It publishes the tested files as a GitHub Release.
PyPI upload requires the repository variable `PYPI_PUBLISH_ENABLED` to equal `true`.
Leave that variable unset while preparing the first publication.

## Version and build

[version.yml](../.github/workflows/version.yml) runs after a push to `main`.
It reads the Conventional Commit subjects since the last root `v*` tag.
`versionbump!` requests a major release. `feat` requests a minor release. `fix` requests a patch release.
Other commit types produce no release. A breaking `feat!` still requests a minor release.

Cocogitto writes the changelog and calls `mise run version:set`.
That script updates the workspace version and the ABI crate version.
The next hook regenerates the bindings so their manifests and generated files use that version.
Maturin reads the Python package version from Cargo.
The workflow pushes the bump commit, root tag and Go module tag atomically, then dispatches [release.yml](../.github/workflows/release.yml) on that tag.
GitHub does not start another workflow for a tag pushed with `GITHUB_TOKEN`.
An explicit dispatch does start one.
See [GitHub's workflow trigger rules](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

The release workflow refuses a branch or a tag whose version differs from the tree.
It runs continuous integration (CI) against the tagged commit before building these files:

| Artifact | Targets |
| --- | --- |
| Linux wheels | x86_64 and aarch64, for glibc and musl |
| macOS wheels | x86_64 and arm64 |
| Windows wheel | x64 |
| Source distribution | Platforms with a compatible Rust toolchain |

The wheels use Python's stable application binary interface (ABI), with Python 3.10 as the minimum version.
Install checks cover Python 3.10 and 3.13 on Linux, macOS and Windows.
A separate job builds and imports the source distribution.
These checks run during a future release. Preparing the workflow does not run them.

The GitHub Release job attaches the wheels and source distribution after those checks pass.
The PyPI job uses those same files. It does not rebuild them.
Both jobs wait for all install checks.

## Go module versions

Each release creates two tags at the same commit: `vX.Y.Z` for the repository and
`crates/edax-go/vX.Y.Z` for `github.com/haskian/edax/crates/edax-go`.
The Go tag is required because the module lives below the repository root.
Go discovers the version from that tag; the module has no separate version literal.
See [Go's tag mapping rules](https://go.dev/ref/mod#vcs-version).

The version workflow creates both tags and pushes them with the bump commit in one atomic push.
It selects only root tags when computing the next release. Cocogitto's `tag_prefix = "v"` excludes the Go tags too.
The Go tag does not dispatch a Python release or depend on `PYPI_PUBLISH_ENABLED`.
This prepares source version discovery. It does not publish native Go libraries.
Go applications still need the [source build](sdk/go.md#build-from-source).

Before releasing version 2 or later, change the module path to end in `/v2`, `/v3`, and so on.
Update `go.mod`, all Go imports, alef's Go module configuration and examples together.
The tag remains `crates/edax-go/v2.0.0`; do not add another `/v2` directory to the tag prefix.
`mise run version:check-go` rejects a mismatched module path before the bump changes any version files.
The checker uses Go's `x/mod` parser and version rules. Its dependency lives in the separate `.mise` tooling module.
The release workflow repeats that check against the tagged tree.
See [Go's major version suffix rule](https://go.dev/ref/mod#major-version-suffixes).

These workflow changes have not been exercised. No Go module tag has been created by this work.
Do not move or reuse a published module tag; module proxies may retain its original content.

## Configure PyPI before enabling upload

Use these exact values for the PyPI trusted publisher:

| Field | Value |
| --- | --- |
| PyPI project name | `edax` |
| GitHub owner | `haskian` |
| Repository | `edax` |
| Workflow filename | `release.yml` |
| Environment | `pypi` |

For an existing project, add the publisher in its PyPI publishing settings.
For a new project, register a pending publisher in your PyPI account.
You must own the existing project or have permission to create it.
A GitHub repository transfer does not update PyPI's publisher configuration.
Replace any publisher that still names the previous owner.
See [PyPI's publisher setup](https://docs.pypi.org/trusted-publishers/adding-a-publisher/) and [pending publisher setup](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/).

Create the `pypi` environment in `haskian/edax` and configure a required reviewer.
Allow release tags matching `v*` in its deployment rules.
Configure these protections before enabling upload.
GitHub automatically creates missing environments without protection rules.
The environment name alone does not block a release.
See [GitHub's environment rules](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments).

Trusted publishing uses OpenID Connect (OIDC). The publish job grants `id-token: write` and needs no long-lived PyPI token.
The workflow runs through a dispatch because PyPI does not support a reusable workflow as the registered publisher.
See [PyPI's publishing guide](https://docs.pypi.org/trusted-publishers/using-a-publisher/) and [reusable workflow limitation](https://docs.pypi.org/trusted-publishers/troubleshooting/#reusable-workflows-on-github).

Check that GitHub Actions can push the version commit to `main` and dispatch workflows.
Keep squash merges enabled so a pull request title becomes one Conventional Commit subject.

When publication is approved, set the **repository** Actions variable `PYPI_PUBLISH_ENABLED` to `true`.
An environment variable cannot control this job-level condition because GitHub evaluates it before loading the environment.
The next release can then request the `pypi` environment's approval and upload its files.
Leaving the variable unset skips the upload job.

## Retry a release

After enabling publication, a maintainer can dispatch `release.yml` on an existing `v*` tag.
The tag must contain this workflow and declare the same version as its name.
The dispatch rebuilds and checks the artifacts before requesting approval.
It can create or update the corresponding GitHub Release.

Do not reuse a version that PyPI already accepted.
PyPI does not allow an uploaded filename to be reused, even after deletion.
For a failed or partial upload, inspect the project's files before choosing a retry or a new patch release.

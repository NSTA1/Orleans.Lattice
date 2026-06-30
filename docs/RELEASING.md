# Releasing Orleans.Lattice packages

This document describes the per-package tag-and-publish protocol the `Publish` GitHub Actions workflow expects. It is the canonical reference for human release engineers; the same protocol is encoded in the repo's automation rules.

## Packages

The package family ships from this repository:

| Package | csproj path |
|---|---|
| `Orleans.Lattice` | `src/lattice/Orleans.Lattice.csproj` |
| `Orleans.Lattice.Replication` | `src/lattice.replication/Orleans.Lattice.Replication.csproj` |
| `Orleans.Lattice.Replication.Grpc` | `src/lattice.replication.grpc/Orleans.Lattice.Replication.Grpc.csproj` |
| `Orleans.Lattice.Storage.AzureTable` | `src/lattice.storage.azuretable/Orleans.Lattice.Storage.AzureTable.csproj` |
| `Orleans.Lattice.Dashboards` | `src/lattice.dashboards/Orleans.Lattice.Dashboards.csproj` |
| `Orleans.Lattice.Api.State` | `src/lattice.api.state/Orleans.Lattice.Api.State.csproj` |
| `Orleans.Lattice.Api.State.Grpc` | `src/lattice.api.state.grpc/Orleans.Lattice.Api.State.Grpc.csproj` |

Major and minor digits move in lockstep across the family. Patch digits may advance independently per package.

## Tag shape

The publish workflow's per-tag trigger globs match these tag shapes:

| Package | Tag shape |
|---|---|
| `Orleans.Lattice` | `lattice-v<X.Y.Z>` |
| `Orleans.Lattice.Replication` | `lattice.replication-v<X.Y.Z>` |
| `Orleans.Lattice.Replication.Grpc` | `lattice.replication.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Storage.AzureTable` | `lattice.storage.azuretable-v<X.Y.Z>` |
| `Orleans.Lattice.Dashboards` | `lattice.dashboards-v<X.Y.Z>` |
| `Orleans.Lattice.Api.State` | `lattice.api.state-v<X.Y.Z>` |
| `Orleans.Lattice.Api.State.Grpc` | `lattice.api.state.grpc-v<X.Y.Z>` |

For historical compatibility, the `v<X.Y.Z>` family tag (e.g. `v3.2.0`) is reserved for "the whole family at this version". When the family moves in lockstep, push **both** the family tag *and* each per-package tag - the publish workflow keys off the per-package tags.

## Release protocol

1. **Confirm the PR has merged.** Check out `main` and pull (`git checkout main && git pull origin main`) so the tag points at the squash-merge commit on `main`, never at the feature branch.

2. **Verify the working tree's `<Version>` slot.** For each package being released, `Get-Content src/<package>/<package>.csproj | Select-String "<Version>"` must show the version you intend to ship. The `<Version>` slot is authoritative - the publish workflow reads it to set the NuGet package version.

3. **Confirm CI is green on the `main` commit you intend to tag.** `gh run list --branch main --limit 5` - the build-and-test run on the squash-merge commit must be `completed/success`.

4. **Tag each package independently.** The publish workflow's per-tag trigger globs fire on `push` events to a **single tag ref**. A bulk push (`git push origin tag1 tag2 tag3`) sends all the refs in one HTTP request and GitHub coalesces them into a single push event - so the publish workflow fires for **at most one** of the tags, and the trailing tags ship no NuGet packages and create no GitHub Release. Push tags **one at a time**:

   ```powershell
   git push origin <package>-v<X.Y.Z>
   ```

   After each push, poll `gh run list` for a matching `event=push, headBranch=<tag>, name=Publish` run before pushing the next tag. A "no run detected within 2 min" result means the workflow trigger glob did not match - fix the trigger or the tag spelling before pushing further tags.

5. **Verify each publish run** reaches `completed/success` before declaring the release done. Failed runs leave NuGet in an inconsistent state where some packages of a coordinated release have shipped and others have not.

## Recovery for an accidental bulk push

If multiple tags were pushed in a single `git push origin tag1 tag2 ...` operation, only one publish run will fire. To recover:

1. Identify which package's publish run **did** fire (`gh run list --workflow Publish --limit 5`).
2. Delete the trailing remote tags:

   ```powershell
   git push origin --delete <missed-tag>
   ```

   Local tags can stay in place; only the remote refs need the delete-and-re-push.
3. Re-push each missed tag individually (step 4 above), polling for the matching publish run between each push.

## Updating `CHANGELOG.md`

Every release moves the working tree's `## [Unreleased]` section into a new dated `## [X.Y.Z] - YYYY-MM-DD` section. The footer `[X.Y.Z]: ...` compare link must be added in the same edit. The ship commit that merges the changelog edit is the commit the tag points at.


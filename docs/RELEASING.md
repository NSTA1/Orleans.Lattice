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
| `Orleans.Lattice.Api.Abstractions` | `src/lattice.api.abstractions/Orleans.Lattice.Api.Abstractions.csproj` |
| `Orleans.Lattice.Api.State` | `src/lattice.api.state/Orleans.Lattice.Api.State.csproj` |
| `Orleans.Lattice.Api.State.Grpc` | `src/lattice.api.state.grpc/Orleans.Lattice.Api.State.Grpc.csproj` |
| `Orleans.Lattice.Membership` | `src/lattice.membership/Orleans.Lattice.Membership.csproj` |
| `Orleans.Lattice.Membership.Entra` | `src/lattice.membership.entra/Orleans.Lattice.Membership.Entra.csproj` |
| `Orleans.Lattice.Membership.Entra.Graph` | `src/lattice.membership.entra.graph/Orleans.Lattice.Membership.Entra.Graph.csproj` |
| `Orleans.Lattice.Auth` | `src/lattice.auth/Orleans.Lattice.Auth.csproj` |
| `Orleans.Lattice.Api.Auth` | `src/lattice.api.auth/Orleans.Lattice.Api.Auth.csproj` |
| `Orleans.Lattice.Api.Auth.Grpc` | `src/lattice.api.auth.grpc/Orleans.Lattice.Api.Auth.Grpc.csproj` |
| `Orleans.Lattice.Api.Data` | `src/lattice.api.data/Orleans.Lattice.Api.Data.csproj` |
| `Orleans.Lattice.Api.Data.Grpc` | `src/lattice.api.data.grpc/Orleans.Lattice.Api.Data.Grpc.csproj` |
| `Orleans.Lattice.Backup` | `src/lattice.backup/Orleans.Lattice.Backup.csproj` |
| `Orleans.Lattice.Backup.AzureBlob` | `src/lattice.backup.azureblob/Orleans.Lattice.Backup.AzureBlob.csproj` |
| `Orleans.Lattice.Api.Backup` | `src/lattice.api.backup/Orleans.Lattice.Api.Backup.csproj` |
| `Orleans.Lattice.Api.Backup.Grpc` | `src/lattice.api.backup.grpc/Orleans.Lattice.Api.Backup.Grpc.csproj` |
| `Orleans.Lattice.Api.Replication` | `src/lattice.api.replication/Orleans.Lattice.Api.Replication.csproj` |
| `Orleans.Lattice.Api.Replication.Grpc` | `src/lattice.api.replication.grpc/Orleans.Lattice.Api.Replication.Grpc.csproj` |
| `Orleans.Lattice.Api.Mcp` | `src/lattice.api.mcp/Orleans.Lattice.Api.Mcp.csproj` |
| `Orleans.Lattice.Api.Mcp.Telemetry` | `src/lattice.api.mcp.telemetry/Orleans.Lattice.Api.Mcp.Telemetry.csproj` |
| `Orleans.Lattice.Api.Mcp.Telemetry.Azure` | `src/lattice.api.mcp.telemetry.azure/Orleans.Lattice.Api.Mcp.Telemetry.Azure.csproj` |
| `Orleans.Lattice.Api.Schema` | `src/lattice.api.schema/Orleans.Lattice.Api.Schema.csproj` |
| `Orleans.Lattice.Api.Schema.Grpc` | `src/lattice.api.schema.grpc/Orleans.Lattice.Api.Schema.Grpc.csproj` |
| `Orleans.Lattice.Api.TreeAdmin` | `src/lattice.api.treeadmin/Orleans.Lattice.Api.TreeAdmin.csproj` |
| `Orleans.Lattice.Api.TreeAdmin.Grpc` | `src/lattice.api.treeadmin.grpc/Orleans.Lattice.Api.TreeAdmin.Grpc.csproj` |
| `Orleans.Lattice.Schema` | `src/lattice.schema/Orleans.Lattice.Schema.csproj` |
| `Orleans.Lattice.Explorer.Core` | `src/lattice.explorer/Core/Orleans.Lattice.Explorer.Core.csproj` |
| `Orleans.Lattice.Explorer.UI` | `src/lattice.explorer/UI/Orleans.Lattice.Explorer.UI.csproj` |
| `Orleans.Lattice.Explorer.Backup` | `src/lattice.explorer/Backup/Orleans.Lattice.Explorer.Backup.csproj` |
| `Orleans.Lattice.Explorer.Access` | `src/lattice.explorer/Access/Orleans.Lattice.Explorer.Access.csproj` |
| `Orleans.Lattice.Explorer.Schema` | `src/lattice.explorer/Schema/Orleans.Lattice.Explorer.Schema.csproj` |
| `Orleans.Lattice.Explorer.Web` | `src/lattice.explorer/WebHosting/Orleans.Lattice.Explorer.Web.csproj` |
| `Orleans.Lattice.Explorer.Entra` | `src/lattice.explorer.entra/Orleans.Lattice.Explorer.Entra.csproj` |
| `Orleans.Lattice.Explorer.Entra.Web` | `src/lattice.explorer.entra.web/Orleans.Lattice.Explorer.Entra.Web.csproj` |
| `Orleans.Lattice.Caching.AzureBlob` | `src/lattice.caching.azureblob/Orleans.Lattice.Caching.AzureBlob.csproj` |
| `Orleans.Lattice.Scaling` | `src/lattice.scaling/Orleans.Lattice.Scaling.csproj` |

## Tag shape

The publish workflow's per-tag trigger globs match these tag shapes:

| Package | Tag shape |
|---|---|
| `Orleans.Lattice` | `lattice-v<X.Y.Z>` |
| `Orleans.Lattice.Replication` | `lattice.replication-v<X.Y.Z>` |
| `Orleans.Lattice.Replication.Grpc` | `lattice.replication.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Storage.AzureTable` | `lattice.storage.azuretable-v<X.Y.Z>` |
| `Orleans.Lattice.Dashboards` | `lattice.dashboards-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Abstractions` | `lattice.api.abstractions-v<X.Y.Z>` |
| `Orleans.Lattice.Api.State` | `lattice.api.state-v<X.Y.Z>` |
| `Orleans.Lattice.Api.State.Grpc` | `lattice.api.state.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Membership` | `lattice.membership-v<X.Y.Z>` |
| `Orleans.Lattice.Membership.Entra` | `lattice.membership.entra-v<X.Y.Z>` |
| `Orleans.Lattice.Membership.Entra.Graph` | `lattice.membership.entra.graph-v<X.Y.Z>` |
| `Orleans.Lattice.Auth` | `lattice.auth-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Auth` | `lattice.api.auth-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Auth.Grpc` | `lattice.api.auth.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Data` | `lattice.api.data-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Data.Grpc` | `lattice.api.data.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Backup` | `lattice.backup-v<X.Y.Z>` |
| `Orleans.Lattice.Backup.AzureBlob` | `lattice.backup.azureblob-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Backup` | `lattice.api.backup-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Backup.Grpc` | `lattice.api.backup.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Replication` | `lattice.api.replication-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Replication.Grpc` | `lattice.api.replication.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Mcp` | `lattice.api.mcp-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Mcp.Telemetry` | `lattice.api.mcp.telemetry-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Mcp.Telemetry.Azure` | `lattice.api.mcp.telemetry.azure-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Schema` | `lattice.api.schema-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Schema.Grpc` | `lattice.api.schema.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Api.TreeAdmin` | `lattice.api.treeadmin-v<X.Y.Z>` |
| `Orleans.Lattice.Api.TreeAdmin.Grpc` | `lattice.api.treeadmin.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Schema` | `lattice.schema-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Core` | `lattice.explorer.core-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.UI` | `lattice.explorer.ui-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Backup` | `lattice.explorer.backup-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Access` | `lattice.explorer.access-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Schema` | `lattice.explorer.schema-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Web` | `lattice.explorer.web-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Entra` | `lattice.explorer.entra-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Entra.Web` | `lattice.explorer.entra.web-v<X.Y.Z>` |
| `Orleans.Lattice.Caching.AzureBlob` | `lattice.caching.azureblob-v<X.Y.Z>` |
| `Orleans.Lattice.Scaling` | `lattice.scaling-v<X.Y.Z>` |

For historical compatibility, the `v<X.Y.Z>` family tag (e.g. `v3.2.0`) is reserved for "the whole family at this version". When the family moves in lockstep, push **both** the family tag *and* each per-package tag - the publish workflow keys off the per-package tags.

## Release protocol

1. **Confirm the PR has merged.** Check out `main` and pull (`git checkout main && git pull origin main`) so the tag points at the squash-merge commit on `main`, never at the feature branch.

2. **Verify the working tree's `<Version>` slot.** For each package being released, `Get-Content src/<package>/<package>.csproj | Select-String "<Version>"` must show the version you intend to ship. The `<Version>` slot is authoritative - the publish workflow reads it to set the NuGet package version.

3. **Confirm CI was green on the PR before it merged.** CI (the `build-and-test` job) runs only on `pull_request` events, **not** on `push` to `main`. So there is no CI run on the squash-merge commit itself, and that commit's combined status reads `pending` with zero checks - this is expected, not a failure, so do not go hunting for a push-to-main run, check-suites, or check-runs on the merge commit. The green gate is the merged PR's final CI run: `gh pr checks <pr-number>` (or `gh run list --branch <feature-branch> --limit 5`) must show the `build-and-test` run `completed/success`. Because a squash merge replays the already-reviewed tree onto `main`, that PR run is the authoritative signal that the commit you are tagging is green.

4. **Tag each package independently.** The publish workflow's per-tag trigger globs fire on `push` events to a **single tag ref**. A bulk push (`git push origin tag1 tag2 tag3`) sends all the refs in one HTTP request and GitHub coalesces them into a single push event - so the publish workflow fires for **at most one** of the tags, and the trailing tags ship no NuGet packages and create no GitHub Release. Push tags **one at a time**:

   ```powershell
   git push origin <package>-v<X.Y.Z>
   ```

   After each push, poll `gh run list` for a matching `event=push, headBranch=<tag>, name=Publish` run before pushing the next tag. A "no run detected within 2 min" result means the workflow trigger glob did not match - fix the trigger or the tag spelling before pushing further tags.

5. **Verify each publish run** reaches `completed/success` before declaring the release done. Failed runs leave NuGet in an inconsistent state where some packages of a coordinated release have shipped and others have not.

6. **Bump the reference-architecture package pins as a post-release action.** The package version bump itself (the `<Version>` slot) belongs in the shipping chore PR alongside the changelog, per steps 1-2 - that is what tag-and-publish releases to NuGet. The `reference-architecture/` hosts, by contrast, consume the family through `PackageReference` to **published** NuGet packages (never `ProjectReference` into `src/`), and the `build-and-test` reference-architecture job restores those versions from nuget.org. So a pin bump to a version that has not shipped yet fails restore with `NU1102: Unable to find package ... with version (>= X.Y.Z)`. Never bump a reference-architecture pin in the same PR that ships the package - that PR cannot go green until the very package it is publishing exists. Instead, the order is: **(a)** the chore PR bumps `<Version>` + folds the changelog and merges; **(b)** the tag publishes the package (steps 4-5); **(c)** once the new version is not just published but actually **indexed and restorable** on nuget.org (indexing lags the publish run's `completed/success` by a few minutes - confirm with `nuget list` / a scratch restore, not just the green publish run), raise a **separate follow-up PR** that advances the affected `reference-architecture/**/*.csproj` pins to the just-published version(s). That PR restores cleanly because the packages now exist. Only reference-architecture hosts that actually consume a bumped package need updating; leave the others untouched.

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

Every release folds the working tree's `## Unreleased` section into a dated `## [YYYY-MM-DD]` section keyed by the **release date, not by a version**. The dated sections live under a plain `## Released` heading - the counterpart to `## Unreleased`; both rolling titles are unbracketed and unlinked so they render as plain text, while each dated section keeps its bracketed `## [YYYY-MM-DD]` form. Because the family ships per-package (patch digits advance independently, and there is usually no single family version for a given ship), a day can carry several package waves - they all belong to **one** date section for that day:

- If a `## [YYYY-MM-DD]` section for today does **not** exist yet, create it: an opening paragraph that enumerates every package version shipped that day (new-package debuts and per-package advances alike), followed by `### Added` / `### Changed` / `### Fixed` / `### Security` subsections (Keep a Changelog order).
- If a `## [YYYY-MM-DD]` section for today **already exists**, merge the new entries into it rather than opening a second dated section: add each bullet under the shared subheading for its change kind, and extend the opening paragraph with the newly-shipped package versions.
- Every bullet names the exact package version(s) that carried it (e.g. `` (`Orleans.Lattice.Replication` 8.0.4) ``), so the granular per-package history survives the consolidation.
- A coordinated lockstep release uses the same date header; its opening paragraph states the single family version every package advanced to.

The ship commit that merges the changelog edit is the commit the tag(s) point at.

### Section titles and compare links

The two rolling section titles - `## Unreleased` and `## Released` - are plain, unbracketed headings, and `CHANGELOG.md` carries **no footer link-reference definitions**; each dated section keeps its bracketed `## [YYYY-MM-DD]` form.

Earlier revisions followed the "Keep a Changelog" convention of a footer `[YYYY-MM-DD]: .../compare/<base>...<target>` block (plus `[Unreleased]: .../compare/vX.Y.Z...HEAD`). That convention was dropped because it rendered inconsistently: the family tags **per-package** (`lattice.<pkg>-v<X.Y.Z>`) and only mints a **family** tag (`vX.Y.Z`) for a coordinated lockstep release, so most dates had no single tag to anchor a link - only the occasional lockstep date and `Unreleased` turned into links, while every per-package-wave date stayed plain bracketed text, a half-linked ladder. Each dated section's opening paragraph already enumerates the exact per-package versions and their `lattice.<pkg>-v<X.Y.Z>` tags, which is the authoritative record; compare those tags directly for a diff. Do not reintroduce footer compare-link definitions.


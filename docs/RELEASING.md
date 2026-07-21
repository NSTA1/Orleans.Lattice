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
| `Orleans.Lattice.Api.Mcp` | `src/lattice.api.mcp/Orleans.Lattice.Api.Mcp.csproj` |
| `Orleans.Lattice.Api.Mcp.Telemetry` | `src/lattice.api.mcp.telemetry/Orleans.Lattice.Api.Mcp.Telemetry.csproj` |
| `Orleans.Lattice.Api.Mcp.Telemetry.Azure` | `src/lattice.api.mcp.telemetry.azure/Orleans.Lattice.Api.Mcp.Telemetry.Azure.csproj` |
| `Orleans.Lattice.Api.Schema` | `src/lattice.api.schema/Orleans.Lattice.Api.Schema.csproj` |
| `Orleans.Lattice.Api.Schema.Grpc` | `src/lattice.api.schema.grpc/Orleans.Lattice.Api.Schema.Grpc.csproj` |
| `Orleans.Lattice.Schema` | `src/lattice.schema/Orleans.Lattice.Schema.csproj` |
| `Orleans.Lattice.Explorer.Core` | `src/lattice.explorer/Core/Orleans.Lattice.Explorer.Core.csproj` |
| `Orleans.Lattice.Explorer.UI` | `src/lattice.explorer/UI/Orleans.Lattice.Explorer.UI.csproj` |
| `Orleans.Lattice.Explorer.Backup` | `src/lattice.explorer/Backup/Orleans.Lattice.Explorer.Backup.csproj` |
| `Orleans.Lattice.Explorer.Access` | `src/lattice.explorer/Access/Orleans.Lattice.Explorer.Access.csproj` |
| `Orleans.Lattice.Explorer.Schema` | `src/lattice.explorer/Schema/Orleans.Lattice.Explorer.Schema.csproj` |
| `Orleans.Lattice.Explorer.Web` | `src/lattice.explorer/WebHosting/Orleans.Lattice.Explorer.Web.csproj` |
| `Orleans.Lattice.Explorer.Entra` | `src/lattice.explorer.entra/Orleans.Lattice.Explorer.Entra.csproj` |
| `Orleans.Lattice.Scaling` | `src/lattice.scaling/Orleans.Lattice.Scaling.csproj` | across the family. Patch digits may advance independently per package.

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
| `Orleans.Lattice.Api.Mcp` | `lattice.api.mcp-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Mcp.Telemetry` | `lattice.api.mcp.telemetry-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Mcp.Telemetry.Azure` | `lattice.api.mcp.telemetry.azure-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Schema` | `lattice.api.schema-v<X.Y.Z>` |
| `Orleans.Lattice.Api.Schema.Grpc` | `lattice.api.schema.grpc-v<X.Y.Z>` |
| `Orleans.Lattice.Schema` | `lattice.schema-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Core` | `lattice.explorer.core-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.UI` | `lattice.explorer.ui-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Backup` | `lattice.explorer.backup-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Access` | `lattice.explorer.access-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Schema` | `lattice.explorer.schema-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Web` | `lattice.explorer.web-v<X.Y.Z>` |
| `Orleans.Lattice.Explorer.Entra` | `lattice.explorer.entra-v<X.Y.Z>` |
| `Orleans.Lattice.Scaling` | `lattice.scaling-v<X.Y.Z>` |

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

Every release moves the working tree's `## [Unreleased]` section into a new dated `## [X.Y.Z] - YYYY-MM-DD` section. The ship commit that merges the changelog edit is the commit the tag points at.

### Compare links

The footer `[X.Y.Z]: .../compare/<base>...<target>` links follow the "Keep a Changelog" convention, but they only work when a git tag exists for both ends. Because this repo tags **per-package** (`lattice.<pkg>-v<X.Y.Z>`) and only creates a **family** tag (`vX.Y.Z`) for a coordinated lockstep release, the rule is:

- **Add a compare link only when a real family `vX.Y.Z` tag exists for the section** (a coordinated lockstep release - typically a minor/major boundary, plus any patch wave that was actually tagged family-wide). Point its `<base>` at the **nearest existing prior family tag**, skipping over any intervening per-package waves that have no family tag.
- **Do not add a compare link for a per-package patch wave** (where each package ships under its own `lattice.<pkg>-v<X.Y.Z>` tag and there is no single family `vX.Y.Z` tag). The section header is a family-umbrella label naming the highest package patch in the wave; it has no tag to link to. Instead, the section's prose enumerates the exact per-package versions, which is the authoritative record.
- Keep `[Unreleased]` pointed at the most recent existing family tag (`compare/vX.Y.Z...HEAD`), never at a per-package-wave label.

A footer comment in `CHANGELOG.md` records which sections intentionally carry no compare link, so the omission is not mistaken for an oversight.


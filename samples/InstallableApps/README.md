# Installable apps

A single-silo tour of the installable App concept: an embedded manifest, a
version-pinned operator consent, generated app-owned authorization rules, and
activation / teardown through the Apps API facade.

## What it shows

One in-process Orleans silo runs the control-plane stack:

- **Membership** (`AddLatticeMembership`) resolves the ambient caller credential
  into a subject and group membership.
- **Auth** (`AddLatticeAuth`) installs the fail-closed enforcement gate.
- **Apps** (`AddLatticeApps`) registers an in-image app source, registry and
  activation pipeline.
- **Apps API** (`AddLatticeAppsApi`) adds the operator facade
  `ILatticeAppsControl`.

The program walks five acts:

1. **Inspect before trust.** The embedded manifest is loaded with
   `AppManifestResources`, then described through `DescribeAsync` before install.
   Both paths expose requested trees, roles and operations without activating app
   code.
2. **Install as an operator.** A bootstrap administrator installs version
   `1.0.0`, binds the app role to a membership group, and pins a structural
   ceiling. A caller without `AppInstall` is denied before any app metadata is
   touched.
3. **Enable and use the app tree.** Activation creates the structural tree
   `a/sample-crm/records`, compiles rules whose ids start with `app:`, and lets a
   member of the bound group write and read while a non-member is denied.
4. **Guard rails.** Direct operator writes to an app-owned rule id are rejected
   with `LatticeAppOwnedRuleException`. Reducing consent below the manifest's
   requested operations records a structured activation failure and withdraws the
   app rules without stopping the silo.
5. **Disable and uninstall.** The lifecycle facade removes app-owned rules and
   soft-deletes the app tree. Data is not purged by uninstall.

## Run it

```
dotnet run --project samples/InstallableApps
```

Expected tail:

```
== Act 5: disable and uninstall ==
  DisableAsync -> Disabled (changed: True)
  UninstallAsync -> Uninstalled (changed: True)
  app-owned rules after uninstall -> 0
  app tree after uninstall -> registered: True; soft-deleted: True

[OK] installable app lifecycle ran end-to-end; consent and app-owned rule guards stayed fail-closed.
```

The process exits `0` on success and `1` if any guard fails to hold.

## How authorization works here

The gate runs default-deny. `platform-operator` is configured as a bootstrap
administrator and therefore holds the cluster-wide `AppInstall` operation that
all Apps API verbs require. The manifest's `writer` role is bound to the
membership group `sample-crm-writers`; activation compiles that binding into
ordinary authorization rules over `a/sample-crm/records`. Only `alice`, the user
placed in that group, can read and write the app tree.

The sample intentionally reduces the app's consent to `Read` after a successful
enable. The manifest still requests `Read` and `Write`, so reconciliation fails
with `CeilingExceeded`, withdraws the compiled rules, reports the app as failed,
and leaves the silo healthy.

## Where to go next

- App manifests, installation, consent and activation:
  [docs/lattice.apps](../../docs/lattice.apps/README.md).
- The transport-independent app-control facade:
  [docs/lattice.api.apps](../../docs/lattice.api.apps/README.md).
- Exposing app-declared MCP tools:
  [docs/lattice.api.mcp.apps](../../docs/lattice.api.mcp.apps/README.md).
- Driving the facade remotely over gRPC:
  [docs/lattice.api.apps.grpc](../../docs/lattice.api.apps.grpc/README.md).

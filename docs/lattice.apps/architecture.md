# Architecture

The apps add-on composes with Orleans.Lattice's registry, authorization, tenant, tree, and replication seams. It does not add a separate data-plane authorization evaluator. See the [engine guide](README.md) for manifest rules and detailed failure behavior.

## Registration and resolution

Register `AddLatticeApps` after `AddLattice`; activation also needs membership and authorization. `AddLatticeApp` registers an embedded manifest as available for installation, while `AddLatticeAppSource` adds a catalogue source. Resolution validates a manifest before app code is loaded. Installed versions resolve through the source recorded for the install.

## Install and ownership

Each facade operation resolves the caller's active tenant; without tenancy, the default tenant gives cluster-wide behavior. Caller-facing administration requires `LatticeOperation.AppInstall` at cluster-wide scope before reading or changing install metadata. Installation records group-only role bindings and a version-pinned capability ceiling.

Structural trees use the app namespace. Adopted trees retain their pre-existing logical ids. Ownership checks prevent sharing across installs: structural ownership is retained through soft-delete retention; an adopted-tree claim is released when the install no longer declares that tree or is uninstalled.

## Activation and lifecycle

`IAppActivationPipeline` is the public lifecycle seam. Enabling or reconciling an enabled install validates the installed manifest, checks requested roles and bridge grants against consent, confirms ownership, applies declared replication when the runtime replication authority is available, provisions structural trees, replaces app-owned authorization rules, and records the outcome. App roles compile to ordinary authorization rules evaluated by the shared access gate. Membership groups confer roles; an explicit deny can remove a role, but unrelated grants do not confer one.

Disabling removes app-owned grants while keeping trees, data, and replication. Uninstall removes grants, withdraws replication that the app itself enrolled, soft-deletes structural trees, and leaves adopted trees intact. The core retention handling performs any later physical purge. Startup reconciliation runs in the background; an app failure is recorded rather than preventing silo startup.

## Public seams

| Public seam | Responsibility |
|---|---|
| `IAppSource` / `IAppCatalogSource` | Resolve manifests and enumerate source offerings. |
| `IAppRegistry` / `IAppRegistryProjection` | Persist install transitions and expose the compiled current view. |
| `IAppActivationPipeline` | Run enable, disable, uninstall, and reconcile. |
| `ITreeOwnershipGuard` | Let core alias changes respect app tree ownership. |
| `IAppChangeFeedHandler` | Handle mutations declared by app subscriptions. |

The control API, gRPC, and MCP integrations are separate layers; see the [facade API](../lattice.api.apps/api.md), [gRPC architecture](../lattice.api.apps.grpc/architecture.md), and [MCP architecture](../lattice.api.mcp.apps/architecture.md).

## See also

- [Public API](api.md)
- [Configuration](configuration.md)
- [Engine guide](README.md)
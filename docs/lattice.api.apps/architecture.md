# Architecture

`Orleans.Lattice.Api.Apps` is the in-process, transport-independent facade over the apps engine. It composes lifecycle, consent, catalogue, workspace, role-binding, and bridge contracts. See the [facade guide](README.md) for details of each operation.

## Control and catalogue path

A control call validates input, resolves the caller's active tenant, and checks `LatticeOperation.AppInstall` at cluster-wide scope before reading source or registry metadata. Read operations are gated too because install records contain consent and bindings. The catalogue uses the same gate before reading sources or registry state; its capabilities call is advisory. Source offerings are cluster-wide, while install state is joined for the caller's tenant.

## Workspace path

The workspace projects enabled installs for which the caller holds at least one bound role in the active tenant. It returns a sanitized view and digest-verified assets for the installed version. Consent, exception scopes, bindings, and composed physical tree ids remain on the administrative surface.

## Bridge path

`ILatticeAppBridge` is the data boundary for app UIs. It validates and rate-limits requests, requires an enabled install at the supplied revision in the active tenant, intersects the manifest bridge request with operator consent, resolves an app-local tree server-side, and requires an app-owned grant for the concrete operation and key or prefix. Reads, scans, writes, and deletes execute under the caller's identity and tenant, so the core data path applies its normal authorization too. No operation accepts a physical tree id.

## Public seams

| Contract | Boundary |
|---|---|
| `ILatticeAppsControl` | Install lifecycle, description, consent, and capability probe. |
| `ILatticeAppRoleBindings` | Version-pinned replacement of role-to-group bindings. |
| `ILatticeAppCatalog` | Source listings and pre-install descriptions. |
| `ILatticeAppWorkspace` | Role-filtered installed-app view and UI assets. |
| `ILatticeAppBridge` | Caller-scoped app-UI data access. |

The gRPC transport applies a separate transport gate before invoking these contracts; see [gRPC architecture](../lattice.api.apps.grpc/architecture.md).

## See also

- [Public API](api.md)
- [Configuration](configuration.md)
- [Control facade guide](README.md)
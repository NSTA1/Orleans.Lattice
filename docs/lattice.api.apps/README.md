# Orleans.Lattice.Api.Apps

The transport-agnostic **control facade** for
[installable apps](../lattice.apps/README.md): one generic contract,
`ILatticeAppsControl`, to install, enable, disable, uninstall, list and describe
apps and to manage each install's consent, dispatching by app slug. It is reachable
in-process, and over the network through the
[gRPC binding](../lattice.api.apps.grpc/README.md).

## What is it?

`Orleans.Lattice.Api.Apps` implements `ILatticeAppsControl` (declared in
[`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md), namespace
`Orleans.Lattice.Api.Apps`) by **composition**: it owns no admin plane of its own.
Every verb authorizes the caller and then delegates to the engine in
[`Orleans.Lattice.Apps`](../lattice.apps/README.md) - the app registry for install,
upgrade and consent, the app source for manifest description, and the activation
pipeline for enable, disable and uninstall. There is one contract for every app;
nothing is generated or registered per app.

## Registration

The facade requires the apps engine; register it after `AddLatticeApps`:

```csharp verify
using Orleans.Lattice.Apps;
using Orleans.Lattice.Api.Apps;

siloBuilder.AddLatticeApps();
siloBuilder.AddLatticeAppsApi();
```

`AddLatticeAppsApi` registers `ILatticeAppsControl` as a singleton. Calling it
before `AddLatticeApps` fails fast.

## Verbs

| Verb | Behaviour |
|---|---|
| `InstallAsync(AppInstallRequest)` | Installs an exact source version with role-to-group bindings and an explicit ceiling. Installation does not enable the app. Installing a different version over a live install upgrades it in place, keeping its lifecycle state, and re-applies an enabled app. Installing the version already installed is refused; change consent with `UpdateConsentAsync` instead. |
| `EnableAsync(slug)` | Runs the activation pipeline: validates the manifest, checks it against the ceiling, provisions trees and grants roles. An excess fails activation until re-consented. |
| `DisableAsync(slug)` | Withdraws the app's grants; trees and data stay. |
| `UninstallAsync(slug)` | Withdraws the app's grants and soft-deletes its structural trees. Never purges data. |
| `ListAsync()` | Summaries of the installs in the caller's tenant, including uninstalled records. |
| `DescribeAsync(slug, version?)` | The manifest's requested capabilities (trees, roles with operations and scopes, subscriptions, MCP tools, replication and schema declarations) plus the install's state, provenance, bindings and ceiling. Works before installation, without loading app code; returns `null` for an unknown app or version. |
| `GetConsentAsync(slug)` | The ceiling pinned to the installed version, or `null` when the app is not installed. |
| `UpdateConsentAsync(AppConsentUpdate)` | Replaces the whole ceiling for the explicitly named installed version, then re-applies an enabled app so a reduced ceiling cannot leave stale authority. Never enables a disabled app. If another upgrade lands between the facade's read and its write, the update is refused with an `InvalidOperationException` rather than rolling that upgrade back; an upgrade through `InstallAsync` is pinned the same way. |
| `GetCapabilitiesAsync()` | An advisory, default-deny probe of what the caller may do. It never grants anything; every verb authorizes independently. |

Lifecycle results report the slug, version, resulting `AppLifecycleState` and whether
anything changed. A lifecycle mutation only ever returns `Installed`, `Enabled`,
`Disabled` or `Uninstalled`; a failed mutation throws (see
[No physical ids on the wire](#no-physical-ids-on-the-wire)) rather than returning a
failure state. The wire state adds two inspection-only values: `NotInstalled`,
returned only by `DescribeAsync` for an app available from its source but not
installed, and `Failed`, reported by `DescribeAsync` and `ListAsync` when the
recorded outcome of a live install's last activation run failed (a caller error such
as an invalid transition does not count). A disabled install
is never reported as `Failed` merely because it is disabled.

## Authorization

Every verb except the capability probe requires `LatticeOperation.AppInstall` over
`LatticeScope.ClusterWide()`, enforced through the shared access gate before any
registry or source metadata is read. This includes the read verbs: the registry
holds ceilings, consent and role bindings, so seeing an app's install is itself a
control-plane capability. A key-filtered allow is refused, because the capability is
not attached to a key. `AppInstall` is excluded from `LatticeAuthOperations.All`, so a
whole-data-plane grant never confers it.

Grant it explicitly:

```csharp verify
var rule = new LatticeAuthorizationRule(
    "platform-app-operators",
    LatticeSubjectSelector.Group("app-operators"),
    LatticeScope.ClusterWide(),
    LatticeOperation.AppInstall,
    LatticeEffect.Allow);
```

Each call runs in the caller's **active tenant**. With tenancy off that is the
default tenant, so installs are per-cluster; with tenancy on, each tenant installs
and governs its own copy of an app.

## Consent scopes

The ceiling's exception scopes travel as `AppExceptionScope`, a logical shape rather
than a raw authorization scope, so a request never carries a composed physical id.
Exactly one target is valid:

- `App` plus `Tree` - another app's local tree, for example a cross-app
  subscription source; or
- `AdoptedTreeId` alone - a pre-app physical tree the manifest adopts.

`Kind` (`Tree`, `Key` or `Prefix`) and `KeyOrPrefix` narrow the scope. There is no
all-trees wildcard: an app ceiling can never approve a cluster-wide grant. Invalid
shapes are rejected before anything changes.

## No physical ids on the wire

Responses echo app slugs and **app-local** tree names only. Composed physical ids
(`a/{app}/{tree}`, `t/{tenant}/a/{app}/{tree}`) never appear in a response, and
exception messages are sanitised before they cross the facade: a composed id is
rewritten to its app-local name (or `{app}:{tree}` for another app), the tenant
segment is stripped, and inner exceptions are dropped. The whole inner and aggregated
exception graph is inspected, and a graph too large to inspect fully is treated as
carrying a composed id and replaced. Exception categories are
preserved for transports: invalid input is an `ArgumentException`, an unknown app or
version is a `KeyNotFoundException`, a denied call is a
`LatticeAuthorizationDeniedException`, and any other failure is an
`InvalidOperationException`.

## See also

- [Installable apps](../lattice.apps/README.md)
- [gRPC binding](../lattice.api.apps.grpc/README.md)
- [App MCP tools](../lattice.api.mcp.apps/README.md)
- [Authorization](../lattice.auth/README.md)
- [Sample: installable apps](../../samples/InstallableApps/README.md)

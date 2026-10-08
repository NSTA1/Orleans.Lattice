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

`AddLatticeAppsApi` registers `ILatticeAppsControl` as a singleton, and the same
facade as the `ILatticeAppRoleBindings` singleton (see
[Re-binding roles](#re-binding-roles)). Calling it before `AddLatticeApps` fails
fast.

## Verbs

| Verb | Behaviour |
|---|---|
| `InstallAsync(AppInstallRequest)` | Installs an exact source version with role-to-group bindings and an explicit ceiling; every binding must name a role the manifest declares. Installation does not enable the app. Installing a different version over a live install upgrades it in place, keeping its lifecycle state, and re-applies an enabled app. Installing the version already installed is refused; change consent with `UpdateConsentAsync` instead. A request that carries `ExpectedManifestDigest` is refused, with nothing recorded, unless the manifest resolved at commit still has that digest (see [Pinning the reviewed manifest](#pinning-the-reviewed-manifest)). |
| `EnableAsync(slug)` | Runs the activation pipeline: validates the manifest, checks it against the ceiling, re-verifies the app's tree ownership, enrols its declared replication, provisions trees and grants roles. An excess fails activation until re-consented. |
| `DisableAsync(slug)` | Withdraws the app's grants; trees and data stay. |
| `UninstallAsync(slug)` | Withdraws the app's grants, unenrols the replication it enrolled and soft-deletes its structural trees; adopted trees are untouched, and their ownership claims are released. It never purges data itself, but each soft-deleted tree is purged by the core once its soft-delete window elapses, unless the app is installed and enabled again first. |
| `ListAsync()` | Summaries of the installs in the caller's tenant, including uninstalled records. |
| `DescribeAsync(slug, version?)` | The manifest's requested capabilities (trees, roles with operations and scopes, subscriptions, MCP tools, replication and schema declarations) and its presentation and UI declarations, plus the install's state, provenance, bindings and ceiling. Works before installation, without loading app code; returns `null` for an unknown app or version. Its `ManifestDigest` identifies the described manifest for an install to pin. |
| `GetConsentAsync(slug)` | The ceiling pinned to the installed version and the consented bridge grants, or `null` when the app is not installed. |
| `UpdateConsentAsync(AppConsentUpdate)` | Replaces the whole ceiling for the explicitly named installed version, and the consented bridge grants too when `BridgeGrants` is set (`null` leaves them unchanged), then re-applies an enabled app so a reduced ceiling cannot leave stale authority. If that re-application fails, the failure is thrown with a note that the consent itself was recorded; a failure the consent or manifest causes, such as a ceiling excess, also withdraws the app's grants. Never enables a disabled app. If another upgrade lands between the facade's read and its write, the update is refused with an `InvalidOperationException` rather than rolling that upgrade back; an upgrade through `InstallAsync` is pinned the same way. |
| `GetCapabilitiesAsync()` | An advisory, default-deny probe of what the caller may do. It never grants anything; every verb authorizes independently. |

### Pinning the reviewed manifest

`InstallAsync` resolves the manifest again when it commits, and a fresh install
consents to the bridge grants that manifest requests. To make sure the install
consents to what the operator actually reviewed, every `AppDescriptor` - from
`DescribeAsync` and from the catalogue's `DescribeFromSourceAsync` - carries a
`ManifestDigest`: the SHA-256, as lower-case hex, of the described manifest and the
provenance its source vouched for. Pass it back as
`AppInstallRequest.ExpectedManifestDigest`. The install or upgrade is then refused
with an `InvalidOperationException`, before anything is recorded, when the manifest
resolved at commit has a different digest - for example because a dynamic source
added a bridge operation, a role or a tree in between. A malformed digest is an
`ArgumentException`.

A request without a digest is not pinned, which is how a client written before the
pin behaves. That is safe with the sources that ship today, which serve fixed
manifests, but a client that reviews before it installs should always send the
digest. The [Explorer](../lattice.explorer/README.md) does. The digest is computed
by the server for the server, so an install whose review and commit straddle a
server upgrade can be refused; review it again.

### Re-binding roles

`ILatticeAppRoleBindings` is a separate contract beside `ILatticeAppsControl`, so the
control contract is unchanged. Its one verb,
`UpdateRoleBindingsAsync(AppRoleBindingsUpdate)`, replaces every role-to-group binding
of an installed app and returns an `AppRoleBindingsReport`: the slug, the installed
version, the recorded bindings and the lifecycle state.

- **Full replacement, group-only.** The update names the slug, the exact installed
  version and the complete bindings. A role it leaves out ends up bound to no group.
  Each binding must name a role the installed manifest declares, no role may appear
  twice, and a role is bound to a membership group, never to a user.
- **Pinned.** A version mismatch is refused. The change is also pinned to the install
  revision just read, so a concurrent re-binding, consent update, upgrade or enable
  refuses this one instead of being rolled back. An install whose consent was never
  recorded for the installed version must be re-consented first.
- **Nothing else moves.** The consent, the ceiling, the bridge consent and the
  lifecycle state are kept, and a disabled or merely installed app is never enabled.
- **Re-applied when enabled.** An enabled app is re-applied, so its compiled role
  rules are replaced and a removed binding leaves no stale grant. If that fails, the
  failure is thrown with a note that the bindings themselves were recorded.

It authorizes `AppInstall` over the cluster-wide scope before reading anything, like
every control verb (see [Authorization](#authorization)), and it follows the same
[no physical ids](#no-physical-ids-on-the-wire) rule.

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

A failed activation run throws `InvalidOperationException` naming the operation,
the `AppActivationFailure` value and the first diagnostic, for example
`The enable of app 'orders' failed (ReplicationModeChangeRejected): ...`. That
covers `EnableAsync`, `DisableAsync` and `UninstallAsync`, and the re-application an
`UpdateConsentAsync` (or an upgrade) runs on an enabled app; there the message is
prefixed to say that the consent or upgrade itself was recorded before re-applying
failed. The replication failures
(`ReplicationModeChangeRejected`, `ReplicationPreconditionFailed`,
`ReplicationEnrolmentFailed`) are described under
[Replication intent](../lattice.apps/README.md#replication-intent); each keeps the
app's existing rules in place, and a mode conflict is detected before any enrolment
changes. `DescribeAsync` then reports the install as `Failed` until a later run
succeeds.

An install or upgrade whose trees are owned by another install, or that would take
over a tree it may not own, is refused with `InvalidOperationException` naming the
tree by its app-local name and the reason, for example
`Could not install app 'orders' (TreeOwnershipConflict): ...`, and it does not take
effect. An enable or reconcile that finds such a conflict fails the same way with
`AppActivationFailure.TreeOwnershipConflict`. `DescribeAsync` reports the conflicts
before installation: each `AppTreeDescriptor` carries an `OwnershipConflict` message
when the caller's tenant could not own that tree, or `null` when it could. See
[Tree ownership](../lattice.apps/README.md#tree-ownership) for the rules.

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

Responses echo app slugs, **app-local** tree names and, for an adopted tree, the
pre-app tree id the manifest or ceiling names. Composed physical ids
(`a/{app}/{tree}`, `t/{tenant}/a/{app}/{tree}`) never appear in a response, and
exception messages are sanitised before they cross the facade: an exception whose
message, or any inner or aggregated exception's message, carries a composed id is
replaced by one whose id is rewritten to its app-local name (or `{app}:{tree}` for
another app), with the tenant segment stripped and the inner exceptions dropped. A
graph too large to inspect fully is treated as carrying a composed id and replaced.
Exception categories are
preserved for transports: invalid input is an `ArgumentException`, an unknown app or
version is a `KeyNotFoundException`, a denied call is a
`LatticeAuthorizationDeniedException`, a denied tenant resolution is a
`LatticeTenantAccessDeniedException`, and a failed precondition or activation is an
`InvalidOperationException`. A replaced exception also keeps a cancellation or timeout
category, and becomes an `InvalidOperationException` when it had any other type.

## Catalogue, workspace and bridge

Beside the control facade, the package implements three more contracts from
`Orleans.Lattice.Api.Apps`. They serve the [Explorer](../lattice.explorer/README.md)'s
Apps area and the untrusted app UIs it frames, and any other client can use them.
`AddLatticeAppsApi` registers `ILatticeAppCatalog` and `ILatticeAppWorkspace` beside
`ILatticeAppsControl`. `AddLatticeAppBridgeApi` registers `ILatticeAppBridge`, and
registers the control facade too:

```csharp verify
using Orleans.Lattice.Apps;
using Orleans.Lattice.Api.Apps;

siloBuilder.AddLatticeApps();
siloBuilder.AddLatticeAppBridgeApi(options => options.RateLimitPermitLimit = 200);
```

| Contract | Who may call it | What it serves |
|---|---|---|
| `ILatticeAppCatalog` | Callers holding `AppInstall` over `LatticeScope.ClusterWide()`, the same gate as the control facade | The configured app sources (`ListSourcesAsync`); what each source offers, joined with the active tenant's installs (`ListAvailableAsync`, filtered by source key, text, and `All`, `Installed`, `Available` or `Updates`); a pre-install description of an exact source version (`DescribeFromSourceAsync`); the pre-install icon (`GetIconAsync`); and an advisory probe (`GetCapabilitiesAsync`) that answers whether the caller may use the catalogue rather than refusing it. |
| `ILatticeAppWorkspace` | Any caller who holds at least one role of an enabled install in the active tenant: a group it belongs to is bound to the role, and no deny takes the role away | "Your apps" (`ListMyAppsAsync`), a sanitised description of one of them (`DescribeMyAppAsync`), its icon, and the digest-verified assets of the **installed** version's UI bundle (`GetUiAssetAsync`). |
| `ILatticeAppBridge` | Per operation (see below) | Get, scan, set and delete on an app's own logical trees, on behalf of that app's UI. |

A caller that fails the gate learns nothing. The catalogue refuses the call before
it reads any source or the registry. The workspace answers as if the app did not
exist. The workspace description excludes the ceiling, the approved exception
scopes, consent history, role-to-group bindings and every physical tree id: those
remain behind `AppInstall` on the control facade. Its `Ui.Bridge` is not the
manifest's bare request: it carries only the grants the operator consented to that
the installed manifest still requests, which is exactly what the bridge admits, so
a client launching the UI is never offered an unconsented grant.

When the same slug is offered by more than one source, the catalogue lists one row
per source. An install names its source with `AppInstallRequest.SourceKey`. Without
a key, an ambiguous slug is refused rather than resolved to either source. `Updates`
means a newer version from the **same** source the install came from.

### The bridge

`ILatticeAppBridge` is the single place where data access by an app UI is enforced.
A target is `AppBridgeTarget(AppSlug, InstallRevision, LogicalTree)`. No overload
accepts a physical tree id. Each call runs these steps in order, and each fails
closed:

1. **The install.** It must be enabled in the caller's active tenant with its
   ceiling pinned to its version, and `InstallRevision` must match. Every recorded
   transition advances the revision, so a frame launched before an upgrade, a consent
   update, a role re-binding, a disable or an uninstall stops working,
   and the call is denied exactly as for an app that does not exist.
2. **Bridge consent.** The operation must be covered for the logical tree both by
   the install's **consented** bridge grants (see
   [presentation and UI](../lattice.apps/README.md#presentation-and-ui)) and by the
   installed manifest's own request.
3. **Tree resolution.** This happens on the server. A declared tree composes to
   `a/{slug}/{tree}`, and then per tenant. An adopted tree uses its adopted id. An
   undeclared name is not found.
4. **App-owned grants only.** The caller must match an app-owned compiled rule for
   this slug (`app:{slug}:` ids) that allows the concrete operation (`Read`,
   `RangeRead`, `Write` or `Delete`) on the concrete key or prefix, with the ceiling
   re-checked. The caller's other rules are deliberately not consulted. This is
   what stops a user's broad operator rights from flowing into an app's UI. A reader
   who is also a cluster operator still cannot write through a viewer role. A scan
   needs `RangeRead`, because that is what the data path enforces. A role whose UI
   scans must therefore request `RangeRead`, and the ceiling must allow it.
5. **Execution.** The call runs under the caller's own identity and tenant, so
   ordinary data-plane authorization also applies.

Before these steps, the request is validated and the caller is resolved and rate
limited. Values are bounded (64 KiB each, keys at most 1024 characters, and a scan
page of at most 200 entries whose encoded response is at most 1 MiB). Each caller,
active tenant and app slug is rate limited in a fixed window - by default 100
requests per second, set through `LatticeAppBridgeOptions` (see
[Configuration reference](#configuration-reference)) - and a request over the limit
is refused as `Unavailable` before any authorization or data access, so a retry in a
later window can succeed. Failures are the closed `AppBridgeFailure` set: `Denied`,
`NotFound`, `Invalid`, `TooLarge`, `Conflict` and `Unavailable`. They are carried by
`AppBridgeException` with a fixed, sanitised message.

## Configuration reference

### `LatticeAppBridgeOptions`

Configured through the `AddLatticeAppBridgeApi(options => ...)` delegate.

| Option | Type | Default | Meaning |
|---|---|---|---|
| `RateLimitPermitLimit` | `int` | 100 (`DefaultRateLimitPermitLimit`) | The requests each caller, active tenant and app slug may make in one window. Must be at least 1. |
| `RateLimitWindow` | `TimeSpan` | 1 second (`DefaultRateLimitWindow`) | The length of one fixed window, which starts at that partition's first request. Must be positive. |

An invalid value fails when the bridge is first resolved, not at registration. The
limiter tracks at most 10,000 partitions at once; when the table is full and holds no
expired window, a request from a new partition is refused as `Unavailable`.

## See also

- [Public API](api.md), [configuration](configuration.md), and [architecture](architecture.md)

- [Installable apps](../lattice.apps/README.md)
- [gRPC binding](../lattice.api.apps.grpc/README.md)
- [App MCP tools](../lattice.api.mcp.apps/README.md)
- [Authorization](../lattice.auth/README.md)
- [Sample: installable apps](../../samples/InstallableApps/README.md)

# Orleans.Lattice.Apps

Opt-in **installable apps** for Orleans.Lattice: an app declares its trees, roles,
replication intent, change-feed subscriptions and MCP tools in a manifest, and the
platform maps that manifest onto the existing data and security planes without a
second enforcement engine.

## What is it?

An **app** is an installable unit of state and behaviour - a CRM, a work queue, or
a repository-memory store such as RepoContext - whose footprint on the cluster is
declared up front in a JSON **manifest**. Installing the app records who may use it
and what it may do; enabling it provisions its trees and grants its roles; disabling
or uninstalling it takes both away again.

The central design point is that an app's role-to-permission mapping is a
**compiler, not an enforcement engine**. Roles in the manifest compile into ordinary
`LatticeAuthorizationRule` records written through the authorization policy store.
The existing access gate from [`Orleans.Lattice.Auth`](../lattice.auth/README.md)
remains the single enforcement seam: an app never evaluates a permission itself, and
nothing about the data path changes when an app is installed.

It is a **companion package**. A host that does not reference it pays nothing, and
core carries only two constants for the app tree namespace and the registry tree.

The package is the engine. Operators reach it through companion packages:

- [`Orleans.Lattice.Api.Apps`](../lattice.api.apps/README.md) - the transport-agnostic
  control facade (`ILatticeAppsControl`): install, enable, disable, uninstall, list,
  describe, and consent management.
- [`Orleans.Lattice.Api.Apps.Grpc`](../lattice.api.apps.grpc/README.md) - the
  code-first gRPC binding and client for that facade.
- [`Orleans.Lattice.Api.Mcp.Apps`](../lattice.api.mcp.apps/README.md) - every enabled
  app's MCP tools on the single MCP endpoint, namespaced by app slug.

## Core properties

- **Manifest before code.** The manifest is an embedded JSON resource, parsed and
  validated without loading or running any app code, so an operator can review what
  an app requests before extending any trust to it.
- **Per-tenant when tenancy is on, per-cluster otherwise.** App trees are named
  `a/{app}/{tree}` and composed through the ordinary tenant resolution seam, which
  yields `a/{app}/{tree}` with tenancy off and `t/{tenant}/a/{app}/{tree}` with
  tenancy on. The tenant stays the outer axis, so prefix scans, quota attribution
  and WAL placement keep working unchanged.
- **Groups only.** App roles bind to membership **groups** at install time, never to
  individual users. Role hierarchy is expressed by nesting groups in
  [`Orleans.Lattice.Membership`](../lattice.membership/README.md), so the manifest
  has no inheritance construct and compilation is a flat, total function.
- **Capability ceiling.** Every install records a ceiling of allowed operations and
  operator-approved exception scopes, pinned to the installed version. Every
  compiled rule is checked against it; a manifest that asks for more fails
  activation until it is re-consented. Nothing is silently clamped.
- **Owned, replaceable rules.** Compiled rules carry deterministic ids under the
  `app:` prefix, and the policy store rejects operator writes and deletes of those
  ids. Reconciliation replaces the app's whole owned rule set, so removing a role
  leaves no stale grant.
- **Safe lifecycle.** Uninstall and tree drops soft-delete app trees on their
  retention window; physical purge still needs the separate `TreeLifecycle`
  capability, so uninstalling an app can never destroy data.
- **Never wedges a silo.** A bad manifest, an over-ceiling request or a missing
  prerequisite fails that app's activation with a structured result. Silo startup
  is never affected.

## Quick start

Register the apps engine after `AddLattice`, membership and authorization, then
register each app that ships in the image:

```csharp verify
using Orleans.Lattice.Apps;

siloBuilder.AddLatticeMembership();
siloBuilder.AddLatticeAuth(options =>
{
    options.DefaultEffect = LatticeEffect.Deny;
    options.BootstrapAdministrators.Add("platform-operator");
});

siloBuilder.AddLatticeApps();
siloBuilder.AddLatticeApp(
    "crm",
    System.Reflection.Assembly.GetExecutingAssembly(),
    "Contoso.Crm.crm.app.json");
```

`AddLatticeApp` makes an app **installable**, not installed. The `slug` must match
the manifest's `identity.slug`, and the resource name is the fully qualified name of
the embedded manifest in the given assembly (normally the app's own assembly).
Installing and enabling it is an operator action, normally through the
[control facade](../lattice.api.apps/README.md) or its
[gRPC binding](../lattice.api.apps.grpc/README.md).

## The manifest

The manifest is canonical JSON, embedded in the app's assembly. A matching JSON
Schema ships inside the package (`AppManifestResources.GetJsonSchema()`, and as
`schema/app-manifest.schema.json` in the NuGet package) for offline authoring. Unknown
properties are rejected.

```json
{
  "identity": { "slug": "crm", "version": "1.0.0" },
  "trees": [
    { "name": "contacts", "softDeleteDuration": "7.00:00:00" },
    { "name": "search-index", "rebuildable": true }
  ],
  "roles": [
    {
      "name": "reader",
      "operations": ["Read", "RangeRead"],
      "scopes": [{ "tree": "contacts" }]
    },
    {
      "name": "editor",
      "operations": ["Read", "RangeRead", "Write", "Delete"],
      "scopes": [{ "tree": "contacts" }]
    }
  ],
  "subscriptions": [{ "name": "reindex", "tree": "contacts" }],
  "mcpTools": [
    { "name": "find_contact", "description": "Finds a contact by name.", "role": "reader" }
  ]
}
```

| Section | Meaning |
|---|---|
| `identity` | `slug` (`^[a-z][a-z0-9-]{1,30}$`; `_` is reserved as the namespace separator), a Semantic Versioning 2.0 `version`, and descriptive `provenance`. |
| `trees` | App-local tree names plus optional physical shape pins (`shardCount`, `virtualShardCount`, `maxLeafKeys`, `maxInternalChildren`, `walPartitions`), a `softDeleteDuration`, a `rebuildable` flag, and an optional `adoptedTreeId`. |
| `roles` | A role name, its `operations` as an array of `LatticeOperation` member names, and one or more scope templates. |
| `replication` | Optional per-tree merge mode, merged additively into replication enrolment when the replication add-on is registered. |
| `schema` | Optional per-tree schema family and envelope version binding. |
| `subscriptions` | Change-feed observations of the app's own trees or another app's trees. |
| `mcpTools` | App-local MCP tool names, descriptions, and the role each tool requires. |

### Trees

A tree declaration names an **app-local** tree. The physical tree is the
structural `a/{app}/{name}`, composed per tenant as described above. Omitted shape
pins inherit the host's defaults. `virtualShardCount` is fixed when the tree is
created, so a manifest upgrade may not change it or drop the pin.

`rebuildable: true` marks a tree whose contents can be re-derived rather than
restored, which drives restore-versus-rederive decisions for app-scoped backup.

`adoptedTreeId` lets a first-party app adopt a tree that existed before the app
concept (the RepoContext pilot uses this for its legacy tree names). An adopted
tree sits outside the app namespace, so it is **never** granted structurally: it
must appear in the install ceiling's approved exception scopes, and uninstall never
soft-deletes it. Adopted ids may not use the structural `a/` prefix or a reserved
prefix (`_lattice_`, `sys-`, `t/`), and each may be adopted only once per manifest.

### Roles and scopes

`operations` is a non-empty array of distinct `LatticeOperation` names such as
`["Read", "RangeRead"]`. Numbers, `None`, and unknown names are rejected. The
scopeless capabilities `Telemetry` and `AppInstall` are rejected, because a role
scope always names a tree. Other tree-scoped operations (including `Admin`,
`Backup`, `Restore`, `SchemaAdmin`, `Replication` and `TreeLifecycle`) are
expressible, but, like everything else, must be inside the operator's ceiling.

Each scope template names an app-local tree declared by the app and an optional
`kind` (`Tree`, `Key`, or `Prefix`) with its `keyOrPrefix`. A scope may instead
name another app's tree with `app`; such a scope, like one over an adopted tree,
requires an approved exception.

### Validation

`AppManifestParser.Parse` and `AppManifestValidator.Validate` return an
`AppManifestResult` carrying either a manifest or a list of `AppManifestError`
values (a code, a JSON path and a message). Validation never throws for bad content;
`AppManifestResources.Load(assembly, resourceName)` reads an embedded manifest the
same way. When a previous manifest is supplied, validation also enforces upgrade
rules: the slug cannot change and an existing tree's `virtualShardCount` cannot
change.

## Install, consent and the ceiling

An install is recorded in the **app registry**, a reserved system tree
(`sys-app-registry`) keyed `{tenantId}/{appSlug}`. With tenancy off every install
uses the default tenant, which is the per-cluster behaviour. Each record carries the
app's slug, version, provenance, isolation context (tenant and cluster), the
capability ceiling and the version it was consented for, the role-to-group bindings,
and the lifecycle state.

- **Role bindings** (`AppRoleBinding`) map each manifest role to one membership group
  id.
- **The ceiling** (`AppCapabilityCeiling`) holds `AllowedOperations` and
  `ApprovedExceptionScopes`. Scopes inside the app's own `a/{app}/` namespace are
  covered structurally, so the common case needs no exception at all
  (`AppCapabilityCeiling.Structural(operations)`). Exceptions are tenant-local
  scopes - another app's `a/{other}/{tree}` or an adopted tree id - approved by the
  operator.
- **Version pinning.** The ceiling is pinned to the version it was consented for.
  Upgrading to a new version requires a new ceiling, and enabling an app whose
  ceiling was consented for a different version fails.

The registry is **control-plane read isolated** exactly like the tenant registry: a
data-plane read grant, including a cluster-wide all-trees wildcard, cannot read
`sys-app-*`. Every lifecycle transition (install, upgrade, enable, disable,
uninstall) requires `LatticeOperation.AppInstall`, a scopeless cluster-wide
capability granted over `LatticeScope.ClusterWide()` and excluded from
`LatticeAuthOperations.All`.

The lifecycle states are `Installed`, `Enabled`, `Disabled` and `Uninstalled`.
Uninstall keeps the record in the `Uninstalled` state; it does not delete data.

## Activation

`IAppActivationPipeline` applies an installed app to the cluster. Its verbs run
serialized per app, authorize `AppInstall`, and return an `AppActivationOutcome`
describing success or a structured failure (`AppActivationFailure`) with diagnostics.

**Enable** resolves the installed version's manifest from the app source, validates
it, compiles its roles against the pinned ceiling and bindings, provisions the app's
structural trees with their declared shape (recovering a tree soft-deleted by an
earlier uninstall), persists the compiled rule set, and marks the app `Enabled`.

**Disable** removes every rule the app owns and marks it `Disabled`; its trees and
data stay in place.

**Uninstall** removes the owned rules, soft-deletes the app's structural trees on
their configured `SoftDeleteDuration`, and marks the app `Uninstalled`. Adopted
trees are never deleted. Physical purge is not part of uninstall.

**Reconcile** re-applies an enabled app, for example after a consent change or an
upgrade. A manifest upgrade that drops a tree soft-deletes that tree.

Two prerequisites are enforced fail-closed. Without membership, every caller
resolves to the anonymous subject with no groups and every app rule would be
unmatchable, so activation fails with a diagnostic naming the App to Auth to
Membership chain. Without the authorization policy store the rules cannot be
persisted, and activation fails the same way.

When `LatticeAppsOptions.ReconcileOnStartup` is `true` (the default), a background
service reconciles every enabled app when the silo starts, retrying with
back-off (`StartupRetryDelay` up to `StartupRetryMaxDelay`). A failure is recorded
against the app and never stops the host.

### Replication intent

When the replication add-on is registered, the trees an in-image app declares in
its `replication` section are merged **additively** into
`LatticeReplicationOptions.ReplicatedTrees`; an operator's existing entry is never
overwritten. Because only apps registered in the image are known at configuration
time, the merge enrols the default-tenant tree names. Without the replication
add-on, replication intent is ignored.

## Compiled rules

`AppRoleCompiler.Compile` is a pure function from a manifest, a tenant, the role
bindings and the ceiling to an `AppRuleCompilation`. On success it holds the full
set of `LatticeAuthorizationRule` records; on failure it lists every excess
(`AppCeilingExcess`) and every binding that names an unknown role, and emits no
rules. A declared role with no binding emits nothing and is reported as a
diagnostic.

- Every rule's subject is `LatticeSubjectSelector.Group(groupId)`.
- Every rule id is `app:{slug}:{role}:{hash}`, where the hash is derived from the
  slug, role, group, scope kind, tenant-composed tree id and key or prefix, so ids
  are stable across runs and processes. `LatticeAppRuleIds.Prefix` (`app:`) and
  `LatticeAppRuleIds.IsAppOwned` expose the owned namespace.
- `AppRoleCompiler.ComputeDiff` computes the rules to upsert and delete to replace
  an app's stored owned set with a freshly compiled one; rules outside the app's
  `app:{slug}:` prefix are never touched.

The activation pipeline persists the result through
`ILatticeAuthorizationPolicyStore` under system origin. A direct write or delete of
an `app:` rule id that does not run under system origin is rejected with
`LatticeAppOwnedRuleException`: to change what an app's users may do, author a
separate rule outside the prefix (for example a narrower deny), or change the
app's bindings or manifest and let the compiler reconcile.

## App sources

`IAppSource` resolves an app slug and optional version to its manifest, provenance
and an activation handle. Resolving never runs app code; the handle is only invoked
when the app is activated. Unknown slugs, version mismatches, invalid manifests,
identity mismatches and duplicate registrations are returned as structured results
(`AppSourceStatus`), never thrown.

`InImageAppSource` is the only implementation in this version. It resolves apps
that ship in the image by ordinary package reference and are registered with
`AddLatticeApp`; each manifest is parsed once and cached. Its provenance is
`in-image` with a first-party publisher. The seam is designed so that a future
runtime source - one that acquires apps after deployment - is a provider swap; the
XML documentation on `IAppSource` states what such a source must additionally
guarantee (signature verification against a pinned publisher key, allow-listing by
slug, version and digest, and manifest-before-code).

## Change-feed subscriptions

A manifest `subscriptions` entry observes committed mutations on one of the app's
trees, or on another app's tree when it names `app`. The app supplies a handler:

```csharp verify
using Orleans.Lattice.Apps;

public sealed class ReindexHandler : IAppChangeFeedHandler
{
    public Task HandleAsync(
        AppSubscriptionContext context,
        LatticeMutation mutation,
        CancellationToken cancellationToken)
    {
        // React to a committed write on context.Tree.
        return Task.CompletedTask;
    }
}
```

and registers it for the subscription by app slug and subscription name with
`AddLatticeAppSubscriptionHandler<ReindexHandler>(slug, "reindex")`.

Subscriptions are realised through the core `IMutationObserver` seam. A routing
table from tree id to subscribers is rebuilt whenever the set of enabled apps
changes, so a mutation on a tree no app observes allocates nothing. Handlers run
inline on the write path; a handler that throws is logged and skipped, and the write
is never failed. Disabling or uninstalling an app stops delivery immediately.

A subscription to the app's own trees needs no consent. A cross-app subscription
(and a subscription to an adopted tree) must be covered by an approved exception
scope in the ceiling, or that app's subscriptions fail to activate with a message
naming the observed app. Cross-tenant observation is not introduced: a subscription
only sees trees composed for its own install's tenant.

## Security

- `AppInstall` is required for every lifecycle transition and every control-facade
  verb, and is never part of `LatticeAuthOperations.All`.
- The registry (`sys-app-*`) is control-plane read isolated; the evaluator excludes
  it from all-trees wildcards, and an unmatched request fails closed even under a
  permissive default effect.
- App rules are ordinary rules evaluated by the existing gate. The data path, the
  gate and the enforcement helpers are unchanged by this package.
- App-owned rule ids cannot be edited or deleted except under system origin.
- Physical tree ids are kept out of the control facade's responses and exception
  messages (see the [facade](../lattice.api.apps/README.md)). They remain visible in
  telemetry, storage accounting and backup artifacts, which require the `Telemetry`,
  `Backup` and `TreeLifecycle` capabilities an app-scoped grant never confers.

## Configuration reference

### `LatticeAppsOptions`

| Option | Default | Meaning |
|---|---|---|
| `ReconcileOnStartup` | `true` | Reconcile every enabled app in the background when the silo starts. |
| `StartupRetryDelay` | 250 ms | Initial back-off between startup reconcile attempts. |
| `StartupRetryMaxDelay` | 30 s | Maximum back-off between startup reconcile attempts. |

## See also

- [Control facade](../lattice.api.apps/README.md) and its
  [gRPC binding](../lattice.api.apps.grpc/README.md)
- [App MCP tools](../lattice.api.mcp.apps/README.md)
- [Authorization](../lattice.auth/README.md) and
  [membership](../lattice.membership/README.md)
- [Tenancy](../lattice.tenancy/README.md)
- [RepoContext as the pilot app](../lattice.api.mcp.repocontext/README.md)
- [Sample: installable apps](../../samples/InstallableApps/README.md)
- [Orleans.Lattice](../../README.md)

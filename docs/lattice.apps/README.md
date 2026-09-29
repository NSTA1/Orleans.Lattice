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
installing one changes nothing about how the data path is authorized.

It is a **companion package**. A host that does not reference it pays nothing: core
carries only the `LatticeOperation.AppInstall` flag and two internal constants naming
the app tree namespace and the app-registry tree prefix.

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
- **Recoverable lifecycle.** Uninstall and tree drops only soft-delete app trees,
  on each tree's `softDeleteDuration` (or the host's `SoftDeleteDuration`). The data
  stays recoverable until that window elapses, when the core's deferred purge removes
  it; uninstall never purges immediately, and an immediate purge still needs the
  separate `TreeLifecycle` capability.
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
| `replication` | Optional per-tree merge mode. Each install enrols these trees in the runtime replication configuration when it is activated - see [Replication intent](#replication-intent). |
| `schema` | Optional per-tree schema family, envelope version and `strictIngest` flag. Validated and reported by the control facade's describe; activation does not apply it to the tree. |
| `subscriptions` | Change-feed observations of the app's own trees or another app's trees. |
| `mcpTools` | App-local MCP tool names, descriptions, and the role each tool requires. |

### Trees

A tree declaration names an **app-local** tree. The physical tree is the
structural `a/{app}/{name}`, composed per tenant as described above. Omitted shape
pins inherit the host's defaults. `virtualShardCount` is applied when the install
first registers the tree, and a manifest upgrade may not change it or drop the pin.
A resize carries the declared slot count over to the resized copy and a reshard keeps
it, and a reshard target cannot exceed it (see
[Virtual shard space](../lattice/configuration.md#virtual-shard-space-constant)).

`rebuildable: true` marks a tree whose contents can be re-derived rather than
restored. It is descriptive metadata: the control facade's describe reports it, and no
backup or restore path in this version acts on it.

`adoptedTreeId` lets a first-party app adopt a tree that existed before the app
concept (the RepoContext pilot uses this for its legacy tree names). An adopted
tree sits outside the app namespace, so it is **never** granted structurally: it
must appear in the install ceiling's approved exception scopes, and uninstall never
soft-deletes it. Adopted ids may not use the structural `a/` prefix or a reserved
prefix (`_lattice_`, `sys-`, `t/`), may not be the cluster-wide sentinel `*`, may not
carry leading or trailing white space or control characters, may be at most 1024
characters, and each may be adopted only once per manifest. Across installs, a tree
can be adopted by only one install at a time; see [Tree ownership](#tree-ownership).

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
requires an approved exception, and compiles only while that app is installed and
owns the tree (see [Tree ownership](#tree-ownership)). No exception can approve a scope on the cluster-wide
sentinel `*` or on a `_lattice_`, `sys-` or `t/` tree, and no ceiling can grant a role
`Telemetry` or `AppInstall`: the role compiler reports either as an excess even for a
manifest that skipped validation.

### Validation

`AppManifestParser.Parse` and `AppManifestValidator.Validate` return an
`AppManifestResult` carrying either a manifest or a list of `AppManifestError`
values (a code, a JSON path and a message). Validation never throws for bad content;
`AppManifestResources.Load(assembly, resourceName)` reads an embedded manifest the
same way. When a previous manifest is supplied, validation also enforces upgrade
rules: the slug cannot change and an existing tree's `virtualShardCount` cannot
change.

Parsing and validation are bounded, because a runtime-installed manifest is
untrusted input: a manifest larger than 1 MiB (characters of text, or bytes of a
stream) is refused before it is deserialized with code `too-large`, each section and
the scopes of each role hold at most 256 entries, keys, prefixes, adopted ids, schema
families and provenance fields are at most 1024 characters, and a tool description is
at most 4096. An exceeded bound is reported with code `limit` (an over-long adopted id fails the `adoption` check instead), and an oversized section
is rejected before any per-entry work.

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
- **Compare on version.** `AppRegistryInstallRequest.ExpectedVersion`, when set,
  applies an upgrade only while that version is still installed and otherwise returns
  `ConcurrencyConflict`. The control facade sets it to the version it read on every upgrade and consent update, so a
  consent update racing an upgrade is refused instead of rolling the upgrade back. An
  upgrade made directly through `IAppRegistry` does not re-apply an enabled app's
  grants; call `IAppActivationPipeline.ReconcileAsync` afterwards.

The registry is **control-plane read isolated** exactly like the tenant registry: a
data-plane read grant, including a cluster-wide all-trees wildcard, cannot read
`sys-app-*`. Every lifecycle transition (install, upgrade, enable, disable,
uninstall) requires `LatticeOperation.AppInstall`, a scopeless cluster-wide
capability granted over `LatticeScope.ClusterWide()` and excluded from
`LatticeAuthOperations.All`.

The lifecycle states are `Installed`, `Enabled`, `Disabled` and `Uninstalled`. Install
applies to an absent or `Uninstalled` app, enable to an `Installed` or `Disabled` one,
disable only to an `Enabled` one (disabling an app that was never enabled is an
`InvalidTransition`), and upgrade and uninstall to any live install. Repeating enable,
disable or uninstall when its target state already holds is an idempotent success that
writes nothing; a rejected transition returns an `AppRegistryTransitionResult` carrying
its `AppRegistryTransitionError` instead of throwing. Uninstall keeps the record in the
`Uninstalled` state; the registry transition itself touches no data (the activation
pipeline's uninstall soft-deletes the app's trees, below).

## Tree ownership

Every tree an app uses - its structural `a/{app}/{tree}` trees and every tree it
adopts - belongs to **exactly one install** at a time: a structural tree for its
whole lifetime, including its soft-delete window, and an adopted tree until the
install that adopted it releases it. Two installs can never share a tree, whichever app they are
and whatever the operator approves, so one app's data is never readable or writable
under another app's name.

Ownership is recorded in a third reserved system tree, the **tree ownership ledger**
(`sys-app-trees`), keyed by the tenant-composed tree id. It sits under the same
`sys-app-` prefix as the registry, so it has the same control-plane read isolation
and user-origin write guard. The owner of a tree is an install identity: the tenant,
the app slug, and the publisher recorded from the app's provenance, so a different
publisher shipping the same slug is a different owner.

- **Claimed at install.** Installing or upgrading an app claims every tree its
  manifest declares. A conflict visible before the install is recorded refuses it
  with nothing written. Claims are compare-and-set writes, taken after the record in
  ascending tree-id order, so of two concurrent installs of one tree exactly one
  wins; the loser releases what it took and rolls its record back. When the app
  source cannot supply the version being installed, claiming is left to activation.
- **Re-verified at activation.** Enable and reconcile re-check, and if needed take,
  every claim before any replication, provisioning or grant, so a conflict that
  appeared after install fails activation closed with
  `AppActivationFailure.TreeOwnershipConflict` and the app's rules withdrawn. A tree
  soft-deleted by an earlier uninstall is recovered only once its claim is confirmed
  as this install's.
- **Released on uninstall.** Uninstall, and an upgrade that stops declaring an
  adopted tree, release the adopted claims, so another install can adopt the tree
  afterwards. A structural claim is held until the tree is purged: while an
  uninstalled app's tree is still in its soft-delete window no other install can
  take it, and reinstalling the same app re-attaches to it.

A fresh claim is refused, and the install or activation fails with a
`TreeOwnershipConflict`, when the tree:

| Reason (`AppTreeOwnershipConflictReason`) | Meaning |
|---|---|
| `OwnedByAnotherApp` | Another install in the tenant - a different app, or the same slug from a different publisher - owns it. |
| `PreExistingUnownedTree` | An app's structural tree already exists with no claim: it was created outside any app lifecycle, so it is never taken over. |
| `DerivedTree` | It is a physical copy core created to back another tree (a resize, restore or remediation copy), so it is not a logical tree anyone can own. |
| `AliasTarget` | Its physical backing is another logical tree's alias target, or it is aliased to a tree not derived from it, so owning it would give the same data a second name. |

`IAppRegistry.GetTreeOwnershipConflictsAsync` reports the conflicts an install would
hit without writing anything, and the control facade's `DescribeAsync` reports them
per tree before installation.

**Cross-app scopes need an installed owner.** A role scope or subscription that names
another app's tree compiles only while that app is the installed owner of the tree in
the same tenant, as the ledger records. Otherwise the role scope is reported as a
ceiling excess, so the activation fails closed (`CeilingExceeded`) and every rule the
app owns is withdrawn, not only those over that tree; and the subscription is denied,
so that app's subscriptions fail to activate. The compilers take the owners as an
optional `AppTreeOwnerSnapshot` argument and stay pure. When an app is uninstalled,
every other enabled app in the tenant whose manifest reaches it through a cross-app
scope or subscription is reconciled at once, so none of its grants over the absent
owner's trees survive; its subscriptions stop at the subscription router's next
rebuild from the changed registry.

**Aliasing is bounded by ownership.** The package registers an
[`ITreeOwnershipGuard`](../lattice/tree-registry.md#ownership-bounded-aliasing) backed
by the ledger, so a core alias from logical tree `L` to physical tree `P` is allowed
only when the install that owns `L` also owns `DerivedFrom(P) ?? P` (either side may
be owned by no app). A resize, restore or remediation of an app tree passes, because
its copy is derived from that tree; an alias between two trees no app owns is
unchanged; every other alias that would cross an ownership boundary is refused with
`LatticeTreeOwnershipDeniedException`, for every caller including core maintenance.
The refusal names only the tree ids the caller supplied, never the owning app.

## Activation

`IAppActivationPipeline` applies an installed app to the cluster. Its mutating verbs run
one at a time per tenant app across the cluster, authorize `AppInstall` (system-origin
callers skip the check), and return an `AppActivationOutcome` describing success or a
structured failure (`AppActivationFailure`) with diagnostics. Its `GetStatusAsync` read
is ungated and in-process, so a facade that exposes it must gate it itself.

**Enable** resolves the installed version's manifest from the app source, validates
it, compiles its roles against the pinned ceiling and bindings, re-verifies its
[tree ownership](#tree-ownership) claims, enrols the app's declared
[replication](#replication-intent), provisions the app's structural trees with their
declared shape (recovering a tree soft-deleted by an earlier uninstall), persists the
compiled rule set, and marks the app `Enabled`. If a
consent change landed while the run was in flight, it re-activates against the record
its own transition wrote, so no grant compiled from superseded consent stays live.
Replacing the owned rule set withdraws stale rules before writing new ones, so a
policy-store fault part-way through never keeps a grant the current consent revoked.
When the installed version itself cannot be activated - its manifest cannot be resolved
or validated, its roles exceed the consented ceiling, a binding names an undeclared
role, or it has a tree ownership conflict - any rules left from an earlier
activation are withdrawn; a replication, tree-provisioning or rule-write failure
keeps the existing rules, so a retry is not an outage.

**Disable** removes every rule the app owns and marks it `Disabled`; its trees and
data stay in place, and their [replication](#replication-intent) keeps running.

**Uninstall** removes the owned rules, unenrols the replication the app enrolled,
soft-deletes the app's structural trees on their configured `SoftDeleteDuration`,
and marks the app `Uninstalled`. Adopted trees are never deleted. Physical purge is
not part of uninstall, but each soft-deleted tree is purged by the core's deferred
purge once its soft-delete window elapses; installing and enabling the app again
within the window recovers it.

**Reconcile** re-applies an enabled app, for example after a consent change or an
upgrade. A manifest upgrade that drops a tree soft-deletes that tree, and one that
drops a tree from the `replication` section unenrols it. Reconciling an app in any
other state withdraws its owned rules; reconcile never changes the registry state.

Each run records its outcome, and the manifest whose trees and rules are currently
applied, as the app's `AppActivationStatus` in a second reserved system tree,
`sys-app-activation`, keyed like the registry; a run that cannot read the existing
status leaves it untouched rather than overwrite it, and a failed status write is
only logged. That record is the evidence the
control facade reports as a `Failed` state; a disabled registry state alone is never
read as a failure.

Two prerequisites are enforced fail-closed. Without membership, every caller
resolves to the anonymous subject with no groups and every app rule would be
unmatchable, so activation fails with a diagnostic naming the App to Auth to
Membership chain. Without the authorization policy store the rules cannot be
persisted, and activation fails the same way.

When `LatticeAppsOptions.ReconcileOnStartup` is `true` (the default), a background
service reconciles every enabled app once when the silo starts. Only the registry read
that lists the enabled apps is retried, with a doubling delay from `StartupRetryDelay`
up to `StartupRetryMaxDelay`, until the silo can serve it. Each app's failure is
recorded against the app and logged; it is not retried and never stops the host.

### Replication intent

An app's manifest `replication` section declares which of its trees replicate
across clusters, and under which merge mode. Replication follows the
**installation**, not the image: each tenant's install enrols its own trees as it
is activated, under the tenant-composed ids that install actually uses, so a
tenant's app trees replicate per install and nothing is enrolled merely because
an app is registered in the image.

- **Enable** and **Reconcile** of an enabled app enrol every declared tree that is
  not yet enrolled. Enrolling is idempotent, so a reconcile that finds everything
  in place changes nothing.
- An **upgrade** that drops a tree from the `replication` section unenrols it once
  the new version activates.
- **Disable** leaves replication running: the app's rules are withdrawn, but its
  data keeps converging with peers, so a later enable does not need a re-seed.
- **Uninstall** unenrols the app's trees. Peer data is not purged, and adopted
  trees are not deleted.

Enrolment goes through the replication package's runtime configuration
(`ILatticeReplicationConfigAuthority`), so it needs
`AddLatticeReplication(..., enableRuntimeConfig: true)`; an operator sees app
trees in the [runtime replication configuration](../lattice.replication/runtime-config.md#installed-apps-enrol-through-the-runtime-configuration)
rather than in `LatticeReplicationOptions.ReplicatedTrees`. Without the replication
add-on or its runtime configuration, replication intent is ignored. With tenancy,
each tenant's trees are admitted by the tenant replication isolation gate like any
other tenant tree.

An app only ever unenrols what it enrolled. Every activation records, as part of
the app's `AppActivationStatus` and before any configuration change, which of its
trees it enrolled itself and which were already enrolled by an operator; uninstall
and dropped declarations disable only the former. A tree an operator had already
enrolled - for example a legacy tree the app adopted - keeps replicating after the
app is uninstalled.

Activation checks every declared tree's merge mode before it changes any enrolment,
so a mode conflict changes nothing. A precondition or enrolment failure can surface
part-way through, after some trees are enrolled; the trees a run attempts are
recorded first, so a later reconcile completes the change and an uninstall still
unenrols them. On an enable or reconcile, every replication failure keeps the app's
existing rules in place:

| Failure | Meaning |
|---|---|
| `ReplicationModeChangeRejected` | A declared merge mode differs from the mode the tree is already enrolled under or from the mode the app's previous version declared for it, or the enrolled mode is ambiguous. Replication cannot switch a tree's merge mode in place, so keep the declared mode; for a tree enrolled outside the app, an operator can disable its replication first. |
| `ReplicationPreconditionFailed` | A replication prerequisite is not met, for example the host has no configured cluster id, or the merge mode needs a configured local replica. |
| `ReplicationEnrolmentFailed` | Reading or writing the runtime enrolment failed. The run can be retried with a reconcile. |

A failure at startup reconcile is recorded against the app and never stops the
host.

Enrolling an adopted tree that already holds data does not bootstrap peers from
it. If a peer needs that existing data, run an explicit replication bootstrap for
the tree as an operator; the app supplies no bootstrap source.

## Compiled rules

`AppRoleCompiler.Compile` is a pure function from a manifest, a tenant, the role
bindings, the ceiling and an optional cross-app `AppTreeOwnerSnapshot` to an
`AppRuleCompilation`. On success it holds the full
set of `LatticeAuthorizationRule` records; on failure it lists every excess
(`AppCeilingExcess`) and every binding that names an unknown role, and emits no
rules. A declared role with no binding emits nothing and is listed in
`AppRuleCompilation.UnboundRoles` without failing the compilation.

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
and an activation handle (`IAppActivationHandle`). Resolving never runs app code, and
the activation pipeline in this version never invokes the handle: an in-image app's
code is already present by package reference, so its handle loads nothing. Unknown
slugs, version mismatches, invalid manifests, identity mismatches and duplicate
registrations are returned as structured results (`AppSourceStatus`), never thrown.

`InImageAppSource` is the only implementation in this version. It resolves apps
that ship in the image by ordinary package reference and are registered with
`AddLatticeApp`; each manifest is parsed once and cached. Its provenance is
`in-image`, with the registration's `Publisher` (`first-party` unless the
registration sets another). The seam is designed so that a future
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
table from tree id to subscribers is rebuilt whenever the app registry changes, so
a mutation on a tree no app observes allocates nothing. Handlers run inline on the
write path; a handler that throws is logged and skipped, and the write is never
failed. Delivery is at-most-once and covers user writes only (library maintenance
writes are never delivered). A newly enabled app starts receiving once the rebuild
lands; a disabled or uninstalled app stops receiving as soon as the silo's registry
snapshot reflects the change, shortly after it commits, without waiting for the
rebuild. A subscription the manifest declares with no registered handler, or with
more than one, fails that app's subscription activation. Delivery is keyed by the
logical tree id the observer receives, which stays the same across a resize, a
shadow-cutover restore (and its revert) or a schema remediation of the observed
tree, so a subscription keeps delivering when its tree's data moves to a new
physical copy.

A subscription to the app's own trees needs no consent. A cross-app subscription
(and a subscription to an adopted tree) must be covered by an approved exception
scope in the ceiling, or that app's subscriptions fail to activate with a message
naming the observed app. A cross-app subscription also needs the observed app to be
the installed owner of the tree, or that app's subscriptions fail to activate the same
way; when the observed app is uninstalled, the next routing-table rebuild denies the
subscription and it stops delivering. A subscription activation failure
is logged; it does not fail the enable and is not recorded in the app's activation
status. Cross-tenant observation is not introduced: a subscription only sees trees
composed for its own install's tenant.

## Security

- `AppInstall` is required for every lifecycle transition and every control-facade
  verb except the advisory capability probe, and is never part of
  `LatticeAuthOperations.All`.
- The registry (`sys-app-*`) is control-plane read isolated; the evaluator excludes
  it from all-trees wildcards, and an unmatched request fails closed even under a
  permissive default effect.
- App rules are ordinary rules evaluated by the existing gate; this package adds no
  enforcement path of its own.
- App-owned rule ids cannot be edited or deleted except under system origin.
- Each tree belongs to exactly one install, recorded in the `sys-app-trees` ledger,
  and core aliasing cannot cross that ownership - see
  [Tree ownership](#tree-ownership).
- The role and subscription compilers never grant or observe the cluster-wide
  sentinel, a reserved or system-data tree, or a tenant-qualified id, whatever the
  ceiling approves, and never emit a scopeless capability.
- Physical tree ids are kept out of the control facade's responses and exception
  messages (see the [facade](../lattice.api.apps/README.md)). Telemetry and mutation
  observers report the logical tree id. Physical ids remain visible in storage
  accounting and backup artifacts, each gated by its own capability: no app role can
  carry the scopeless `Telemetry` capability, and a role confers `Backup` or
  `TreeLifecycle` only when the manifest requests it and the operator's ceiling
  allows it.

## Configuration reference

### `LatticeAppsOptions`

| Option | Default | Meaning |
|---|---|---|
| `ReconcileOnStartup` | `true` | Reconcile every enabled app once, in the background, when the silo starts. |
| `StartupRetryDelay` | 250 ms | Initial delay before retrying the startup registry read while the silo cannot yet serve it; doubles on each retry. Must be positive. |
| `StartupRetryMaxDelay` | 30 s | Upper bound on that retry delay. Must be positive and not less than `StartupRetryDelay`. |

### `InImageAppSourceOptions`

| Option | Default | Meaning |
|---|---|---|
| `Registrations` | empty | The apps present in the image, in registration order, each an `InImageAppRegistration` (slug, assembly, manifest resource name, and a `Publisher` that defaults to `first-party`). `AddLatticeApp` appends one, and `Register(slug, assembly, manifestResourceName)` adds one directly. Read once, when the source is constructed; a slug registered twice resolves as `DuplicateRegistration`. |

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

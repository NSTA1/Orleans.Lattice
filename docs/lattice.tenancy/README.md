# Orleans.Lattice.Tenancy

Opt-in **multi-tenancy** for Orleans.Lattice: complete tenant isolation and
runtime resource governance, layered over a small number of generic core seams.

## What is it?

`Orleans.Lattice.Tenancy` makes a **tenant** a first-class citizen of a Lattice
deployment. Tenants that share a cluster (or a set of replicated clusters) are:

- **Completely isolated** in what they can access - a subject's active tenant may
  only read, write, enumerate, administer, back up, restore, or replicate trees
  inside its own tenant's namespace; and
- **Governed at runtime** - each tenant carries aggregate quotas across all of its
  trees (durable bytes, live keys, resident memory, tree count, request rate), an
  optional burst allowance whose overage is explicitly metered, and an optional
  physical-isolation binding, all adjustable at runtime through the control plane.

It is a **companion package**, following the same model as `lattice.auth` and
`lattice.schema`: the tenancy logic (registry, compiled quota/isolation policy,
admission metering, tenant-aware enforcement, overage metering, physical-placement
binding) lives here, and core gains only thin, generic null seams. When this
package is **not** registered those seams resolve to null implementations and core
keeps its exact current path, so a tree that never opts in pays **zero overhead**.

The tenant lifecycle and governance control plane ships as a sibling facade
family - see [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md)
and its [gRPC binding](../lattice.api.tenantadmin.grpc/README.md).

## Core properties

- **Opt-in and non-destructive.** Enabling the feature on an existing cluster
  preserves all existing configuration and data. Every pre-tenancy tree keeps its
  bare, unsegmented id and is adopted into a reserved `default` tenant that owns
  the entire legacy namespace. Existing per-tree options, registry entries,
  aliases, shard maps, and data are untouched.
- **Zero-cost when absent.** With the package unregistered, tree-id derivation,
  enumeration, and the access gate behave byte-for-byte as before, because the
  core seams resolve to their null defaults.
- **Fail-closed isolation.** The tenant boundary is a hard default-deny wall.
  Every access path - data plane, enumeration/catalog, control plane,
  backup/restore, replication apply, observability, and Explorer - is
  tenant-scoped. Cross-tenant access exists only where an explicit grant or a
  platform-operator scope authorizes it.
- **Hard dependency on identity.** The tenant is a membership attribute, so
  registering `lattice.tenancy` without both `lattice.auth` and
  `lattice.membership` is a fail-fast at silo-build time, never a silent downgrade
  to an unenforced state.
- **Coordination-free multi-cluster.** Tenant definitions converge across clusters
  on the existing system-tree replication path; usage enforcement uses a
  convergent CRDT sum (no locks, no consensus) with bounded, quantified overshoot.

## Quick start

Register the package on the silo, alongside the auth and membership packages it
depends on:

```csharp verify
using Orleans.Lattice.Tenancy;

siloBuilder.AddLatticeTenancy(options =>
{
    // Seed the reserved `default` tenant (unbounded quota) so an existing
    // cluster's legacy trees are adopted non-destructively. Default: true.
    options.SeedDefaultTenant = true;

    // Materialise the durable tenant-definition history view so tenant changes
    // are queryable without a process restart. Default: true.
    options.EnableDurableHistoryView = true;
});
```

Tune the durable history retention over the `sys-tenant-*` registry trees:

```csharp verify
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;

siloBuilder.ConfigureLatticeTenancy(options =>
{
    options.HistoryRetentionMode = HistoryRetentionMode.FullValue;
    options.HistoryRetentionWindow = TimeSpan.FromDays(30);
});
```

## Isolation model

- **Structural tenant-segment prefix.** A tenant owns a namespace of trees
  addressed by an unqualified name; the tenancy layer injects a reserved tenant
  segment so a tree id self-describes its owner. The composed id has the shape
  `t/{tenantId}/{name}`, and the gate enforces ownership with a cheap ordinal
  prefix check - the same shape as the existing `_lattice_` / `sys-` reserved
  namespaces. The tenant prefix is a third reserved namespace with its own
  user-write guard. Compose and inspect tenant tree ids with the core
  `LatticeTenantTrees` helper:

```csharp verify
using Orleans.Lattice;

TenantId acme = TenantId.Parse("acme");

// "t/acme/orders"
string treeId = LatticeTenantTrees.Compose(acme, "orders");

bool isScoped = LatticeTenantTrees.IsTenantScoped(treeId);
if (LatticeTenantTrees.TryGetTenant(treeId, out TenantId owner))
{
    // owner == acme
}
```

- **Identity-derived, enforced at the auth gate.** The active tenant is carried in
  the Orleans `RequestContext` under a single well-known key, populated from the
  caller's membership at the auth seam. The fail-closed `PolicyAccessGate` is made
  tenant-aware: a request is denied unless the subject's active tenant owns the
  target tree (prefix match), or an explicit cross-tenant grant or platform-operator
  scope authorizes it.
- **Active-tenant assertion.** A subject carries a set of tenant memberships. A
  subject with exactly one membership gets that tenant as an implicit default; a
  subject with two or more memberships must assert an active tenant explicitly, and
  a request that asserts none - or one outside the membership set - is denied, not
  silently defaulted.
- **Tenant id grammar.** A `TenantId` matches `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`
  (lower-case alphanumeric and hyphen, 1-63 chars). This guarantees a tenant id can
  never contain the `/` segment separator, never begins with `_`, and can never
  spell `sys-`, so it cannot collide with or spoof the reserved namespaces. The id
  `default` is reserved for the legacy-adoption tenant and cannot be created,
  suspended, or deleted. Tenant ids are immutable once created.
- **Registry-store read isolation.** The `sys-tenant-*` registry, usage, and
  overage trees hold the cross-tenant registry itself - every tenant's admin
  subjects, quotas, region residency, and cross-tenant grants. They live in the
  `sys-` system-data namespace, so first-party access runs system-origin and
  short-circuits the gate; every external request is governed with **control-plane
  read isolation**, exactly like the reserved `sys-auth-*` policy store. A
  data-plane read or scan is denied independently of `DefaultEffect`, and a
  cluster-wide all-trees (`Tree:*`) wildcard grant never reaches them, so no broad
  data-plane role can enumerate one tenant's metadata from another. Only a
  bootstrap administrator, a system-origin caller, or an explicit rule an operator
  deliberately scopes at a registry tree may read them.

## Resource governance

Each tenant carries **aggregate quotas** across all of its trees, expressed by
`TenantQuotas`:

| Dimension | Property | Meaning |
|---|---|---|
| Durable bytes | `MaxBytes` | Aggregate durable size across the tenant's trees. |
| Live keys | `MaxKeys` | Aggregate live-key count. |
| Resident memory | `MaxMemoryBytes` | Aggregate resident cache budget. |
| Tree count | `MaxTreeCount` | Number of trees the tenant may own. |
| Request rate | `MaxOpsPerSecond` | Cluster-wide ops/sec ceiling. |
| Burst | `BurstPercent` | Percentage overage above the steady-state caps. |

A `null` cap on a dimension means unlimited on that dimension. The reserved
`default` tenant and every newly created tenant start with no caps until an
operator sets them, so opt-in never suddenly throttles an existing workload.

- **Compiled quota policy.** Steady-state enforcement uses a compiled policy
  snapshot with a monotonic epoch, refreshed off the `sys-tenant-*` change feed and
  evaluated synchronously in-memory with no I/O once warm - mirroring the auth
  `PolicyAccessGate` / `ILatticeDecisionEngine`.
- **Burst and metering.** Usage at or below the steady-state cap is ordinary; usage
  above the cap and at or below `cap x (1 + burst%)` is admitted and **metered as
  overage** - a first-class, billing-ready signal distinct from ordinary usage;
  usage above `cap x (1 + burst%)` is refused with `LatticeQuotaExceededException`
  carrying the tenant id and dimension. A tenant with burst `0` refuses at the cap.
- **Apply-path admission bypass, never isolation bypass.** As in core, the
  replication-apply and saga-apply paths bypass quota *admission* (they re-enter
  under a foreign/prepared scope) but never bypass the tenant *isolation* boundary.

### Enforcement scope (multi-cluster)

Each tenant carries a per-tenant `TenantEnforcementScope` (`TenantUsageAccountingOptions.DefaultEnforcementScope`
supplies the default for new tenants):

- **`GlobalConverged` (default).** For the slow-moving storage gauges (bytes, keys,
  memory, tree count) each resident cluster contributes its current local usage to a
  per-cluster-slot state CRDT (a map from `ClusterId` to that cluster's latest
  sample). A cluster writes only its own slot and reads the whole map, so global
  usage is the sum-fold over the `Online` resident regions' slots. Enforcement admits
  against the global fold, giving a single global budget rather than `limit x clusters`,
  with bounded transient overshoot. Monotonic tallies (overage, ops-ever) use
  grow-only `GCounter`s. Slots are republished on a cadence with hysteresis so
  continuous usage does not flood the replication path.
- **`PerCluster` (fallback).** Each cluster meters only its own local usage against
  the limit, adding no usage-replication traffic. Effective global capacity is
  `limit x clusters`. Selectable per tenant for operators who prefer hard-partitioned,
  zero-telemetry-cost capacity.

There is no cross-cluster coordination or consensus in either scope -
`GlobalConverged` reads a convergent CRDT sum, it never locks or votes.

### Rate limiting

The `ops/sec` limit is always enforced **per-cluster** in both scopes (a rate window
is too short relative to replication lag for a converged global count to be
meaningful). It is enforced by silo-local, in-process token buckets - a per-silo
singleton limiter (not a grain) the data-plane entry path consults with a lock-free
token decrement - so the per-op hot path takes zero grain hops. A low-frequency
per-`(tenant, cluster)` budget coordinator divides the cluster rate across the live
silos at lease cadence (`O(silos)`, never `O(ops)`). `LatticeTenantRateLimiterOptions`
controls the lease interval and the apportionment strategy
(`TenantRateApportionmentStrategy.Demand`, the default demand-proportional leasing,
or `StaticEven` as the zero-coordination fallback).

## Region residency

Which regions a tenant lives in is a per-tenant, runtime-mutable choice layered on
top of the replication topology:

- **Allowed vs resident.** A platform operator authorizes, per tenant, the *allowed*
  region set; the tenant's delegated admin selects its *residency set* (the subset it
  actually replicates to and is served from) within that allowed set. A new tenant
  defaults to the region it is created in.
- **Metadata everywhere, data to the residency set.** Tenant definitions still
  converge to every region, so any region can fail-closed answer "is this tenant
  resident here?"; tenant data replicates only to the residency set.
- **Observable lifecycle.** Adding a region moves it `Provisioning -> Backfilling ->
  Online`; it is not served, not counted for quota, and not part of the
  `GlobalConverged` fold until `Online` (its backfill is complete). Removing a region
  moves it `Draining -> Offline -> Removed`, and only after its data is confirmed
  present in the remaining residency set. The last resident region cannot be removed.
- **Symmetric multi-master.** An `Online` region is a full read-write replica; there
  is no primary or leader. Enforcement ties in at the gate (a tenant not `Online` in
  the serving region is refused) and the replication apply path (a tenant's writes
  never land in a non-resident region).

Region residency is administered through the
[`ILatticeTenantRegionAdmin`](../lattice.api.tenantadmin/README.md) control-plane
facade.

## Observability

`TenantObservabilityOptions` (default `PublishGauges = true`) publishes per-tenant
gauges - current usage against each quota dimension and the metered overage tallies -
on a fixed cadence, so an operator can see per-tenant consumption and headroom, and
region-status change events surface exactly when a new region becomes `Online` or an
old one is fully drained.

## Security

- **Fail-closed everywhere.** Every enforcement seam denies on an unmatched request.
  Tenant data isolation and tenant lifecycle administration are both independent of
  the data-plane `DefaultEffect`, so an unmatched request always resolves to deny
  even under `DefaultEffect = Allow`.
- **Registry confidentiality.** The `sys-tenant-*` registry, usage, and overage
  trees are control-plane read-isolated: a data-plane read or scan is denied
  independently of `DefaultEffect`, and no cluster-wide all-trees (`Tree:*`) grant
  can reach them, so the cross-tenant registry can never be enumerated through a
  broad data-plane read role. See "Registry-store read isolation" above.
- **A tenant admin cannot self-grant cross-tenant read.** The registry escape hatch -
  "an explicit rule an operator deliberately scopes at a registry tree" - is an
  *operator* act by construction, and here **operator** has a narrow, specific
  meaning: a **bootstrap administrator** (the break-glass root of trust configured on
  the silo) or a subject a bootstrap administrator has **explicitly promoted to
  access-administrator** through the access-administration delegation. It does **not**
  mean "any authenticated caller," "any caller with a broad data-plane grant," or "a
  tenant admin." The reason a tenant admin cannot perform this act is structural, not a
  matter of degree: authoring *any* authorization rule is a write to the reserved
  `sys-auth-*` policy store, which requires whole-tree `Admin` on that store - a
  control-plane capability held only by the operators just defined. A tenant-admin
  capability is `Admin` on its own tenant-administration scope only, honoured for that
  one tenant and never inherited for another's, so a tenant admin can neither reach the
  policy store to author a registry-read rule (for itself or anyone else) nor match a
  second tenant's scope. Direct writes to `sys-tenant-*` are likewise refused off the
  system-origin path by the reserved-prefix write guard. Consequently, granting
  cross-tenant registry visibility always requires a deliberate operator decision to
  author (or delegate the authority to author) that rule; a caller acting purely as a
  tenant admin has no path to it.
- **Two-tier governance.** A platform-operator capability (cluster-wide) performs
  tenant lifecycle, quota/burst/scope/placement changes, allowed-region authorization,
  and cross-tenant grants. A delegated per-tenant admin capability (scoped to one
  tenant) manages that tenant's trees, subjects, schema, and region residency strictly
  within the granted quota and allowed-region set - it can neither raise its own caps,
  widen its allowed regions, nor reach another tenant.
- **Enable-gated.** No tenant can be created unless the feature is enabled, and every
  mutating control-plane tool is contributed only when the host opts writes in.

## Tenant-aware surfaces

Tenancy also reaches the operator- and agent-facing surfaces. Each is activated purely
by whether tenancy is enabled - there is no separate opt-in flag - so a deployment
without tenancy keeps a byte-for-byte-unchanged UI and tool surface.

- **Explorer.** When the Explorer's tenant view is enabled, the signed-in header shows
  the caller's current tenant and, for a platform operator, a selector to switch the
  active tenant or request an all-tenant view. The controls render nothing for an
  anonymous caller or a non-tenancy deployment, and every switch is authorized
  fail-closed through the operator gate. See
  [`Orleans.Lattice.Explorer`](../lattice.explorer/README.md).
- **MCP.** When tenancy is wired, the MCP server contributes three read-only tenant
  self-awareness tools - `lattice_tenant_current` (the tenant the caller is operating
  as), `lattice_tenant_list` (the tenants the caller may access), and
  `lattice_tenant_get` (one accessible tenant's lifecycle and per-region residency).
  They are scoped fail-closed to the caller's subject: an anonymous caller lists
  nothing, and an inaccessible tenant is indistinguishable from an absent one. The
  mutating tenant-admin tools remain separately gated behind
  `EnableTenantAdminControlTools`. See
  [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md).

## Configuration reference

### `LatticeTenancyOptions`

| Property | Default | Meaning |
|---|---|---|
| `HistoryRetentionMode` | `MetadataOnly` | Retention mode for the durable per-key history captured on the `sys-tenant-*` trees. History is never disabled by default. |
| `HistoryRetentionWindow` | `null` | Age after which a registry history revision row expires; `null` means no age bound. Must be strictly positive when supplied. |
| `EnableDurableHistoryView` | `true` | Whether to create the durable history materialised view over the registry trees. |
| `SeedDefaultTenant` | `true` | Whether to seed the reserved `default` tenant (unbounded quota) at startup when absent. The seed is create-if-absent, so it never clobbers an operator's later edits. |

## See also

- [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) - the
  transport-agnostic tenant-administration and region-residency control facades.
- [`Orleans.Lattice.Api.TenantAdmin.Grpc`](../lattice.api.tenantadmin.grpc/README.md) -
  the code-first gRPC binding and remote client for the tenant-administration facade.
- [`Orleans.Lattice.Explorer`](../lattice.explorer/README.md) - the web UI whose
  tenant view surfaces the signed-in tenant crumb and operator tenant selector.
- [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md) - the MCP server that
  contributes the read-only tenant self-awareness tools.
- [`Orleans.Lattice.Auth`](../lattice.auth/README.md) - the authorization gate the
  tenant boundary is enforced at.
- [`Orleans.Lattice.Membership`](../lattice.membership/README.md) - the identity layer
  that supplies a subject's tenant memberships.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md) - a runnable end-to-end
  walkthrough of opt-in wire-up, tenant lifecycle, isolation, and quota governance.

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
- **Two-tier governance.** A platform-operator capability (cluster-wide) performs
  tenant lifecycle, quota/burst/scope/placement changes, allowed-region authorization,
  and cross-tenant grants. A delegated per-tenant admin capability (scoped to one
  tenant) manages that tenant's trees, subjects, schema, and region residency strictly
  within the granted quota and allowed-region set - it can neither raise its own caps,
  widen its allowed regions, nor reach another tenant.
- **Enable-gated.** No tenant can be created unless the feature is enabled, and every
  mutating control-plane tool is contributed only when the host opts writes in.

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
- [`Orleans.Lattice.Auth`](../lattice.auth/README.md) - the authorization gate the
  tenant boundary is enforced at.
- [`Orleans.Lattice.Membership`](../lattice.membership/README.md) - the identity layer
  that supplies a subject's tenant memberships.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md) - a runnable end-to-end
  walkthrough of opt-in wire-up, tenant lifecycle, isolation, and quota governance.

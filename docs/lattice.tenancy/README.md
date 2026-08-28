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
  user-write guard: a user-origin write may name a `t/` id only when the id's
  structural owner is the caller's own active tenant - which is exactly what the
  facades compose - so a caller can never name another tenant's namespace, and with
  tenancy off (where there is no active tenant) the namespace is uncreatable through
  the public surface. Compose and inspect tenant tree ids with the core
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

- **Derived trees are scoped through their name.** A materialised view is not a
  caller-supplied tree id but a tree the maintainer derives from the view's name,
  so it is the *name* that is resolved to the active tenant: a view created as
  `orders` materialises as `t/{tenant}/view-orders`. Placing the tenant segment
  outermost is what makes ownership, enumeration filtering, and the tenant delete
  cascade apply to a view tree exactly as they do to any other tree, and it lets
  two tenants use the same unqualified view name over their own same-named sources
  while each reads back only its own. Because the maintainer, the view catalog, and
  the durable view registry are all keyed by the view name, scoping the name is
  also what makes the isolation survive a silo restart. Tag-index trees are not
  partitioned today and remain cluster-global.
- **The asserted tenant must reach the silo on every transport.** Tenant scoping
  is applied inside each API facade, so it only takes effect when the caller's
  asserted tenant has been lifted onto the ambient context. In a co-hosted head
  that happens in-process and flows to the grain on the Orleans request context.
  On a **split head** - an API head in its own process reaching the silo over gRPC
  - each binding must lift the `lattice-active-tenant` header itself, which every
  binding does through the shared `LatticeActiveTenantAssertion` seam. A binding
  that did not would not fault: its facade would resolve the reserved default
  tenant and serve the caller the shared cluster-global namespace, so the
  behaviour is covered by a contract guard rather than left to review.- **A refused assertion is reported as a refusal, on every surface.** The resolver
  denies a caller by resolving the uninitialised `default(TenantId)` "no tenant"
  value - a `null` `TenantId.Value`, deliberately distinct from the reserved
  `TenantId.Default`, whose value is `default`. Every surface that reads a
  resolved tenant honours that sentinel: the data plane refuses the operation, and
  the tenant self-awareness surface refuses too rather than reporting a live
  descriptor, so "which tenant am I acting as" can never answer with a tenant the
  caller was denied. The distinction matters when reading this page: the reserved
  default tenant is a real tenant that a caller legitimately resolves when it
  asserts nothing, whereas the sentinel means the assertion was rejected.
- **A denial is an authorization outcome, not a fault.** A call refused by
  fail-closed tenant resolution surfaces as `PermissionDenied` on every gRPC
  binding, carrying the reason. It is deliberately not `Internal`: that is a
  retryable status, so a client would back off and retry a decision that can never
  change, and the refusal would be counted against the server-fault rate operators
  alert on. A call that resolves cleanly but breaches the tenant's quota is a
  different outcome again - capacity, not authorization - and surfaces as
  `ResourceExhausted`. An enumeration for a denied caller returns an empty page
  rather than an error, so listing never leaks the cluster-global catalog.
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
  never contain the `/` segment separator and never begins with `_`, so it cannot
  collide with or spoof the `_lattice_` namespace or the `t/{tenant}/{name}`
  segment structure. The grammar alone does *not* exclude a `sys-` prefix (those
  are all legal characters), so tenant **creation** additionally rejects an id
  beginning with `sys-` or `_lattice_`: a tenant id travels into tree ids, metric
  labels, and log lines beside real tree ids, and one shadowing a reserved
  namespace is an avoidable confusion trap. The check is applied at create only,
  so a tenant registered before the guard existed stays readable and deletable.
  The id `default` is reserved for the legacy-adoption tenant and cannot be
  created, suspended, or deleted. Tenant ids are immutable once created.
- **Tenant-scoped tree naming.** `AddLatticeTenancy` replaces the core's no-op
  `ITenantContextResolver` with one that reads the caller's active tenant and
  re-validates it against that caller's own membership before it is allowed to
  scope a name. This is what makes
  `services.GetLatticeAsync("orders")` address `t/acme/orders` for a caller acting
  as `acme` and `t/globex/orders` for one acting as `globex`, rather than handing
  both the same physical tree. **Every external API facade does the same**: the
  data, state, tree-administration, schema, replication, and backup facades resolve
  the caller-supplied name through `ITenantContextResolver.ResolveEffectiveTreeIdAsync`
  at their entry point and use that one effective id for **both** the authorization
  check and the operation, so a verb can never authorize one tree and act on
  another. Without that, an external caller had no route into its own namespace at
  all: an unqualified name stayed a shared default-tenant tree, and a directly
  supplied `t/{tenant}/...` id is (correctly) refused by the reserved-namespace
  guard, because composition is internal. A caller that asserts no tenant resolves
  the reserved `default` tenant and keeps its bare tree ids (non-destructive
  adoption); a caller asserting a tenant it may not act as resolves the
  uninitialised "no tenant" value, which fails closed with a
  `LatticeTenantAccessDeniedException` rather than silently defaulting. Because the
  effective id is tenant-owned, usage metering and quota admission attribute the
  traffic to the acting tenant - the two only line up once the name is actually
  scoped.
- **Enumeration pruning.** `AddLatticeTenancy` likewise replaces the core's no-op
  `ITenantEnumerationFilter`, so a tree-id enumeration (the cluster-state tree
  catalog, the tag-index catalog, the in-cluster all-tree-ids read) is pruned to
  the trees the active tenant owns. Pruning is defence in depth rather than the
  boundary: a caller that asserts no tenant is not pruned, and is confined instead
  by the per-entry authorization check, which composes the same tenant enforcer the
  write path uses and denies a tenant-scoped tree outright when no active tenant is
  selected. That check is the durable guarantee - an existence probe can never
  out-reach the enforcement decision, so no broad grant or `DefaultEffect = Allow`
  posture can surface another tenant's tree names.
- **Enumeration is scoped at the source.** Because a tenant's trees all begin
  `t/{tenant}/` and the tree registry is itself an ordinally-sorted Lattice tree,
  a tenant's trees occupy one contiguous key range. Where it is provably
  equivalent to the unscoped read, an enumeration pushes that prefix down to the
  registry (`LatticeTenantTrees.ComposePrefix` supplies it), so the scan is bounded
  to the tenant's own range and no other tenant's ids cross the grain boundary at
  all - rather than transferring the whole catalog for the caller to discard most
  of. The tenant delete cascade, which enumerates exactly one tenant's trees, is
  scoped this way, as is the tree catalog when a non-default tenant is active and
  the request excludes system trees (the ids a prefix scan skips are the ones that
  switch already drops). The prefix is a **performance hint, never an
  authorization boundary**: it can only ever return a subset of what the caller
  could already enumerate, and the pruning filter and per-entry authorization check
  both still run unchanged.
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
  Over the [data gRPC binding](../lattice.api.data.grpc/README.md#quota-refusals)
  the refusal reaches a remote caller as `ResourceExhausted` carrying the breached
  dimension as a trailer; the tenant id is not echoed back, because the caller
  asserted its own active tenant on the request.
- **Metering drives enforcement, on a cadence.** A quota is admitted against the
  tenant's *metered* usage, so nothing binds until a usage sample lands. Each silo
  runs a background metering cycle every
  `TenantUsageAccountingOptions.MeterInterval` (default 30 seconds) that walks each
  tenant's own trees - a bounded range scan over the tenant's `t/{tenant}/` key
  range, not a read of the whole catalog - samples their footprint, and rolls the
  result up into that tenant's per-cluster usage slot. Admission deliberately
  **fails open** for a tenant with no landed sample yet, so a cold silo never
  spuriously refuses; that means enforcement arms one cycle after a tenant first
  has usage. Setting `MeterInterval` to zero disables metering entirely and leaves
  admission permanently open, which is only appropriate for a deployment running
  tenancy without resource governance.
- **A tenant's first sample always publishes.** Republishing a usage slot is gated
  by a hysteresis band (`PublishMinAbsoluteDelta` / `PublishMinRelativeDelta`) so a
  stream of negligible movements does not churn the registry. That band damps churn
  *between successive samples*, so it is deliberately not applied to a tenant's
  first publish: until the slot exists admission is fail-open and no quota binds at
  all, so a tenant whose whole footprint sits below the absolute floor (default
  65,536) would otherwise never be governed. Establishing the slot costs one write
  per tenant per publisher lifetime; every movement after it is damped as normal.
- **A stale footprint is re-anchored, not trusted.** The key and memory figures a
  tree reports are activation-scoped: a shard root rebuilds them as its leaves
  republish on commit boundaries, so they read zero after a reactivation until
  writes resume - and Orleans collects idle grains, so that needs no restart or
  fault. The byte figure is unaffected because it adds durable WAL retention. A
  tree that reports no keys and no leaf bytes yet a non-zero total is therefore
  showing a cold cache rather than an empty tree, and metering re-anchors it with
  a deep walk instead of publishing the zero. Without that, `MaxKeys` and
  `MaxMemoryBytes` would fail **open** - admitting a tenant well over quota - while
  `MaxBytes` and `MaxTreeCount` kept binding. The cost is self-limiting: a large
  tree re-anchors once and then reports non-zero, so only a genuinely empty tree
  that still retains WAL is re-walked, and walking an empty tree is cheap.
- **Request rate is enforced with the footprint dimensions.** `MaxOpsPerSecond` is
  applied by the same admission seam, ahead of the footprint checks, from the
  tenant's silo-local token budget. A breach surfaces as
  `LatticeQuotaExceededException` on the `ops-per-second` dimension and is
  explicitly **transient**: the budget refills continuously, so an immediate retry
  after a short backoff succeeds, unlike a footprint breach which persists until
  the tenant's usage drops.
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
tunes that coordinator; none of its knobs touch the per-op hot path, so a
misconfiguration changes only how the cluster rate is split, never whether
enforcement stays lock-free:

| Option | Type | Default | Meaning |
|---|---|---|---|
| `LeaseInterval` | `TimeSpan` | `5s` | How often the coordinator re-apportions each tenant's cluster rate across the live silos. Must be strictly positive. A longer interval lowers coordination cost but widens the transient overshoot bound (lease interval times cluster rate). |
| `Apportionment` | `TenantRateApportionmentStrategy` | `Demand` | `Demand` leases demand-proportionally and degrades to static-even when no cluster-wide demand aggregate is available; `StaticEven` is the zero-coordination fallback that splits the rate evenly. |
| `DemandReserveFraction` | `double` | `0.2` | The fraction of the cluster rate that demand-proportional leasing reserves and splits evenly, guaranteeing an idle silo a non-zero floor so it can never be starved out of building demand. In `[0, 1]`; ignored under `StaticEven`. |

A breach surfaces as a `LatticeQuotaExceededException` on the `ops-per-second`
dimension. Unlike the footprint dimensions it is **transient**: the same call
generally succeeds once the bucket refills, so a client should treat it as a
back-pressure signal to retry rather than as a durable capacity failure. Over the
[data gRPC binding](../lattice.api.data.grpc/README.md#quota-refusals) it reaches
a remote caller as a `ResourceExhausted` `RpcException` carrying the breached
dimension as a trailer, so a client can tell a retryable rate breach from a
footprint breach that will not clear on its own.

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

`TenantObservabilityOptions` (default `PublishGauges = true`, `PublishInterval` 30
seconds) publishes per-tenant gauges - current usage against each quota dimension and
the metered overage tallies - on a fixed cadence, so an operator can see per-tenant
consumption and headroom, and region-status change events surface exactly when a new
region becomes `Online` or an old one is fully drained.

Every instrument is an **observable gauge** on the `orleans.lattice.tenancy` meter
(`LatticeTenantMetrics.MeterName`). Each series carries a single `tenant` tag
(`LatticeTenantMetrics.TagTenant`) identifying the owning tenant; the one
cluster-aggregate series is untagged. Set `PublishGauges = false` to publish none of
them.

| Instrument | Meaning |
|---|---|
| `orleans.lattice.tenancy.tenants` | Cluster-aggregate count of tenants in the warm usage index. The one series with no `tenant` tag. |
| `orleans.lattice.tenancy.usage.bytes` | The tenant's current aggregate durable bytes. |
| `orleans.lattice.tenancy.usage.keys` | The tenant's current aggregate live-key count. |
| `orleans.lattice.tenancy.usage.memory_bytes` | The tenant's current aggregate resident memory. |
| `orleans.lattice.tenancy.usage.trees` | The number of trees the tenant currently owns. |
| `orleans.lattice.tenancy.quota.bytes` | The tenant's steady-state `MaxBytes` ceiling. |
| `orleans.lattice.tenancy.quota.keys` | The tenant's steady-state `MaxKeys` ceiling. |
| `orleans.lattice.tenancy.quota.memory_bytes` | The tenant's steady-state `MaxMemoryBytes` ceiling. |
| `orleans.lattice.tenancy.quota.trees` | The tenant's steady-state `MaxTreeCount` ceiling. |
| `orleans.lattice.tenancy.quota.burst_percent` | The tenant's `BurstPercent` headroom above its bounded ceilings. |
| `orleans.lattice.tenancy.overage.bytes` | Converged, durable metered byte overage accrued above the byte ceiling. |
| `orleans.lattice.tenancy.overage.keys` | Converged, durable metered key overage. |
| `orleans.lattice.tenancy.overage.memory_bytes` | Converged, durable metered resident-memory overage. |
| `orleans.lattice.tenancy.overage.trees` | Converged, durable metered owned-tree overage. |

A `quota.*` gauge emits a measurement **only for a tenant whose corresponding
dimension is bounded** - an unbounded (`null`) ceiling contributes no series at all,
so "no series" reads as "unlimited on that dimension" rather than "zero". Usage
gauges reflect the last landed metering sample (see `MeterInterval` above), so a
tenant with no sample yet has no usage series. The `overage.*` gauges are the
billing-ready tallies: they are grow-only converged sums, not instantaneous
readings.

`MaxOpsPerSecond` has no gauge: the rate budget is enforced from silo-local token
buckets rather than from a published aggregate, so a breach is observed through the
`ops-per-second` `LatticeQuotaExceededException` rather than a series.

These instruments are charted by the bundled **Per-Tenant Observability** Grafana
dashboard (`LatticeDashboardKind.Tenancy`), which offers a templated `tenant`
variable so a panel can be scoped to a single tenant or to every tenant. See
`docs/lattice.dashboards/metrics-to-panel-map.md` for the instrument-to-panel
mapping. To consume them directly instead, subscribe to the
`orleans.lattice.tenancy` meter from your OpenTelemetry exporter.

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

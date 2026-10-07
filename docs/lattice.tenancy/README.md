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
  trees (durable bytes, live keys, resident memory, tree count, request rate) and an
  optional burst allowance whose overage is explicitly metered, both adjustable at
  runtime through the control plane. Its tenant record also carries a physical
  placement binding (a dedicated WAL provider and/or a silo placement filter - the
  filter is recorded but not acted on); the control plane creates every tenant on
  the shared placement, and a binding is immutable in effect once the tenant's trees
  are placed.

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
  `AddLatticeTenancy` is guarded: it throws an `InvalidOperationException` at
  registration time - not at silo start, and never as a silent downgrade to an
  unenforced state - unless `AddLattice`, `AddLatticeMembership`, and
  `AddLatticeAuth` have all already run on the same builder.
- **Coordination-free multi-cluster.** Tenant definitions and usage are convergent
  CRDT state - a registry record merges field by field, and usage enforcement reads a
  convergent sum (no locks, no consensus) with bounded, quantified overshoot - so they
  converge across every cluster the `sys-tenant-*` trees replicate to.
  `AddLatticeTenancy` automatically declares `sys-tenant-registry` in the static
  replication map as `LwwRegister` on every host, regardless of whether
  `AddLatticeReplication` runs before or after it. With replication absent the
  declaration is inert. Usage and overage trees remain explicitly enrolled by the
  deployment; `ReplicateLatticeSystemTrees` covers membership and authorization.
  Local registry writes join fields before persisting; cross-region transport
  resolves whole serialized records by last-writer-wins, not a field-wise join.
  Seed tenant definitions once and let them replicate rather than creating
  independent copies in each region.

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

Tune the durable history retention on the `sys-tenant-registry` tree (the usage and
overage trees keep no per-key history):

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
  user-write guard on the `ILattice` mutation surface: a user-origin write there
  may name a `t/` id only when the id's structural owner is the caller's own active
  tenant - which is exactly what the facades compose. Reads are not guarded: a read
  naming another tenant's `t/` id reaches the access gate, which refuses the
  crossing unless a cross-tenant grant (or a platform-operator scope) authorizes
  it. A grant's scope is matched against that full `t/{owner}/...` id, so a grant
  offered for an unqualified name such as `orders` is accepted but covers no tree. The app-tree prefix `a/` used by
  [installable apps](../lattice.apps/README.md) is deliberately **not** reserved or
  treated as qualified: an app tree `a/{app}/{tree}` is an ordinary unqualified name,
  so it composes to `t/{tenantId}/a/{app}/{tree}` and each tenant gets its own copy of
  an installed app (with tenancy off it stays the bare `a/{app}/{tree}`). An app that
  declares replication enrols each tenant's composed trees per install, through the
  runtime replication configuration (see
  [Replication intent](../lattice.apps/README.md#replication-intent)), and they are
  admitted by the tenant replication isolation gate like any other tenant tree. Compose and inspect tenant tree ids with the core
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
  - each binding must lift the `lattice-active-tenant` header itself. The control-plane
  bindings do it through the shared `LatticeActiveTenantAssertion` helper, and the
  data binding through its own replaceable `ILatticeDataApiActiveTenantBridge` seam. A binding
  that did not would not fault: its facade would resolve the reserved default
  tenant and serve the caller the shared cluster-global namespace, so the
  behaviour is covered by a contract guard rather than left to review.
- **A refused assertion is reported as a refusal, on every surface.** The resolver
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
  binding, carrying the reason (the apps bindings send a fixed message instead).
  It is deliberately not `Internal`: that is a
  retryable status, so a client would back off and retry a decision that can never
  change, and the refusal would be counted against the server-fault rate operators
  alert on. A call that resolves cleanly but breaches the tenant's quota is a
  different outcome again - capacity, not authorization - and surfaces as
  `ResourceExhausted` on the data and schema gRPC bindings. The
  tree-administration binding (where a create can breach the tree-count ceiling)
  maps it, as the `InvalidOperationException` that `LatticeQuotaExceededException`
  derives from, to `FailedPrecondition`. An enumeration for a denied caller returns an empty page
  rather than an error, so listing never leaks the cluster-global catalog.
- **Identity-derived, enforced at the auth gate.** The active tenant is carried in
  the Orleans `RequestContext` under a single well-known key, stamped at the edge
  from the caller's asserted tenant and validated against the caller's membership at
  the auth seam. The fail-closed access gate behind
  `ILatticeAccessGate` is made tenant-aware: a request is denied unless the
  subject's active tenant owns the target tree (prefix match), or an explicit
  cross-tenant grant or platform-operator scope authorizes it.
- **Membership, status and grant changes take effect at once.** The gate answers
  from a compiled snapshot of the tenant registry, and a registry write (adding or
  removing a tenant admin, suspending or deleting a tenant, approving, rejecting, or
  revoking a grant) only schedules a background rebuild of it. While that rebuild
  is outstanding, or while rebuilds are failing, the gate confirms every request
  that acts as an asserted active tenant against the registry itself: the active
  tenant's record must exist, be `Active`, and list the subject as an admin (the
  same rule the snapshot applies; with
  [delegated tenant access administration](#delegated-tenant-access-administration)
  on, a member entry or a group entry counts too), and a cross-tenant crossing must also find an
  active grant in the owning tenant's record. Both checks must pass on their own.
  So a removed admin, or a subject acting as a just-suspended or deleted tenant, is
  refused on that tenant's own trees as soon as the write commits, a revoked grant
  stops admitting access, and an added admin or an approved grant admits the next
  request. An owned-tree request reads one record and a crossing reads its two
  records concurrently. A request that cannot be confirmed (for example because the
  registry read fails) is denied. Only requests in that window pay the registry
  read; the steady state stays an in-memory decision.
- **Every silo sees the change.** The change feed that drives the rebuild fires only
  on the silo whose grain committed the registry write, so each silo's snapshot is
  also kept current by a cluster-wide tenant-policy epoch. Before a registry write
  returns, the committing silo advances the epoch and pushes it to every silo, which
  marks its snapshot out of date and rebuilds; the write completes once every silo
  has acknowledged, or has had its lease lapse. Each silo holds its snapshot
  authoritative only while it holds a live lease from the epoch (renewed every third
  of `PolicySnapshotLeaseDuration`) and has compiled the latest epoch it has seen.
  A silo that cannot know it is current - its lease has lapsed, it has been told of a
  change it has not compiled, or it could not publish a write of its own - confirms
  active-tenant requests and crossings against the registry (or denies) exactly as above, and the inbound
  replication isolation gate falls back to the registry for tenant existence and
  status in the same windows. The steady state pays only a few field reads and a
  timestamp read. A restarted epoch holds each write open for about 1.1 times the
  lease (one lease plus a tenth), or until
  every silo cluster membership does not report dead has leased from it, so no silo
  leased by its previous incarnation stays authoritative. One window is bounded
  rather than closed: a silo that crashes after committing a registry write but
  before publishing it leaves the other silos unaware of that write until cluster
  membership declares it dead, at which point every surviving silo rebuilds. Each
  silo also builds its snapshot at start-up, and on its first decision if that has
  not happened yet, so a new silo never reports a registered tenant as unregistered.
  The same epoch and lease keep each silo's residency and placement views of the
  registry current, with the same fail-closed fallbacks: see
  [Every silo applies a residency change](#every-silo-applies-a-residency-change) and
  [Placement follows the registry on every silo](#placement-follows-the-registry-on-every-silo).
- **Active-tenant assertion.** A subject carries a set of tenant memberships, but
  the active tenant is always a caller-supplied *assertion*, never inferred from
  that set - there is no implicit "sole membership" default. Every branch that
  consumes the assertion re-validates it against the caller's own membership, and
  it is denied unless the named tenant is registered, `Active`, and lists the
  caller as an admin subject (or, with
  [delegated tenant access administration](#delegated-tenant-access-administration)
  on, as a member, directly or through one of its groups). A request that asserts *nothing* resolves the
  reserved `default` tenant, which is what keeps legacy adoption
  non-destructive; on a tenant-owned (`t/...`) tree that unasserted request is
  denied instead, because the uninitialised "no tenant" value can never be an
  active tenant. Asserting `default` explicitly is not the same as asserting
  nothing: it is validated like any other assertion, and because the reserved
  tenant is seeded with no admin subjects (and the control plane refuses to add
  any) that assertion fails validation.
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
  The id `default` is reserved for the legacy-adoption tenant: it can never be
  suspended, deleted, given quotas, have its admin-subject set changed, or be named
  on either side of a cross-tenant grant offer (each fails closed with a
  `ReservedTenantOperationException`), while a resume of it is an allowed no-op.
  Tenant ids are immutable once created.
- **Tenant-scoped tree naming.** `AddLatticeTenancy` replaces the core's no-op
  `ITenantContextResolver` with one that reads the caller's active tenant and
  re-validates it against that caller's own membership before it is allowed to
  scope a name. This is what makes
  `services.GetLatticeAsync("orders")` address `t/acme/orders` for a caller acting
  as `acme` and `t/globex/orders` for one acting as `globex`, rather than handing
  both the same physical tree. **The tree-addressing API facades do the same**: the
  data, state, tree-administration, schema, replication, and backup facades resolve
  the caller-supplied name through the `ResolveEffectiveTreeIdAsync` extension
  `LatticeTenantExtensions` adds over `ITenantContextResolver` (the interface
  itself carries only `ResolveCurrentAsync` and its synchronous `TryResolveCurrent`
  fast path) at their entry point and use that one
  effective id for **both** the authorization check and the operation, so a verb
  can never authorize one tree and act on
  another. Without that, an unqualified name would stay a shared default-tenant
  tree. A caller that asserts no tenant resolves
  the reserved `default` tenant and keeps its bare tree ids (non-destructive
  adoption); a caller asserting a tenant it may not act as resolves the
  uninitialised "no tenant" value, which fails closed with a
  `LatticeTenantAccessDeniedException` rather than silently defaulting. Because the
  effective id is tenant-owned, usage metering and quota admission attribute the
  traffic to the acting tenant - attribution only lines up with the acting tenant once
  the name is actually scoped.
- **Enumeration pruning.** `AddLatticeTenancy` likewise replaces the core's no-op
  `ITenantEnumerationFilter`, so a tree-id enumeration (the cluster-state tree
  catalog, the tag-index catalog, the view catalog, the in-cluster all-tree-ids
  read) is pruned to the trees the active tenant owns; platform-owned `_lattice_` and
  `sys-` ids are left in, for the catalog's system-tree switch and the per-entry
  authorization check to govern. Pruning is defence in depth rather than the
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
  still run unchanged.
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

### Placement follows the registry on every silo

A tenant tree's WAL placement is resolved once, when the tree is first registered,
from an in-memory placement view of the registry, and the resulting pin is
immutable. The view is kept current by the same epoch and lease, so a placement
change made through one silo reaches every silo before the write returns. Placement
cannot be confirmed against the registry instead - it is resolved inside the tree
registry's own turn, which a registry read could re-enter - so while a silo's view
is not authoritative a tenant tree's registration waits up to a fifth of
`PolicySnapshotLeaseDuration` (2 seconds by default) for the view to catch up, and
is otherwise refused with a retryable `TimeoutException` rather than pinned to a
placement that may be stale. Creating a tenant and then its trees works without a
retry: the wait covers the rebuild the tenant write triggers. Non-tenant trees are
never affected.

## Resource governance

Each tenant carries **aggregate quotas** across all of its trees, expressed by
`TenantQuotas`:

| Dimension | Property | Meaning |
|---|---|---|
| Durable bytes | `MaxBytes` | Aggregate durable size across the tenant's trees (WAL, snapshot, and leaf-state bytes). |
| Live keys | `MaxKeys` | Aggregate live-key count. |
| Resident memory | `MaxMemoryBytes` | Aggregate resident memory, metered as the summed serialized leaf and shard-root grain-state bytes of the tenant's trees. |
| Tree count | `MaxTreeCount` | Number of trees the tenant may own. |
| Request rate | `MaxOpsPerSecond` | Cluster-wide ops/sec ceiling. |
| Burst | `BurstPercent` | Percentage overage above the steady-state caps. |

A `null` cap on a dimension means unlimited on that dimension; a bounded cap, like
`BurstPercent`, must be non-negative, and a negative one is rejected with an
`ArgumentException` when the quotas are authored. The reserved
`default` tenant is permanently unbounded (it can never be given quotas), and every
newly created tenant starts with no caps until an operator sets them, so opt-in
never suddenly throttles an existing workload.

- **Compiled quota policy.** Steady-state enforcement uses a compiled policy
  snapshot with a monotonic epoch, refreshed off the `sys-tenant-*` change feed and
  evaluated synchronously in-memory with no I/O once warm - mirroring how the auth
  package answers `ILatticeDecisionEngine` from a compiled snapshot behind its own
  fail-closed access gate.
- **Burst and metering.** Usage at or below the steady-state cap is ordinary; usage
  above the cap and at or below `cap x (1 + burst%)` is admitted and **metered as
  overage** - a first-class, billing-ready signal distinct from ordinary usage;
  usage above `cap x (1 + burst%)` is refused with `LatticeQuotaExceededException`
  carrying the tenant id and dimension (`Dimension` is `bytes`, `keys`, `memory`, or
  `trees`, and `ops-per-second` for the request-rate budget). A tenant with burst `0` refuses as soon as
  usage exceeds the cap. The refusal gates new writes only: usage that already sits
  above the cap - for example after a cap is lowered - keeps accruing overage on
  every metering tick, in whichever band it sits.
  Over the [data gRPC binding](../lattice.api.data.grpc/README.md#quota-refusals)
  the refusal reaches a remote caller as `ResourceExhausted` carrying the breached
  dimension as a trailer; the tenant id is not added as a trailer (only the status
  message names it), because the caller asserted its own active tenant on the
  request.
- **Metering drives enforcement, on a cadence.** A footprint quota (bytes, keys,
  memory, and the tree count an ordinary write is checked against) is admitted
  against the tenant's *metered* usage, so it binds only once a usage sample lands;
  the request rate and the tree-count check at creation do not wait for one (both
  are covered below). Each silo
  runs a background metering cycle every
  `TenantUsageAccountingOptions.MeterInterval` (default 30 seconds) that walks each
  tenant's own trees - a bounded range scan over the tenant's `t/{tenant}/` key
  range, not a read of the whole catalog - samples their footprint, and rolls the
  result up into that tenant's per-cluster usage slot. Every id registered in that
  range is sampled, including the physical copy that a resize, a shadow-cutover
  restore or a schema remediation registers beside the tree it aliases; the
  logical id's report resolves the alias to that same copy, so an aliased tree's
  footprint, and its tree count, are counted twice. The reserved `default`
  tenant is skipped: it can carry no quotas, so it is never metered. Admission deliberately
  **fails open** for a tenant with no landed sample yet, so a cold silo never
  spuriously refuses; that means enforcement arms one cycle after a tenant first
  has usage. Setting `MeterInterval` to zero disables metering entirely and leaves
  footprint admission permanently open, which is only appropriate for a deployment
  running tenancy without resource governance.
- **A tenant's first non-empty sample always publishes.** Republishing a usage slot is gated
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
- **Reads are rate-admitted too, but never footprint-admitted.** A read is charged
  against `MaxOpsPerSecond` at the read plane, so a tenant cannot saturate a shared
  silo with reads that cost it nothing - the rate ceiling is what bounds a noisy
  neighbour, and leaving the entire read plane outside it left that ceiling
  bounding only half the traffic. The footprint dimensions are deliberately **not**
  applied to reads: refusing reads because a tenant is over its storage quota would
  trap it, unable to read the data it must delete to get back under. The charge is
  taken strictly **after** the access gate authorizes the read, never before,
  because the tenant is a caller assertion that only the gate validates - charging
  first would let an unauthorized caller drain a named victim's budget and read the
  victim's usage and ceiling back out of the refusal. The charge covers every read
  shape a tenant can drive, including the two that do not cross the data-plane
  seam: a **backup capture**, which is the largest tenant-triggerable read the
  platform offers, is charged once per capture at its own seam after the backup
  authorizer allows it; and a **snapshot cursor** page, which reads snapshot leaf
  grains directly, is charged per page. A turn the access gate never adjudicated -
  a system-origin turn, or authorised view-maintenance traffic - is never charged,
  because there is no validated tenant to charge it to.
- **Tree creation is admitted.** `MaxTreeCount` is charged where a tree is
  explicitly created, so the one dimension whose whole purpose is to bound tree
  creation binds at the point of creation. It is enforced once, at the
  tree-administration facade every explicit create funnels through, rather than
  additionally at the tenant-scoped facade above it: admission consumes a rate token,
  so evaluating it at both layers would bill a single create twice. The ceiling is
  checked against an authoritative count of the tenant's registered trees read at
  the moment of the create (every id registered under its `t/{tenant}/` prefix, so
  the physical copy beside an aliased tree counts as a tree of its own), not
  against the metered sample, so it binds even for a
  tenant that has never been metered; creates that read the count concurrently can
  each be admitted, so the cap can be overshot by at most the number of creates in
  flight. A tree the data plane registers implicitly on first use - for example the
  first write to a new name - never passes that check: it is bounded only by the
  metered tree count an ordinary write is admitted against, so implicit creation can
  overshoot the cap until the next metering sample lands.
- **The reserved `sys-` namespace is closed to tenants.** Tenant scoping composes
  the active tenant into a tree name, and deliberately passes an already-qualified
  name through uncomposed so it is never double-composed. The reserved `sys-`
  system-data namespace counts as already-qualified, which is right for the
  first-party add-ons that own those trees but meant a tenant naming one had the id
  returned **uncomposed** - and therefore global. Such a tree sits outside the
  `t/{tenant}/` prefix that per-tenant tree-count and footprint accounting
  enumerates, so it is invisible to the quotas meant to bound it; it is shared with
  every other tenant that picks the same name; and it can collide with an add-on's
  own store. A non-default tenant addressing that namespace outside a system-origin
  scope is now refused with `LatticeTenantAccessDeniedException`. A malformed
  `t/`-prefixed id carrying no tenant segment is refused on the same seam, for the
  same reason: it resolves to platform ownership, which the tenancy gate allows
  unconditionally. A **well-formed** foreign id such as `t/other/orders` is
  deliberately *not* refused here, because cross-tenant grants are real and only
  the gate can adjudicate them - the resolution layer cannot see grants, so it must
  not decide crossings.
- **Apply-path admission bypass, never isolation bypass.** As in core, the
  replication-apply and saga-apply paths bypass quota *admission* (they re-enter
  under a foreign/prepared scope) but never bypass the tenant *isolation* boundary.

### Enforcement scope (multi-cluster)

Quota admission runs under a `TenantEnforcementScope`. Today the scope is
cluster-wide rather than per tenant: every tenant is admitted under
`TenantUsageAccountingOptions.DefaultEnforcementScope`, read live, and the tenant
record carries no scope of its own (the resolver is a seam for a future per-tenant
override):

- **`GlobalConverged` (default).** For the slow-moving storage gauges (bytes, keys,
  memory, tree count) each cluster contributes its current local usage for the tenant
  to a per-cluster-slot state CRDT (a map from `ClusterId` to that cluster's latest
  sample). A cluster writes only its own slot and reads the whole map, so global
  usage is the sum-fold over every slot published so far - the fold is not filtered
  by the tenant's region statuses. The map holds another cluster's slot only once
  the `sys-tenant-usage` tree replicates between them; until then the global fold
  equals this cluster's own sample. Enforcement admits against the global fold,
  giving a single global budget rather than `limit x clusters`, with bounded
  transient overshoot. The monotonic overage tallies use grow-only `GCounter`s (one
  per bytes, keys, memory, and tree-count dimension), but they are not metered from
  the global fold: whatever the scope, each cluster accrues the overage of its own
  local usage above the tenant's whole steady-state cap into its own component, and
  the converged tally sums the components. Slots are republished on a
  cadence with hysteresis so continuous usage does not flood the replication path.
- **`PerCluster` (fallback).** Each cluster admits against only its own local usage
  slot, so effective global capacity is `limit x clusters`. Selectable, cluster-wide,
  by operators who prefer hard-partitioned capacity. The scope changes only which
  figure admission reads: every cluster still meters and publishes its slot on the
  same cadence, so it does not remove the usage-publishing traffic.

No enforcement scope introduces cross-cluster coordination or consensus -
`GlobalConverged` reads a convergent CRDT sum, it never locks or votes.

### Rate limiting

The `ops/sec` limit is always enforced **per-cluster**, whichever enforcement scope is
configured (a rate window is too short relative to replication lag for a
converged global count to be meaningful). It is enforced by silo-local, in-process token buckets - a per-silo
singleton limiter (not a grain) the data-plane entry path consults with a lock-free
token decrement - so the per-op hot path takes zero grain hops. A low-frequency
per-`(tenant, cluster)` budget coordinator divides the cluster rate across the live
silos at lease cadence (`O(silos)`, never `O(ops)`). `LatticeTenantRateLimiterOptions`
tunes that coordinator; none of its knobs touch the per-op hot path, so a
misconfiguration changes only how the cluster rate is split, never whether
enforcement stays lock-free. Only an active tenant with a positive
`MaxOpsPerSecond` gets a bucket: a `MaxOpsPerSecond` of `0` leaves the tenant as
unthrottled as `null` does. Each silo's share is floored at one operation per
second, and `BurstPercent` applies here too: a bucket may run about `BurstPercent`
percent of the silo's share (at least one operation when the percent is positive)
ahead of the steady rate, while a burst of `0` admits operations no closer together
than the share's steady spacing.

| Option | Type | Default | Meaning |
|---|---|---|---|
| `LeaseInterval` | `TimeSpan` | `30s` | How often the coordinator re-apportions each tenant's cluster rate across the live silos. A longer interval lowers coordination cost but widens the transient overshoot bound (lease interval times cluster rate); the default is sized for work backed by a whole-tree registry scan. A non-positive value falls back to the default, and the tick period is held to about 49.7 days (the longest period a timer accepts). |
| `LeaseCycleTimeout` | `TimeSpan` | `20s` | The bound on a single lease cycle. A cycle that exceeds it is cancelled and retried on a later tick, so a stalled tenant-registry read can never occupy the loop for longer than one interval. Clamped down to `LeaseInterval` if set at or above it, so the duty cycle stays bounded, and held to about 49.7 days (the longest delay a timer accepts). A non-positive value falls back to the default. |
| `MaxLeaseBackoff` | `TimeSpan` | `5m` | The ceiling the lease interval backs off to after consecutive cycle failures. The effective interval doubles per consecutive failure and resets to `LeaseInterval` on the first success, so a persistently unhealthy registry is probed at a decaying rate rather than hammered every tick. A value below `LeaseInterval` disables backoff; a non-positive value falls back to the default. |
| `RateSnapshotTtl` | `TimeSpan` | `2m` | How long a read of the registry's configured rates stays usable before the next cycle re-reads it. Configured rates change at administrative cadence, so caching them decouples the frequent re-apportionment of token buckets from the expensive whole-tree registry scan. The snapshot is stale-if-error, so a failed refresh apportions from the previous snapshot rather than pruning every tenant's bucket. A non-positive value falls back to the default. |
| `Apportionment` | `TenantRateApportionmentStrategy` | `Demand` | `Demand` leases demand-proportionally and degrades to static-even when no cluster-wide demand aggregate is available; `StaticEven` is the zero-coordination fallback that splits the rate evenly. The package ships no cluster-wide demand aggregator - its in-process demand exchange always reports none - so `Demand` apportions exactly as `StaticEven` does. |
| `DemandReserveFraction` | `double` | `0.2` | The fraction of the cluster rate that demand-proportional leasing reserves and splits evenly, guaranteeing an idle silo a non-zero floor so it can never be starved out of building demand. In `[0, 1]` (a value outside is clamped to that range); ignored under `StaticEven`, and whenever no cluster-wide demand aggregate is available (see `Apportionment`). |

A breach surfaces as a `LatticeQuotaExceededException` on the `ops-per-second`
dimension. Unlike the footprint dimensions it is **transient**: the same call
generally succeeds once the bucket refills, so a client should treat it as a
back-pressure signal to retry rather than as a durable capacity failure. Over the
[data gRPC binding](../lattice.api.data.grpc/README.md#quota-refusals) it reaches
a remote caller as a `ResourceExhausted` `RpcException` carrying the breached
dimension as a trailer, so a client can tell a retryable rate breach from a
footprint breach that will not clear on its own.

## Store write contention

The `sys-tenant-*` stores' write paths - `ITenantRegistry.PutAsync`, the
usage-slot publish, and the overage accrual - is an optimistic read-merge-write:
the store reads the tenant's record with its version, folds the change in with the
record's CRDT join, and writes back only if the version has not moved. A write that
loses that race re-reads (now seeing the competing write) and merges again, at once
and with no backoff, so a concurrent change is never dropped. After a small, fixed
number of lost races on the same tenant's record the store gives up and throws one
a public exception for that store, carrying the `Tenant` and the number of `Attempts`
it made. The retries absorb ordinary contention; each exception signals sustained
write contention on one tenant.

| Exception | Raised by | What happens |
|---|---|---|
| `TenantRegistryConcurrencyException` | `ITenantRegistry.PutAsync`, which every record change the [tenant-administration facades](../lattice.api.tenantadmin/README.md) make is written through (`DeleteAsync` removes a record outright and never raises it) | The change is not applied and the exception reaches the caller, which may retry. The tenant-administration gRPC binding has no arm for it, so a remote caller sees `Internal`. |
| `TenantUsageConcurrencyException` | The metering cycle's usage-slot publish | Caught and logged for that tenant: its overage accrual is skipped for the tick too, the rest of the pass continues, and the next tick retries. |
| `TenantOverageConcurrencyException` | The metering cycle's overage accrual | Caught and logged for that tenant: that tick's overage is not recorded - the tally is a per-tick sum, so it is not recovered later - and the next tick accrues as normal. |

## Region residency

Which regions a tenant lives in is a per-tenant, runtime-mutable choice layered on
top of the replication topology.

### The region sets

Most confusion about region residency comes from collapsing distinct sets into a
single notion of "where a tenant is". Each has a different owner and a different
surface:

| Set | Who controls it | Surface | What it means |
|-----|-----------------|---------|---------------|
| **Physical / routable** | Operator (deployment topology) | `lattice_list_regions` in the [MCP binding](../lattice.api.mcp/tools.md) | Every region the deployment actually has a route to. |
| **Allowed** | **Operator only** - a tenant admin cannot change it | `ILatticeTenantRegionAdmin.AuthorizeAllowedRegionsAsync` | The regions a tenant is permitted to place residency in. |
| **Resident** | **Tenant admin**, but only within the allowed set | `ILatticeTenantRegionAdmin.SetResidencyAsync` | The regions the tenant actually replicates to and is served from. |

The union of *allowed* and *resident* is the tenant's **actionable set**: the regions
it is in, plus the regions it may move into. A tenant admin never needs the physical
list - `SetResidencyAsync` refuses anything outside the allowed set - so tenant-facing
discovery never shows a tenant caller more than its actionable set plus the region
serving the call. See
[MCP security](../lattice.api.mcp/security.md#3b-tenant-scoped-region-discovery) for
how that scoping applies to region discovery, and
[Tenant-aware surfaces](#tenant-aware-surfaces) for what a tenant-asserting caller is
shown today.

- **Allowed vs resident.** A platform operator authorizes, per tenant, the *allowed*
  region set; the tenant's delegated admin selects its *residency set* (the subset it
  actually replicates to and is served from) within that allowed set. A tenant that
  has never configured residency - every newly created tenant - is treated as online
  in every region, the pre-residency admit-all behaviour, until it does.
- **Metadata everywhere, data to the residency set.** Tenant definitions
  converge to every region the registry tree replicates to, so any such region can
  fail-closed answer "is this tenant resident here?". A tenant's data is shipped to peers like any other replicated
  tree; the receiving region refuses (and dead-letters) a replicated write for a
  tenant that is not `Online` there, so the data lands only where the tenant is
  online. The same gate refuses and dead-letters a replicated write for a tenant the
  receiving region does not know or holds suspended.
- **Symmetric multi-master.** An `Online` region is a full read-write replica; there
  is no primary or leader. Enforcement ties in at the gate (a tenant not `Online` in
  the serving region is refused) and the replication apply path (a tenant's
  replicated writes land only in a region where it is `Online`).

### Every silo applies a residency change

Each silo answers "is this tenant online here?" from an in-memory residency view of
the registry, and the change feed that refreshes it fires only on the silo that
committed the write. So the view is kept current by the same cluster-wide epoch and
lease as the tenant-policy snapshot (see [Isolation model](#isolation-model), "Every
silo sees the change"): a residency change made through one silo reaches every silo
before the write returns. While a silo's view is not authoritative - it has been
told of a change it has not yet compiled, or its lease has lapsed - the tenant gate
and the replication isolation gate confirm the tenant's residency against its
registry record instead, so a tenant drained or taken offline through any silo is
refused on every silo at once, and one brought online is admitted at once. A
residency check that cannot be confirmed is refused. The steady state stays an
in-memory lookup.

### Lifecycle states

Each region carries one `TenantRegionStatus` per tenant, readable through
`GetTenantRegionStatusAsync`:

| Status | Resident? | Meaning |
|--------|-----------|---------|
| `None` | No | No relationship. An *allowed but not yet entered* region reports `None`. |
| `Provisioning` | Yes | The region has been added to the residency set and is not yet serving. |
| `Backfilling` | Yes | The step between `Provisioning` and `Online`, reserved for copying existing data into the region; not yet serving. |
| `Online` | Yes | A full read-write replica, and the only status in which this region serves the tenant. |
| `Draining` | No | The region has been dropped from residency and no longer serves. |
| `Offline` | No | The step after `Draining`: drained, and no longer serving. |
| `Removed` | No | Terminal: the removal is complete. |

The **resident set** is exactly the rows whose status is `Provisioning`,
`Backfilling`, or `Online`. A region does not serve the tenant until it reaches
`Online`, and once any region status is set the tenant is served only in a region
where its status is exactly `Online`.

`SetResidencyAsync` applies only the first step of each path: `Provisioning` for an
added region and `Draining` for a dropped one. The later steps (`Provisioning` ->
`Backfilling` -> `Online`, and `Draining` -> `Offline` -> `Removed`) are single-step
promotions, and the two paths are completed differently.

**The remove path completes on its own.** With the
[tenant-admin control API](../lattice.api.tenantadmin/README.md#registration)
registered, each silo watches its own serving region (its cluster id) and, when a
tenant's status there becomes `Draining`, advances it to `Offline` and then to
`Removed` without any caller. Nothing needs to be waited for first: a region stops
serving a tenant and stops admitting its replicated writes the moment its status
leaves `Online`, and outbound shipping of the writes it accepted while online does not
depend on the status.

The drained region must first **observe the registry change**, and its completion
must replicate back to the origin. Configure a connected, bidirectional replication
topology on every tenancy region; the automatically enrolled definition registry
is control-plane metadata and is not filtered by a tenant's residency. Do not
independently runtime-enrol it in each region: concurrent runtime enrolments can
be ambiguous and override the static floor. Existing runtime overrides must be
removed or resolved to the same `LwwRegister` mode.

A registry snapshot import publishes a logical-tree alias cutover. Tenancy observes
that cutover and advances the same cluster policy epoch as a registry mutation,
invalidating policy, residency and placement snapshots on every local silo. This
lets the drained region observe imported lifecycle changes even when bootstrap
writes only to a shadow copy of the registry.

A lasting `Draining` means the origin is still awaiting the region's confirmation:
check that the drained region's silos run the tenant-admin API and that
`sys-tenant-registry` ships in **both** directions. This status is not evidence
that the remote region has stopped serving: a disconnected region may still hold
`Online` until it observes the removal. Explorer explains this uncertainty rather
than claiming a remote acknowledgment. A decommissioned region that never runs
again cannot acknowledge the drain and remains `Draining`.

**The add path needs an operator step.** No shipped component backfills a region a
tenant is added to. While the tenant is not `Online` in a region, that region refuses
and dead-letters every replicated write for it - including anything a backfill would
apply, because dead-letter replay and snapshot re-seed go through the same gate - so
an added region lacks whatever was written while it was not admitting the tenant, and
cannot be filled in until it is `Online`. Promoting it automatically would declare an
incomplete replica online without anyone deciding to, so nothing does: an added region
stays at `Provisioning`, and a tenant whose residency has been set is served in no
region until an operator advances one. Where the region was admitting the tenant's
writes up to the add - residency being configured for the first time, so every region
was admit-all until then - it misses at most the writes shipped since, and advancing
it is enough. Otherwise advance it and then recover the gap: replay the region's
dead-lettered writes for the tenant's trees (see the
[dead-letter queue](../lattice.replication/dead-letter-queue.md)), or let the
[anti-entropy digest probe](../lattice.replication/anti-entropy-digest-probe.md)
repair it where that is enabled. Advance the region one step at a time,
`Provisioning` -> `Backfilling` -> `Online`, from host code on a silo:

```csharp verify
using Orleans.Lattice.Tenancy;

// Advances a region one legal lifecycle step and returns its committed status.
// Call it twice to take an added region from Provisioning to Online.
static async Task<TenantRegionStatus> PromoteRegionAsync(
    ITenantRegistry registry, TenantId tenant, string regionId, string clusterId, CancellationToken cancellationToken)
{
    var record = await registry.GetAsync(tenant, cancellationToken)
        ?? throw new InvalidOperationException($"Tenant '{tenant}' is not registered.");

    if (!record.TryPromoteRegionStatus(regionId, clusterId, out _))
    {
        return record.GetRegionStatus(regionId);
    }

    var committed = await registry.PutAsync(record, cancellationToken);
    return committed.GetRegionStatus(regionId);
}
```

`TenantRecord.TryPromoteRegionStatus` applies only the next legal step
(`TenantRegionLifecycle.TryNextPromotion`) and is a no-op at `Online`, `Removed`, and
`None`. It stamps the promotion as the immediate successor of the status it read, not
at wall-clock now, so it never overwrites a residency change a tenant admin commits
after that read. Prefer it to `TenantRecord.SetRegionStatus`, which applies any status
whose stamp supersedes the current one and checks neither the step nor that ordering.

Quota accounting does not follow these statuses: the `GlobalConverged`
fold sums every cluster slot the tenant has published, whatever the status of that
slot's region.

### Invariants

Enforced by `ILatticeTenantRegionAdmin` and never bypassed by a transport binding:

- **Residency is always a subset of allowed.** Setting residency to a region outside
  the allowed set is refused with `TenantRegionNotAllowedException`.
- **The last resident region can never be removed.** Narrowing residency to the empty
  set is refused with `TenantLastRegionException`.
- **A region a tenant is still resident in cannot be revoked.** Drain it with
  `SetResidencyAsync` first, then revoke it with `AuthorizeAllowedRegionsAsync`.
- **An unknown tenant fails closed** on every operation: with
  `TenantNotFoundException` for a platform operator, and with the same
  `LatticeAuthorizationDeniedException` as any other refusal for a non-operator
  caller, so a tenant admin cannot probe for another tenant's existence.

Region residency is administered through the
[`ILatticeTenantRegionAdmin`](../lattice.api.tenantadmin/README.md#ilatticetenantregionadmin)
control-plane facade, which is reachable
[over gRPC](../lattice.api.tenantadmin.grpc/README.md#region-residency-rpcs) and
[through MCP tools](../lattice.api.mcp/tools.md#tenant-region-residency-lattice_tenant_authorize_regions-lattice_tenant_set_residency-lattice_tenant_region_status).

## Delegated tenant access administration

A tenant usually stands for an organisation, so this opt-in feature lets a tenant's
own administrators decide who belongs to the tenant, which groups exist inside it,
and who may do what on its trees, without a platform operator in the loop and without
reaching outside the tenant. It adds:

- **Tenant groups** - membership groups a tenant's administrators create and manage
  inside the tenant.
- **Tenant members** - a member set of users and groups that may act as the tenant on
  the data plane.
- **Tenant-tier rules** - authorization rules a tenant's administrators write on the
  tenant's own trees, evaluated beneath every operator rule.

Tenant administrators manage these surfaces through `ILatticeTenantDirectoryAdmin` and
`ILatticeTenantPolicyAdmin` (see
[`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md#delegated-tenant-access-administration)),
in-process, over gRPC, as MCP tools, or from the Explorer's
[tenant Access pages](../lattice.explorer/tenant-scope.md#tenant-access).

### Turning it on

The feature is off by default. Enable it with
`LatticeTenancyOptions.DelegatedAccessAdministrationEnabled`:

```csharp verify
using Orleans.Lattice.Tenancy;

siloBuilder.AddLatticeTenancy(options =>
{
    options.DelegatedAccessAdministrationEnabled = true;
});
```

Each silo logs its value once at start-up, at Information level, in a line beginning
`Lattice tenancy posture: DelegatedAccessAdministrationEnabled={value}.`. The auth
facade's `GetAccessModelAsync` reports it as
`AccessModelDescriptor.DelegatedTenantAccessAdministrationEnabled`, and
`ILatticeTenantPolicyAdmin.GetPostureAsync` reports it per tenant as
`TenantAccessPosture.Enabled`.

While the flag is off:

- Member entries, group entries in an admin set and tenant-tier rules are **inert**.
  Active-tenant validation is the exact-subject-id admin check it has always been,
  the compiled tenant-policy snapshot builds no member or group index, and the
  authorization engine never enters the tenant rule layer. Tenant group records are
  kept, and an operator rule or app role binding that names one is still honoured
  through the group's recorded members, as the claim filter below explains.
- After the caller is authorized, every delegated access operation except
  `GetPostureAsync` is refused with `TenantAccessAdministrationDisabledException`.
- **Nothing is deleted.** Existing groups, member entries and tenant-tier rules are
  kept and become effective again when the flag is turned back on.

The flag is read through the options monitor, so a change applies without a restart:

- It invalidates the silo's compiled tenant-policy snapshot and schedules a rebuild.
  Until the rebuild lands, a request that acts as an asserted active tenant is
  confirmed against the tenant registry under the new value, as for any other
  registry change (see
  [Membership, status and grant changes take effect at once](#isolation-model)).
  Member and group entries are honoured only while both the snapshot and the live
  flag have the feature on, so turning it off takes effect at once, before the
  rebuild.
- It clears the membership package's subject-resolution cache, so no subject
  resolved before the change outlives it.

### Tenant groups

A tenant group's id has the reserved grammar `t/{tenant}/{name}`, owned by the core
type `LatticeTenantGroupId` (`Compose`, `Parse`, `TryParse`, `IsTenantGroupId`,
`Tenant`, `Name`, `Value`). `{tenant}` is a valid tenant other than `default`, and
`{name}` is 1 to 63 (`LatticeTenantGroupId.MaxNameLength`) characters of lower-case
ASCII letters, digits, `-`, `_` and `.`. Administrators name a group by its local
`{name}`; the facades compose the full id from the tenant the call names.

Tenant groups live in the same `sys-membership-*` trees as cluster groups, so they
replicate to every region with the other system trees and are not bound by the
tenant's residency. The whole `t/` group namespace is reserved to the tenant tier:

- The cluster auth facade refuses to create a group whose id starts with `t/`
  (`LatticeTenantOwnedGroupException`).
- Whenever tenancy is registered, **whatever the flag**, a group id starting with
  `t/` that an identity provider asserts (a token claim, a group overage, or a
  `ClaimToGroups` projection) is stripped before group expansion, so no
  identity-provider administrator can join a tenant group by asserting its id.
  Groups the membership directory itself records are kept. The filter does not
  follow the flag because operator rules and app role bindings that name a tenant
  group are honoured whatever the flag: were it switched off by the flag, a token
  asserting `t/{tenant}/{name}` could match them after a rollback, or on a silo whose
  flag has not yet changed.
- A tenant group may contain users, groups of the same tenant and cluster groups, but
  may never become a member of a cluster group or of another tenant's group. The
  membership directory enforces this for every caller, operators included; see
  [Tenant groups](../lattice.membership/README.md#tenant-groups) in the membership
  guide.

### Tenant members and group admins

A `TenantRecord` carries a member set (`MemberSubjects`, `MemberSubjectCount`,
`HasMemberSubject`, `AddMemberSubject`, `RemoveMemberSubject`) beside its admin set.
Its entries are users, the tenant's own groups and cluster groups; the reserved
`default` tenant accepts none.

While the feature is on, a subject may act as tenant T when its id, or any of its
resolved transitive groups, is an entry of T's admin set or member set
(`TenantRecord.IsMember`; admins are implicitly members). Being a member only passes
the tenant gate: everything a member may then do is decided by rules, default-deny.
The admin set accepts the tenant's own groups and cluster groups too
(`TenantRecord.IsAdmin`), so a group of people can administer a tenant. A group entry
counts as one entry for the guard that refuses removing the last one. An entry naming
another tenant's group, or a malformed `t/` id, never counts, even when it arrives by
replication or restore.

### Tenant-tier rules

A tenant administrator writes rules only for the tenant's own trees
(`t/{tenant}/...`), never for its app-owned trees (`t/{tenant}/a/...`, which app roles
govern), a reserved or `sys-` tree, or another tenant's tree. A rule may cover only
the data-plane operations (`LatticeAuthOperations.All`), at tree, prefix or key
scope, or at the **tenant-wide** scope `LatticeScope.TenantWide(tenant)`, which
stands for every tree the tenant owns. Its subject may be a user, one of the tenant's
own groups or a cluster group. The stored rule id carries the reserved prefix
`tenant:{tenant}:` (`LatticeTenantRuleIds`), which only the tenant facade writes;
operators can list and remove these rules but not write them.

Operator rules are evaluated first, and a matched operator verdict is final. Only
when no operator rule matches does the tree owner's tenant layer decide, so a tenant
allow can never carve a hole in an operator deny, and a tenant deny can never revoke
an operator allow. See [The tenant rule layer](../lattice.auth/tenant-layer.md).

### Caps

Tenant groups, membership edges and tenant-tier rules live in trees every tenant
shares, so `TenantQuotas` access-cap dimensions bound one tenant's footprint:

| Property | Default | Bounds |
|---|---|---|
| `MaxGroups` | 500 (`DefaultMaxGroups`) | The tenant's groups. |
| `MaxMembershipEdges` | 10000 (`DefaultMaxMembershipEdges`) | Membership edges whose parent is one of the tenant's groups. |
| `MaxMemberSubjects` | 5000 (`DefaultMaxMemberSubjects`) | Entries in the tenant's member set. |
| `MaxTenantRules` | 1000 (`DefaultMaxTenantRules`) | The tenant's tenant-tier rules. |

Unlike the resource dimensions above, a `null` access cap means its **default, never
unbounded**: read the cap in force through `EffectiveMaxGroups`,
`EffectiveMaxMembershipEdges`, `EffectiveMaxMemberSubjects` and
`EffectiveMaxTenantRules`. The caps take no burst and play no part in `IsUnbounded`.
An operator sets them through `SetTenantQuotasAsync`, like any other quota. An
addition that would exceed one is refused through `TenantAccessCaps.AdmitAddition`,
which throws `LatticeQuotaExceededException` with `Dimension` set to one of
`TenantAccessCaps.GroupsDimension` (`tenant-groups`), `MembershipEdgesDimension`
(`tenant-membership-edges`), `MemberSubjectsDimension` (`tenant-member-subjects`) or
`TenantRulesDimension` (`tenant-rules`). That check runs before the write; the
delegated access facades count again after it, and a call that a concurrent addition
left over the cap withdraws its own addition and is refused the same way (see
[Caps: verify and compensate](../lattice.api.tenantadmin/README.md#rules-both-facades-share)).

### Isolation and the default tenant

- **No bypass of tenant isolation.** Membership and tenant-tier rules never bypass the
  tenant gate or the cross-tenant grant protocol. Reaching another tenant's trees
  still needs an active cross-tenant grant, and the tenant layer that then applies is
  the tree owner's.
- **The reserved `default` tenant is excluded.** It gets no tenant groups, members or
  tenant-tier rules, its tree ids carry no `t/` segment for the tenant layer to
  govern, and the delegated access facades refuse it with
  `ReservedTenantOperationException` (while the feature is off, every operation but
  `GetPostureAsync` is refused as disabled first). Its access stays
  operator-administered.

### Deleting a tenant purges its access data

Deleting a tenant (`ITenantRegistry.DeleteAsync`, which the tenant-admin facade's
delete calls after cascading the tenant's trees) first purges the tenant's access
data: its tenant-tier rules, then its tenant groups with their membership edges in
both directions. Only then is the registry record removed, taking the member set with
it. The purge runs whether or not the feature is enabled, and is idempotent: a delete
that fails part-way leaves the record in place, and running it again completes the
purge. A host that replaced the membership directory or the authorization policy
store with its own implementation cannot be purged, so deleting a tenant there fails
closed with `InvalidOperationException` and keeps the record.

## Observability

`TenantObservabilityOptions` (default `PublishGauges = true`, `PublishInterval` 30
seconds) publishes per-tenant gauges - current usage against each quota dimension and
the metered overage tallies - on a fixed cadence, so an operator can see per-tenant
consumption and headroom. Separately, every registered
`ITenantRegionStatusChangeListener` is notified of each tenant's local-region status
transition - any change of `TenantRegionStatus`, such as a region entering
`Provisioning` or `Draining` - once the residency snapshot has observed it.

Every instrument is an **observable gauge** on the `orleans.lattice.tenancy` meter
(`LatticeTenantMetrics.MeterName`). Each series carries a single `tenant` tag
(`LatticeTenantMetrics.TagTenant`): the owning tenant's id on every per-tenant
series (`default` for the reserved legacy-adoption tenant), and the reserved
`_platform_` sentinel on the cluster-aggregate series,
`orleans.lattice.tenancy.tenants`. The per-tenant series cover every tenant in the
registry. Set `PublishGauges = false` to publish none of them.

| Instrument | Unit | Meaning |
|---|---|---|
| `orleans.lattice.tenancy.tenants` | `{tenant}` | Cluster-aggregate count of tenants in the warm usage index. It belongs to the platform rather than to any tenant, so it carries the reserved `_platform_` sentinel as its `tenant` value rather than being left untagged - a tenant-scoped matcher then excludes it by stating so, not by accident of absence. |
| `orleans.lattice.tenancy.usage.bytes` | `By` | The tenant's current aggregate durable bytes. |
| `orleans.lattice.tenancy.usage.keys` | `{key}` | The tenant's current aggregate live-key count. |
| `orleans.lattice.tenancy.usage.memory_bytes` | `By` | The tenant's current aggregate resident memory. |
| `orleans.lattice.tenancy.usage.trees` | `{tree}` | The number of trees the tenant currently owns. |
| `orleans.lattice.tenancy.quota.bytes` | `By` | The tenant's steady-state `MaxBytes` ceiling. |
| `orleans.lattice.tenancy.quota.keys` | `{key}` | The tenant's steady-state `MaxKeys` ceiling. |
| `orleans.lattice.tenancy.quota.memory_bytes` | `By` | The tenant's steady-state `MaxMemoryBytes` ceiling. |
| `orleans.lattice.tenancy.quota.trees` | `{tree}` | The tenant's steady-state `MaxTreeCount` ceiling. |
| `orleans.lattice.tenancy.quota.burst_percent` | `%` | The tenant's `BurstPercent` headroom above its bounded ceilings. |
| `orleans.lattice.tenancy.overage.bytes` | `By` | Converged, durable metered byte overage accrued above the byte ceiling. |
| `orleans.lattice.tenancy.overage.keys` | `{key}` | Converged, durable metered key overage. |
| `orleans.lattice.tenancy.overage.memory_bytes` | `By` | Converged, durable metered resident-memory overage. |
| `orleans.lattice.tenancy.overage.trees` | `{tree}` | Converged, durable metered owned-tree overage. |

Each instrument name is also a public constant on `LatticeTenantMetrics`
(`TenantsName`, `UsageBytesName`, `UsageKeysName`, `UsageMemoryBytesName`,
`UsageTreesName`, `QuotaBytesName`, `QuotaKeysName`, `QuotaMemoryBytesName`,
`QuotaTreesName`, `QuotaBurstPercentName`, `OverageBytesName`, `OverageKeysName`,
`OverageMemoryBytesName`, and `OverageTreesName`), and the `LatticeTenantMetrics.Meter`
instance is public, so a listener can subscribe by reference rather than by name.

The ceiling gauges (`quota.bytes`, `quota.keys`, `quota.memory_bytes`, and
`quota.trees`) emit a measurement **only for a tenant whose corresponding dimension
is bounded** - an unbounded (`null`) ceiling contributes no series at all, so "no
series" reads as "unlimited on that dimension" rather than "zero".
`quota.burst_percent` is emitted for every tenant, `0` when it has no burst
allowance. Usage gauges reflect the last landed metering sample (see
`MeterInterval` above), folded across every published cluster slot whatever the
enforcement scope; a registered tenant with no sample yet reports zero usage
rather than no series, so a zero reading can also mean "not yet metered" - and the
reserved `default` tenant, which is never metered, always reads zero usage. The
`overage.*` gauges are the billing-ready tallies: they are grow-only converged sums,
not instantaneous readings.

The same figures are readable in-process through public seams the package
registers: `ITenantOverageBilling` returns the converged metered overage for one
tenant or every tenant, for a billing consumer to poll; `ITenantObservabilityView`
returns the caller's own validated active tenant's usage, quota, burst, and overage
snapshot, and every tenant's only under an explicit
`TenantObservabilityScope.ClusterWide(subject)` scope whose subject validates as a
platform operator (anything else falls back to the active tenant); and `ITenantUsageReader` reads one tenant's usage by id
with no visibility check of its own, so its consumer must authorize the caller
against that tenant first.

`MaxOpsPerSecond` has no gauge: the rate budget is enforced from silo-local token
buckets rather than from a published aggregate, so a breach is observed through the
`ops-per-second` `LatticeQuotaExceededException` rather than a series.

These instruments are charted by the bundled **Per-Tenant Observability** Grafana
dashboard (`LatticeDashboardKind.Tenancy`), which offers a templated `tenant`
variable so a panel can be scoped to a single tenant or to every tenant. See
`docs/lattice.dashboards/metrics-to-panel-map.md` for the instrument-to-panel
mapping. To consume them directly instead, subscribe to the
`orleans.lattice.tenancy` meter from your OpenTelemetry exporter.

### The derived `tenant` dimension

Beyond this meter, every instrument that Orleans.Lattice and its add-on packages
publish carries the same derived `tenant` tag (`LatticeTenantLabel.TagTenant`). It
is emitted on tenancy-on and tenancy-off clusters alike, so a dashboard query is
byte-identical in both deployment modes, and it is derived from the tree id rather
than read from the caller:

| Tree id | `tenant` value |
|---|---|
| A well-formed `t/{tenantId}/{name}` id | The owning tenant's id. |
| A bare, unsegmented id (every id on a tenancy-off cluster) | `default` (`LatticeTenantLabel.DefaultTenant`), the legacy-adoption tenant. |
| A `_lattice_` or `sys-` id, or a malformed `t/` id | `_platform_` (`LatticeTenantLabel.PlatformTenant`). |

An instrument with no tree dimension carries `_platform_` too, unless its
measurement is attributable to a tenant some other way - the per-tenant gauges on
this meter carry the tenant id directly. `default` is a real, queryable tenant;
`_platform_` names platform state that no tenant may see. The sentinel is a tag
value rather than an absent label, and it opens with an underscore - which the
tenant-id grammar forbids - so it can never collide with a real tenant. A
tenant-scoped matcher such as `tenant="acme"` therefore excludes platform series
by construction, and a tree-id regex is not a substitute: `tree!~"^t/.*"` would
also match the `_lattice_` and `sys-` platform trees.

## Security

- **Fail-closed everywhere.** Every enforcement seam denies on an unmatched request.
  Tenant data isolation and tenant lifecycle administration are each independent of
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
  the silo) or a subject **explicitly promoted to access-administrator** through the
  access-administration delegation - by a bootstrap administrator, or by an existing
  delegate, since a delegate may delegate further. It does **not**
  mean "any authenticated caller," "any caller with a broad data-plane grant," or "a
  tenant admin." The reason a tenant admin cannot perform this act is structural, not a
  matter of degree: authoring *any* authorization rule is a write to the reserved
  `sys-auth-*` policy store, which requires whole-tree `Admin` on that store - a
  control-plane capability held only by the operators just defined. A tenant admin's
  authority is only its membership of a tenant's admin-subject set, which the
  tenant-tier facades check on that tenant's own record: it confers nothing on the
  policy store and nothing over a tenant whose set does not name it, so a tenant admin
  can neither reach the policy store to author a registry-read rule (for itself or
  anyone else) nor act for another tenant. With
  [delegated tenant access administration](#delegated-tenant-access-administration)
  on, a tenant admin can author tenant-tier rules, but only through the tenant facade
  and only over its own tenant's trees, never a reserved or `sys-` tree, so the
  registry stays out of reach. Direct writes to `sys-tenant-*` are likewise refused off the
  system-origin path by the reserved-prefix write guard. Consequently, granting
  cross-tenant registry visibility always requires a deliberate operator decision to
  author (or delegate the authority to author) that rule; a caller acting purely as a
  tenant admin has no path to it.
- **Two-tier governance.** A platform-operator capability (cluster-wide) performs
  tenant lifecycle, quota and burst changes, and allowed-region authorization, and
  may act on any tenant's behalf. A delegated per-tenant admin capability (scoped to
  one tenant) manages that tenant's trees, admin subjects, schema, region residency,
  and its own side of a cross-tenant grant (offering its data, or approving,
  rejecting, or revoking a grant it is party to) strictly within the granted quota
  and allowed-region set - it can neither raise its own caps, widen its allowed
  regions, nor reach another tenant.
- **Enable-gated.** No tenant can be created unless the feature is enabled, and every
  tenant-administration control tool is contributed only when the host opts control
  in (`EnableTenantAdminControlTools`).

## Tenant-aware surfaces

Tenancy also reaches the operator- and agent-facing surfaces. Each is a module the
host registers (below); once registered, it keys its behaviour off whether tenancy is
actually present rather than off a separate opt-in flag, so a deployment without
tenancy keeps a byte-for-byte-unchanged UI and tool surface.

- **Explorer.** The Explorer's web head (`AddLatticeExplorerWeb`) always registers
  its tenant view (`AddExplorerTenantView()`). With tenancy on, the active tenant
  becomes the root node of every
  tenant-scoped address (`/t/{tenant}/...`), typing `t/` in the address line
  re-roots the current address at another tenant the caller may reach, and the
  Tenancy area serves the operator's tenant directory and each tenant's own pages.
  An address for another tenant is a switch request, authorized fail-closed through
  the operator gate; a refusal redirects to the active tenant with a warning. A
  deployment without tenancy shows no tenant root anywhere. See
  [Tenant scope](../lattice.explorer/tenant-scope.md).
- **MCP.** When tenancy is wired and the self-awareness module is registered
  (`AddTenantSelfAwarenessTools()`, which the split-head remote registration calls
  itself whenever a tenant-admin endpoint is configured; the module then self-gates on
  the tenant self-service facade and needs no flag), the MCP server contributes a
  read-only tenant
  self-awareness tool group - `lattice_tenant_current` (the tenant the caller is
  operating as), `lattice_tenant_list` (the tenants the caller may access), and
  `lattice_tenant_get` (one accessible tenant's lifecycle and per-region residency).
  Each is scoped fail-closed to the caller's subject: an anonymous caller lists
  nothing, and an inaccessible tenant is indistinguishable from an absent one. The
  tenant-admin control tools - the lifecycle and quota tools
  `lattice_tenant_create`, `lattice_tenant_suspend`, `lattice_tenant_resume`,
  `lattice_tenant_delete`, and `lattice_tenant_set_quotas`, plus the
  region-residency tools `lattice_tenant_authorize_regions`,
  `lattice_tenant_set_residency`, and `lattice_tenant_region_status` - remain
  separately gated behind `EnableTenantAdminControlTools`. Every tool in that
  group is annotated destructive and non-read-only except
  `lattice_tenant_region_status`, which is a read. Region discovery is
  tenant-scoped too, and fails closed: `lattice_list_regions` honours a non-default
  tenant assertion only after re-validating it against the caller's own membership,
  and then advertises that tenant's actionable set plus the region serving the call,
  annotated with its standing; an assertion that does not validate is shown the
  serving region alone, with no tenant annotation. In the shipped registrations the
  discovery tool cannot validate an assertion - a co-hosted head carries no caller
  credential into it, so the caller resolves as anonymous, and a split head has no
  resolver able to validate one - so a tenant-asserting caller is currently shown
  only the serving region. See
  [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md).

## Configuration reference

Only `LatticeTenancyOptions` is bound by the registration delegate:
`AddLatticeTenancy(Action<LatticeTenancyOptions>?)` and
`ConfigureLatticeTenancy(Action<LatticeTenancyOptions>)` each accept that type and no
other. Every other options type below is a plain registered option, so configure it on
the service collection directly - for example
`services.Configure<TenantUsageAccountingOptions>(o => o.MeterInterval = TimeSpan.FromSeconds(10))`.

### `LatticeTenancyOptions`

| Property | Type | Default | Meaning |
|---|---|---|---|
| `HistoryRetentionMode` | `HistoryRetentionMode` | `MetadataOnly` | Retention mode for the durable per-key history captured on the `sys-tenant-registry` tree; the usage and overage trees keep none. History is never disabled by default. |
| `HistoryRetentionWindow` | `TimeSpan?` | `null` | Age after which a registry history revision row expires; `null` means no age bound. Must be strictly positive when supplied. |
| `EnableDurableHistoryView` | `bool` | `true` | Whether to create the durable history materialised view (`sys-tenant-registry-history`) over the `sys-tenant-registry` tree. |
| `SeedDefaultTenant` | `bool` | `true` | Whether to seed the reserved `default` tenant (unbounded quota) at startup when absent. The seed is create-if-absent, so it never clobbers an operator's later edits. |
| `DelegatedAccessAdministrationEnabled` | `bool` | `false` | Whether [delegated tenant access administration](#delegated-tenant-access-administration) is enabled: tenant groups, tenant member sets, group entries in an admin set, and tenant-tier rules. While `false` the facades refuse to change any of them, member entries, group entries in an admin set and tenant-tier rules are inert, and nothing is deleted. Read live: a change invalidates the compiled tenant-policy snapshot and clears the subject-resolution cache, with no restart. |
| `PolicySnapshotLeaseDuration` | `TimeSpan` | `10s` | How long a silo may treat its compiled tenant-policy, residency and placement snapshots as authoritative without renewing its lease from the cluster-wide tenant-policy epoch; renewed every third of this. While the lease is lapsed, every request that acts as an asserted active tenant (on its own trees or across a cross-tenant grant), residency checks and inbound-replication tenant checks are confirmed against the registry or denied, and a tenant tree's registration waits up to a fifth of this for the placement view to become authoritative before it is refused. It is also the most a registry write can be held open (about 1.1 times this) when a silo cannot be reached, or just after the epoch restarts, so keep it well below the Orleans response timeout. Must be strictly positive and at most `0xFFFFFFFE` milliseconds (about 49.7 days). |

### `TenantUsageAccountingOptions`

Governs usage metering and the quota-enforcement scope every tenant is admitted under.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultEnforcementScope` | `TenantEnforcementScope` | `GlobalConverged` | The [enforcement scope](#enforcement-scope-multi-cluster) every tenant's quota admission runs under, read live; there is no per-tenant override yet. |
| `PublishMinAbsoluteDelta` | `long` | `65536` (`64 * 1024`) | Absolute movement, in the sampled unit, below which a usage republish is damped. A tenant's *first* non-empty publish is never damped. |
| `PublishMinRelativeDelta` | `double` | `0.05` | Relative movement, as a fraction of the last published value, below which a usage republish is damped. Per dimension the effective threshold is the larger of `PublishMinAbsoluteDelta` and this fraction of that dimension's last published value, and the slot republishes when any one dimension moves by at least its threshold. A negative value of either knob is treated as zero. |
| `MeterInterval` | `TimeSpan` | `30s` | The per-silo metering cycle that samples each tenant's footprint and rolls it into that tenant's per-cluster usage slot. Zero or a negative value disables metering entirely, which pins footprint admission in its documented fail-open branch so an authored footprint quota never binds (the request rate and the tree-count check at creation still apply). Re-read before every cycle, so a reload to zero or a negative value stops a running loop. A value above about 49.7 days (`0xFFFFFFFE` milliseconds, the longest delay a timer accepts) is clamped to it. |

### `TenantObservabilityOptions`

Governs the per-tenant gauges described under [Observability](#observability).

| Property | Type | Default | Meaning |
|---|---|---|---|
| `PublishGauges` | `bool` | `true` | Whether to publish the per-tenant observable gauges on the `orleans.lattice.tenancy` meter. `false` leaves the meter inert and skips the periodic overage scan. |
| `PublishInterval` | `TimeSpan` | `30s` (`DefaultPublishInterval`) | How often the publisher re-samples the warm usage index and the overage billing seam. A non-positive value is treated as the default, and a value above about 49.7 days (the longest period a timer accepts) is clamped to it. |

### `LatticeTenantRateLimiterOptions`

Governs how a tenant's cluster-wide `MaxOpsPerSecond` is divided across live
silos. See [Rate limiting](#rate-limiting) for the full table.

## See also

- [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md) - the
  transport-agnostic tenant-administration and region-residency control facades.
- [`Orleans.Lattice.Api.TenantAdmin.Grpc`](../lattice.api.tenantadmin.grpc/README.md) -
  the code-first gRPC binding and remote client for the tenant-administration facade.
- [`Orleans.Lattice.Explorer`](../lattice.explorer/README.md) - the web UI whose
  tenant view roots each tenant-scoped address at the active tenant and offers a
  platform operator a tenant switcher.
- [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md) - the MCP server that
  contributes the read-only tenant self-awareness tools.
- [`Orleans.Lattice.Auth`](../lattice.auth/README.md) - the authorization gate the
  tenant boundary is enforced at, and [the tenant rule layer](../lattice.auth/tenant-layer.md)
  that evaluates tenant-tier rules.
- [`Orleans.Lattice.Membership`](../lattice.membership/README.md) - the identity layer
  that resolves the caller subject a tenant's admin-subject set is matched against.
- [MultiTenancy sample](../../samples/MultiTenancy/README.md) - a runnable end-to-end
  walkthrough of opt-in wire-up, tenant tree naming, the tenant lifecycle, and the
  fail-closed control plane.

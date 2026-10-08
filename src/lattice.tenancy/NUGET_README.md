# Orleans.Lattice.Tenancy

Optional, opt-in **multi-tenancy** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Partitions a
deployment into keyspace-isolated tenants, each with its own trees, quotas, and
optional region residency, across a single cluster or many - **byte-for-byte
identical to the pre-tenancy behaviour, and zero runtime cost, when
`AddLatticeTenancy` is not registered**.

## Design

`AddLatticeTenancy()` supplies the durable, conflict-free-mergeable definition of
every tenant: status, resource quotas and burst allowance, placement binding,
tenant-admin subjects, cross-tenant grants, and its allowed regions and per-region
residency status. The `ITenantRegistry` dogfoods
the reserved `sys-tenant-*` Lattice trees under system-origin, converging
concurrent edits with last-writer-wins register semantics, and those registry
trees are read-isolated on the control plane so no data-plane grant can scan
them. Registration seeds the reserved `default` tenant with an unbounded quota,
so an existing cluster adopts the add-on non-destructively.

Isolation is achieved by filling in seams that core declares as inert no-ops:

- `ITenantContextResolver` scopes a caller-supplied, tenant-local tree name into
  the tenant's own namespace (`t/{tenant}/{name}`), so two tenants using the same
  unqualified name reach different trees. The assertion is re-validated against
  the caller's own membership, and an unresolvable or unauthorized one fails
  closed with a `LatticeTenantAccessDeniedException` rather than falling back to
  a shared tree.
- `ITenantEnumerationFilter` prunes a tree-id enumeration made under an active
  tenant to the trees that tenant owns (platform-owned system ids stay in, governed
  separately); a caller asserting no tenant is confined instead by the per-entry
  authorization check, so a catalog read can never disclose another tenant's tree
  names - or the tenant roster itself.
- `ITenantRegionVisibilityResolver` reports the regions a tenant is authorized
  into or resident in, so region discovery never hands a tenant caller the
  cluster's whole routing topology (in the shipped registrations discovery cannot
  validate a tenant assertion, so a tenant-asserting caller is shown only the
  serving region).

Each seam keeps its no-op default until the add-on replaces it, which is what
makes a host that never calls `AddLatticeTenancy()` unchanged.

## Quotas, metering, and rate limiting

Usage metering samples each tenant's live keys, bytes, memory, and tree count
into a durable per-tenant usage store, and quota admission refuses a write once
the tenant's metered usage is beyond its quota and burst allowance, with a
`LatticeQuotaExceededException` (surfaced over the data gRPC binding as
`ResourceExhausted` carrying the breached dimension). A cluster-wide operations-per-second budget is
apportioned across live silos and enforced silo-locally by a token bucket, so
rate limiting needs no per-request cross-silo hop. Usage above a steady-state cap
is accrued as billable overage on every metering tick.

Replication backfill bypasses tenant quota and request-rate admission so a
target-region quota cannot prevent existing data from converging. It does not
bypass accounting: metering includes the target's locally held tenant trees, and
`GlobalConverged` usage sums each region's published footprint. Backfill does not
consume client rate tokens, but it uses shared storage, WAL, CPU, and network
capacity and can contend with client work.

## Region residency and observability

An optional per-tenant residency policy confines a tenant's data to a residency
set within an operator-authorized set of regions. Client requests are served only
in `Online` regions, while receiver replication is admitted during `Backfilling`.
With the tenant-admin and replication packages registered, an added region
automatically backfills from an `Online` peer and becomes `Online` only after
every tenant-tree bootstrap and parked offline entry is verified complete; an
empty tenant can go directly online. A dropped region completes its drain on its
own. The region-residency guide describes progress and the explicit
data-in-place operator override. A separate placement binding on the tenant
record can pin its
trees to a dedicated WAL provider. Every
tenant is observable through the `orleans.lattice.tenancy` OpenTelemetry meter,
which publishes per-tenant usage, quota, and overage gauges tagged by tenant.

## Registration

```csharp
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddLatticeMembership()
    .AddLatticeAuth()
    .AddLatticeTenancy(options => options.SeedDefaultTenant = true);
```

Must be registered after `AddLattice()`, `AddLatticeMembership()`, and
`AddLatticeAuth()`: membership resolves the tenant-admin subjects the registry
names, and auth is the enforcement seam that acts on tenant status, quotas, and
grants. Calling it out of order fails fast with an actionable message.

This package carries no operator control surface of its own. Add
`Orleans.Lattice.Api.TenantAdmin` (and its gRPC or MCP binding) to administer the
tenant lifecycle.

See the
[Multi-tenancy documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.tenancy/README.md)
for the full guide.
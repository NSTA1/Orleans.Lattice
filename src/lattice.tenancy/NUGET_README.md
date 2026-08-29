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
tenant-admin subjects, and cross-tenant grants. The `ITenantRegistry` dogfoods
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
- `ITenantEnumerationFilter` prunes every tree-id enumeration to the trees the
  active tenant owns, so a catalog read can never disclose another tenant's
  tree names - or the tenant roster itself.
- `ITenantRegionVisibilityResolver` scopes region discovery to the regions a
  tenant is actually authorized into or resident in, so a tenant caller is not
  handed the cluster's whole routing topology.

Each seam keeps its no-op default until the add-on replaces it, which is what
makes a host that never calls `AddLatticeTenancy()` unchanged.

## Quotas, metering, and rate limiting

Usage metering samples each tenant's live keys, bytes, memory, and tree count
into a durable per-tenant usage store, and `LatticeTenantAdmissionController`
refuses a write that would breach the tenant's quota with a
`LatticeQuotaExceededException` (surfaced over gRPC as `ResourceExhausted`
carrying the breached dimension). A cluster-wide operations-per-second budget is
apportioned across live silos and enforced silo-locally by a token bucket, so
rate limiting needs no per-request cross-silo hop. Sustained breaches are
recorded as billable overage samples.

## Region residency and observability

An optional per-tenant residency policy binds a tenant's data to an allowed set
of regions, steering WAL placement and refusing an out-of-region crossing. Every
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
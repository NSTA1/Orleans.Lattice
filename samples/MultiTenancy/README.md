# Multi-tenancy

An opt-in, single-silo tour of Orleans.Lattice multi-tenancy: the tenant
registry, the isolation naming seam, and the operator control-plane facade -
all turned on by adding a few packages, with **no change to the core tree**.

## What it shows

One in-process Orleans silo runs the full control-plane stack:

- **Membership** (`AddLatticeMembership`) resolves the ambient caller credential
  into a subject.
- **Auth** (`AddLatticeAuth`) installs the fail-closed enforcement gate.
- **Tenancy** (`AddLatticeTenancy`) turns on the tenant registry, the isolation
  seams, and the reserved `default` tenant.
- **Tenant-Admin API** (`AddLatticeTenantAdminApi`) adds the operator
  control-plane facade `ILatticeTenantAdmin`.

The program walks four acts:

1. **Tenant tree naming.** A tenant-scoped tree id self-describes its owner:
   `LatticeTenantTrees.Compose(tenant, name)` yields `t/{tenant}/{name}`, and
   `TryGetTenant` reverses it. This structural prefix is what the isolation gate
   checks - a caller in tenant `acme` can only ever name `t/acme/*`.
2. **Tenant lifecycle as a platform operator.** A bootstrap administrator
   creates two tenants and reads back their lifecycle status.
3. **Lifecycle transitions and guards.** Suspend / resume a tenant, delete a
   tenant (cascading its trees), and prove create is not upsert - a second
   create of the same id is refused with `TenantAlreadyExistsException`.
4. **Fail-closed control plane.** The reserved `default` tenant can never be
   deleted or suspended (`ReservedTenantOperationException`), and a caller who is
   not a platform operator is denied every lifecycle op
   (`LatticeAuthorizationDeniedException`) under the default-deny gate.

## Run it

```
dotnet run --project samples/MultiTenancy
```

Expected tail:

```
== Act 4: fail-closed control plane ==
  delete reserved 'default' -> refused (ReservedTenantOperationException)
  create as non-operator 'mallory' -> denied (LatticeAuthorizationDeniedException)

[OK] tenant lifecycle ran end-to-end; the reserved tenant and the operator seam stayed fail-closed.
```

The process exits `0` on success and `1` if any control-plane guard fails to
hold.

## How authorization works here

The gate runs **default-deny** (the production posture). Tenant-lifecycle
operations authorize the cluster-wide `Admin` capability, which only a bootstrap
administrator - or an explicitly authored cluster-wide `Admin` rule - holds, so
the operator seam is fail-closed against every other caller. The sample declares
`platform-operator` as a bootstrap administrator and runs each operator action
under that credential; the unrelated subject `mallory` is refused.

## Where to go next

- Core tenancy concepts, isolation, quotas, metering, rate limiting, region
  residency, and observability: [docs/lattice.tenancy](../../docs/lattice.tenancy/README.md).
- The operator control-plane facade surface:
  [docs/lattice.api.tenantadmin](../../docs/lattice.api.tenantadmin/README.md).
- Driving the facade remotely over gRPC:
  [docs/lattice.api.tenantadmin.grpc](../../docs/lattice.api.tenantadmin.grpc/README.md).

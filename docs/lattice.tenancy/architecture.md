# Architecture

The tenancy add-on connects a durable tenant definition store to core authorization, admission, replication isolation, enumeration and placement seams. It does not put a tenant id into each value payload; tenant trees use tenant-qualified logical names while the default tenant retains legacy bare names.

## Registration and authoritative definitions

`AddLatticeTenancy` checks core, membership and authorization registrations before wiring the add-on. It configures history, seeds the default tenant if requested, and registers registry, policy, residency, usage, rate and observability services. When replication is present, a post-configuration step declares the reserved tenant-definition tree as `LwwRegister` regardless of whether replication was registered first. Usage and overage tree replication remains deployment-controlled.

Registry writes merge timestamped definition fields locally and persist the resulting record through Lattice. That local field merge is not the cross-region wire merge: replicated serialized records are resolved by last-writer-wins. Hosts should create definitions once and replicate them rather than independently seeding conflicting records in each region.

## Compiled policy and freshness

Mutation and alias observers rebuild local compiled views for policy, residency and placement. An epoch/lease mechanism prevents a disconnected silo from indefinitely serving a stale tenant decision. The configured `PolicySnapshotLeaseDuration` bounds the authoritative lease. While it is stale, asynchronous enforcement confirms the relevant active-tenant/residency decision against the registry or denies it; synchronous enforcement cannot turn stale cached state into permission.

The caller's membership and asserted tenant are resolved before an access decision. Tenant enforcement composes with the ordinary `ILatticeAccessGate`; a tenant namespace is not an independent way to grant an operation denied by ordinary authorization. Delegated group/member/rule administration is disabled by default and is constrained to the acting tenant when enabled.

## Quota, usage and rate pipeline

Admission consults the tenant policy and sampled usage in the configured `TenantEnforcementScope`. Usage samples are aggregated and published with absolute/relative hysteresis. A changed sample is refreshed after five minutes of supplied metering-clock time even if its movement stays below the band. With available metering, publication and replication, a stable small quota crossing therefore reaches subsequent admission; unchanged samples do not churn the usage tree. Overage accounting and the read-only observability view consume those measurements; gauges can be disabled without removing enforcement. Sampled or globally convergent usage uses soft admission rather than a consensus-backed global reservation. A numerical overshoot bound additionally requires bounds on arrivals, write sizes, metering and replication delays.

Request rates use silo-local admission budgets. Lease coordination apportions the configured rate across participating silos. The default strategy is `Demand`, but the shipped in-process demand exchange provides no cluster aggregate, so it falls back to an even split; demand-proportional allocation requires an aggregate provider. Lease cycle timeout, retry backoff and rate-snapshot TTL bound the coordination and failure paths. See [configuration](configuration.md) and [rate limiting](README.md#rate-limiting) for the defaults and scope-specific limitations.

## Residency, placement and control plane

Region allow/resident sets and lifecycle status are tenant definition state. Residency changes are observed on every silo, and inbound replicated mutations pass tenant isolation/residency checks as well. The [TenantAdmin facade](../lattice.api.tenantadmin/README.md) drives allowed-region changes, backfill and drain; the tenancy package supplies the registry and enforcement seams, not the transport endpoint.

WAL placement can select a dedicated provider for a tenant. The recorded silo placement filter is not acted on. Existing placed trees do not move merely because a definition changes; see [placement](README.md#placement-follows-the-registry-on-every-silo).

## Source map

- [Registration and system-tree enrollment](../../src/lattice.tenancy/LatticeTenancyServiceCollectionExtensions.cs)
- [Registry contract](../../src/lattice.tenancy/ITenantRegistry.cs)
- [Tenant gate behavior](../../src/lattice.tenancy/TenantGateEnforcer.cs)
- [Policy snapshot maintenance](../../src/lattice.tenancy/CompiledTenantPolicySnapshotMaintainer.cs)
- [Usage accounting options](../../src/lattice.tenancy/TenantUsageAccountingOptions.cs)
- [Rate coordination options](../../src/lattice.tenancy/LatticeTenantRateLimiterOptions.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [Package guide](README.md)

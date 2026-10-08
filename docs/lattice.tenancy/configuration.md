# Configuration

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
| `DelegatedAccessAdministrationEnabled` | `bool` | `false` | Whether [delegated tenant access administration](README.md#delegated-tenant-access-administration) is enabled: tenant groups, tenant member sets, group entries in an admin set, and tenant-tier rules. While `false` the facades refuse to change any of them, member entries, group entries in an admin set and tenant-tier rules are inert, and nothing is deleted. Read live: a change invalidates the compiled tenant-policy snapshot and clears the subject-resolution cache, with no restart. |
| `PolicySnapshotLeaseDuration` | `TimeSpan` | `10s` | How long a silo may treat its compiled tenant-policy, residency and placement snapshots as authoritative without renewing its lease from the cluster-wide tenant-policy epoch; renewed every third of this. While the lease is lapsed, every request that acts as an asserted active tenant (on its own trees or across a cross-tenant grant), residency checks and inbound-replication tenant checks are confirmed against the registry or denied, and a tenant tree's registration waits up to a fifth of this for the placement view to become authoritative before it is refused. It is also the most a registry write can be held open (about 1.1 times this) when a silo cannot be reached, or just after the epoch restarts, so keep it well below the Orleans response timeout. Must be strictly positive and at most `0xFFFFFFFE` milliseconds (about 49.7 days). |

### `TenantUsageAccountingOptions`

Governs usage metering and the quota-enforcement scope every tenant is admitted under.

| Property | Type | Default | Meaning |
|---|---|---|---|
| `DefaultEnforcementScope` | `TenantEnforcementScope` | `GlobalConverged` | The [enforcement scope](README.md#enforcement-scope-multi-cluster) every tenant's quota admission runs under, read live; there is no per-tenant override yet. |
| `PublishMinAbsoluteDelta` | `long` | `65536` (`64 * 1024`) | Absolute movement, in the sampled unit, below which a usage republish is damped. A tenant's *first* non-empty publish is never damped. |
| `PublishMinRelativeDelta` | `double` | `0.05` | Relative movement, as a fraction of the last published value, below which a usage republish is damped. Per dimension the effective threshold is the larger of `PublishMinAbsoluteDelta` and this fraction of that dimension's last published value, and the slot republishes when any one dimension moves by at least its threshold. A negative value of either knob is treated as zero. |
| `MeterInterval` | `TimeSpan` | `30s` | The per-silo metering cycle that samples each tenant's footprint and rolls it into that tenant's per-cluster usage slot. Zero or a negative value disables metering entirely, which pins footprint admission in its documented fail-open branch so an authored footprint quota never binds (the request rate and the tree-count check at creation still apply). Re-read before every cycle, so a reload to zero or a negative value stops a running loop. A value above about 49.7 days (`0xFFFFFFFE` milliseconds, the longest delay a timer accepts) is clamped to it. |

### `TenantObservabilityOptions`

Governs the per-tenant gauges described under [Observability](README.md#observability).

| Property | Type | Default | Meaning |
|---|---|---|---|
| `PublishGauges` | `bool` | `true` | Whether to publish the per-tenant observable gauges on the `orleans.lattice.tenancy` meter. `false` leaves the meter inert and skips the periodic overage scan. |
| `PublishInterval` | `TimeSpan` | `30s` (`DefaultPublishInterval`) | How often the publisher re-samples the warm usage index and the overage billing seam. A non-positive value is treated as the default, and a value above about 49.7 days (the longest period a timer accepts) is clamped to it. |

### `LatticeTenantRateLimiterOptions`

Governs how a tenant's cluster-wide `MaxOpsPerSecond` is divided across live
silos. See [Rate limiting](README.md#rate-limiting) for the admission behavior.

| Option | Type | Default | Meaning |
|---|---|---|---|
| `LeaseInterval` | `TimeSpan` | `30s` | How often the coordinator re-apportions each tenant's cluster rate across the live silos. A longer interval lowers coordination cost but widens the transient overshoot bound (lease interval times cluster rate); the default is sized for work backed by a whole-tree registry scan. A non-positive value falls back to the default, and the tick period is held to about 49.7 days (the longest period a timer accepts). |
| `LeaseCycleTimeout` | `TimeSpan` | `20s` | The bound on a single lease cycle. A cycle that exceeds it is cancelled and retried on a later tick, so a stalled tenant-registry read can never occupy the loop for longer than one interval. Clamped down to `LeaseInterval` if set at or above it, so the duty cycle stays bounded, and held to about 49.7 days (the longest delay a timer accepts). A non-positive value falls back to the default. |
| `MaxLeaseBackoff` | `TimeSpan` | `5m` | The ceiling the lease interval backs off to after consecutive cycle failures. The effective interval doubles per consecutive failure and resets to `LeaseInterval` on the first success, so a persistently unhealthy registry is probed at a decaying rate rather than hammered every tick. A value below `LeaseInterval` disables backoff; a non-positive value falls back to the default. |
| `RateSnapshotTtl` | `TimeSpan` | `2m` | How long a read of the registry's configured rates stays usable before the next cycle re-reads it. Configured rates change at administrative cadence, so caching them decouples the frequent re-apportionment of token buckets from the expensive whole-tree registry scan. The snapshot is stale-if-error, so a failed refresh apportions from the previous snapshot rather than pruning every tenant's bucket. A non-positive value falls back to the default. |
| `Apportionment` | `TenantRateApportionmentStrategy` | `Demand` | `Demand` leases demand-proportionally and degrades to static-even when no cluster-wide demand aggregate is available; `StaticEven` is the zero-coordination fallback that splits the rate evenly. The package ships no cluster-wide demand aggregator - its in-process demand exchange always reports none - so `Demand` apportions exactly as `StaticEven` does. |
| `DemandReserveFraction` | `double` | `0.2` | The fraction of the cluster rate that demand-proportional leasing reserves and splits evenly, guaranteeing an idle silo a non-zero floor so it can never be starved out of building demand. In `[0, 1]` (a value outside is clamped to that range); ignored under `StaticEven`, and whenever no cluster-wide demand aggregate is available (see `Apportionment`). |


## Source declarations

- [Registry and policy options](../../src/lattice.tenancy/LatticeTenancyOptions.cs)
- [Usage accounting](../../src/lattice.tenancy/TenantUsageAccountingOptions.cs)
- [Observability](../../src/lattice.tenancy/TenantObservabilityOptions.cs)
- [Rate limiter](../../src/lattice.tenancy/LatticeTenantRateLimiterOptions.cs)

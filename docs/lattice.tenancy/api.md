# Public API

`Orleans.Lattice.Tenancy` adds tenant registry, access, quota, metering and residency services. The [package guide](README.md) explains the isolation model and operational limits; the declarations below are the concrete public contract, not a transport binding.

## Registration and read/write boundaries

Call `AddLatticeTenancy` on the silo after `AddLattice`, membership and authorization. Registration checks those dependencies and fails immediately when one is missing. Repeated calls apply a supplied options delegate but do not duplicate the structural wiring. `ConfigureLatticeTenancy` configures only `LatticeTenancyOptions`; configure usage, observability and rate-limiter options through the service collection. See [configuration](configuration.md).

`ITenantRegistry` is the durable definition-store seam. It reads and enumerates records and applies timestamped lifecycle, quota, region and access mutations. It is not a caller-facing authorization facade: applications that administer tenants on behalf of a user should use [the TenantAdmin API](../lattice.api.tenantadmin/README.md). The store joins a caller-supplied mutation with existing state; transport replication of the serialized record is distinct from that local join.

`TenantRecord` carries the tenant definition and its stamped field/slot state; quota and placement values are not separate user trees. `ITenantPolicyEngine` exposes the current compiled policy. Residency, usage, rate and overage interfaces provide independent seams so a host can observe or replace one without treating the registry as a request-rate counter. Their exact read/write signatures appear below.

## Public models and failure handling

`TenantId` and the core tenant-context seams are defined by the core library, not re-declared here. This package defines tenant status, region status, quota, placement, grant, usage and overage models. Concurrency failures distinguish registry, usage and overage write contention; do not retry an authorization denial as a concurrency conflict. The [package guide](README.md#store-write-contention) describes retry behavior.

Use the read-only observability and usage interfaces for measurements. Their published values are sampled/convergent, not a globally synchronous reservation; see [resource governance](README.md#resource-governance) before interpreting a quota or rate setting as a strict multi-cluster ceiling.

## Related

- [Architecture](architecture.md)
- [Configuration](configuration.md)
- [Tenant administration](../lattice.api.tenantadmin/README.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Tenancy.CrossTenantGrant`

[Source](../../src/lattice.tenancy/CrossTenantGrant.cs) (line 17).

`public readonly record struct CrossTenantGrant`

- `public string Grantee { get; init; }`
- `public TenantGranteeKind GranteeKind { get; init; }`
- `public string Scope { get; init; }`
- `public TenantGrantOperations Operations { get; init; }`
- `public TenantGrantState State { get; init; }`
- `public static CrossTenantGrant Create( string grantee, TenantGranteeKind granteeKind, string scope, TenantGrantOperations operations)`
- `public static CrossTenantGrant Create( string grantee, TenantGranteeKind granteeKind, string scope, TenantGrantOperations operations, TenantGrantState state)`
- `public string GrantId`

### `Orleans.Lattice.Tenancy.ITenantObservabilityView`

[Source](../../src/lattice.tenancy/ITenantObservabilityView.cs) (line 20).

`public interface ITenantObservabilityView`

- `Task<TenantObservabilitySnapshot?> GetActiveTenantAsync(CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<TenantObservabilitySnapshot> ListAsync( TenantObservabilityScope scope, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Tenancy.ITenantOverageBilling`

[Source](../../src/lattice.tenancy/ITenantOverageBilling.cs) (line 16).

`public interface ITenantOverageBilling`

- `Task<TenantOverageSample> GetMeteredOverageAsync(TenantId tenant, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<TenantMeteredOverage> ListMeteredOverageAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Tenancy.ITenantPolicyEngine`

[Source](../../src/lattice.tenancy/ITenantPolicyEngine.cs) (line 25).

`public interface ITenantPolicyEngine`

- `long CurrentEpoch { get; }`
- `IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId)`
- `TenantAccessDecision ValidateActiveTenant(string subjectId, TenantId activeTenant)`
- `IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId, IReadOnlyCollection<string> groupIds)`
- `TenantAccessDecision ValidateActiveTenant(string subjectId, IReadOnlyCollection<string> groupIds, TenantId activeTenant)`
- `TenantAccessDecision ResolveCrossTenantGrant( TenantId sourceTenant, TenantId targetTenant, string scope, TenantGrantOperations operation)`

### `Orleans.Lattice.Tenancy.ITenantRateLimiter`

[Source](../../src/lattice.tenancy/ITenantRateLimiter.cs) (line 35).

`public interface ITenantRateLimiter`

- `bool TryAcquire(TenantId tenant)`

### `Orleans.Lattice.Tenancy.ITenantRegionStatusChangeListener`

[Source](../../src/lattice.tenancy/ITenantRegionStatusChangeListener.cs) (line 18).

`public interface ITenantRegionStatusChangeListener`

- `Task OnRegionStatusChangedAsync(TenantRegionStatusChange change, CancellationToken cancellationToken)`

### `Orleans.Lattice.Tenancy.ITenantRegistry`

[Source](../../src/lattice.tenancy/ITenantRegistry.cs) (line 17).

`public interface ITenantRegistry`

- `Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default)`
- `Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<TenantRecord> ListAsync(CancellationToken cancellationToken = default)`
- `Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default)`
- `Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Tenancy.ITenantResidencyResolver`

[Source](../../src/lattice.tenancy/ITenantResidencyResolver.cs) (line 18).

`public interface ITenantResidencyResolver`

- `bool IsActive { get; }`
- `bool IsOnlineInServingRegion(TenantId tenant)`

### `Orleans.Lattice.Tenancy.ITenantUsageReader`

[Source](../../src/lattice.tenancy/ITenantUsageReader.cs) (line 27).

`public interface ITenantUsageReader`

- `TenantEnforcementScope ResolveScope(TenantId tenant)`
- `Task<TenantUsageReading?> ReadAsync(TenantId tenant, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Tenancy.LatticeTenancyOptions`

[Source](../../src/lattice.tenancy/LatticeTenancyOptions.cs) (line 12).

`public sealed class LatticeTenancyOptions`

- `public HistoryRetentionMode HistoryRetentionMode { get; set; }`
- `public TimeSpan? HistoryRetentionWindow { get; set; }`
- `public bool EnableDurableHistoryView { get; set; }`
- `public bool SeedDefaultTenant { get; set; }`
- `public TimeSpan PolicySnapshotLeaseDuration { get; set; }`
- `public bool DelegatedAccessAdministrationEnabled { get; set; }`

### `Orleans.Lattice.Tenancy.LatticeTenancyServiceCollectionExtensions`

[Source](../../src/lattice.tenancy/LatticeTenancyServiceCollectionExtensions.cs) (line 18).

`public static class LatticeTenancyServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeTenancy( this ISiloBuilder builder, Action<LatticeTenancyOptions>? configure = null)`
- `public static ISiloBuilder ConfigureLatticeTenancy( this ISiloBuilder builder, Action<LatticeTenancyOptions> configure)`

### `Orleans.Lattice.Tenancy.LatticeTenantMetrics`

[Source](../../src/lattice.tenancy/LatticeTenantMetrics.cs) (line 34).

`public static class LatticeTenantMetrics`

- `public const string MeterName`
- `public const string TagTenant`
- `public const string TenantsName`
- `public const string UsageBytesName`
- `public const string UsageKeysName`
- `public const string UsageMemoryBytesName`
- `public const string UsageTreesName`
- `public const string QuotaBytesName`
- `public const string QuotaKeysName`
- `public const string QuotaMemoryBytesName`
- `public const string QuotaTreesName`
- `public const string QuotaBurstPercentName`
- `public const string OverageBytesName`
- `public const string OverageKeysName`
- `public const string OverageMemoryBytesName`
- `public const string OverageTreesName`
- `public static readonly Meter Meter`

### `Orleans.Lattice.Tenancy.LatticeTenantRateLimiterOptions`

[Source](../../src/lattice.tenancy/LatticeTenantRateLimiterOptions.cs) (line 21).

`public sealed class LatticeTenantRateLimiterOptions`

- `public static readonly TimeSpan DefaultLeaseInterval`
- `public static readonly TimeSpan DefaultLeaseCycleTimeout`
- `public static readonly TimeSpan DefaultMaxLeaseBackoff`
- `public static readonly TimeSpan DefaultRateSnapshotTtl`
- `public TimeSpan LeaseInterval { get; set; }`
- `public TimeSpan LeaseCycleTimeout { get; set; }`
- `public TimeSpan MaxLeaseBackoff { get; set; }`
- `public TimeSpan RateSnapshotTtl { get; set; }`
- `public TenantRateApportionmentStrategy Apportionment { get; set; }`
- `public double DemandReserveFraction { get; set; }`

### `Orleans.Lattice.Tenancy.LocalUsageSample`

[Source](../../src/lattice.tenancy/LocalUsageSample.cs) (line 18).

`public readonly record struct LocalUsageSample`

- `public long Bytes { get; init; }`
- `public long Keys { get; init; }`
- `public long MemoryBytes { get; init; }`
- `public long TreeCount { get; init; }`
- `public static LocalUsageSample Empty`
- `public bool IsEmpty`
- `public LocalUsageSample Add(LocalUsageSample other)`
- `public static LocalUsageSample RollUp(IReadOnlyCollection<TreeUsageSample> trees)`

### `Orleans.Lattice.Tenancy.TenantAccessCaps`

[Source](../../src/lattice.tenancy/TenantAccessCaps.cs) (line 23).

`public static class TenantAccessCaps`

- `public const string GroupsDimension`
- `public const string MembershipEdgesDimension`
- `public const string MemberSubjectsDimension`
- `public const string TenantRulesDimension`
- `public static void AdmitAddition(TenantId tenant, string treeId, string dimension, long currentCount, long cap)`

### `Orleans.Lattice.Tenancy.TenantAccessDecision`

[Source](../../src/lattice.tenancy/TenantAccessDecision.cs) (line 16).

`public readonly struct TenantAccessDecision`

- `public bool Allowed { get; }`
- `public string? Reason { get; }`
- `public static TenantAccessDecision Allow()`
- `public static TenantAccessDecision Deny(string reason)`

### `Orleans.Lattice.Tenancy.TenantEnforcementScope`

[Source](../../src/lattice.tenancy/TenantEnforcementScope.cs) (line 13).

`public enum TenantEnforcementScope`

- `GlobalConverged = 0`
- `PerCluster = 1`

### `Orleans.Lattice.Tenancy.TenantGrantLifecycle`

[Source](../../src/lattice.tenancy/TenantGrantLifecycle.cs) (line 38).

`public static class TenantGrantLifecycle`

- `public static bool Authorizes(TenantGrantState state)`
- `public static bool IsTerminal(TenantGrantState state)`
- `public static bool IsLegalTransition(TenantGrantState from, TenantGrantState to)`
- `public static bool IsLegalOffer(TenantGrantState current)`
- `public static TenantGrantState Join(TenantGrantState left, TenantGrantState right)`

### `Orleans.Lattice.Tenancy.TenantGrantOperations`

[Source](../../src/lattice.tenancy/TenantGrantOperations.cs) (line 8).

`public enum TenantGrantOperations`

- `None = 0`
- `Read = 1`
- `Write = 2`
- `ReadWrite = Read | Write`

### `Orleans.Lattice.Tenancy.TenantGrantSlot`

[Source](../../src/lattice.tenancy/TenantGrantSlot.cs) (line 49).

`public readonly record struct TenantGrantSlot`

- `public CrossTenantGrant Grant { get; init; }`
- `public bool Present { get; init; }`
- `public HybridLogicalClock Clock { get; init; }`
- `public string? WriterId { get; init; }`
- `public long Generation { get; init; }`
- `public static TenantGrantSlot Merge(TenantGrantSlot left, TenantGrantSlot right)`

### `Orleans.Lattice.Tenancy.TenantGrantState`

[Source](../../src/lattice.tenancy/TenantGrantState.cs) (line 39).

`public enum TenantGrantState`

- `Active = 0`
- `Pending = 1`
- `Rejected = 2`
- `Revoked = 3`

### `Orleans.Lattice.Tenancy.TenantGranteeKind`

[Source](../../src/lattice.tenancy/TenantGranteeKind.cs) (line 7).

`public enum TenantGranteeKind`

- `Subject = 0`
- `Tenant = 1`

### `Orleans.Lattice.Tenancy.TenantLwwRegister`

[Source](../../src/lattice.tenancy/TenantLwwRegister.cs) (line 13).

`public readonly record struct TenantLwwRegister<T>`

- `public T Value { get; init; }`
- `public HybridLogicalClock Clock { get; init; }`
- `public string? WriterId { get; init; }`
- `public static TenantLwwRegister<T> Create(T value, HybridLogicalClock clock, string? writerId)`
- `public TenantLwwRegister<T> Set(T value, HybridLogicalClock clock, string? writerId)`
- `public static TenantLwwRegister<T> Merge(TenantLwwRegister<T> left, TenantLwwRegister<T> right)`

### `Orleans.Lattice.Tenancy.TenantMeteredOverage`

[Source](../../src/lattice.tenancy/TenantMeteredOverage.cs) (line 15).

`public readonly record struct TenantMeteredOverage`

- `public TenantMeteredOverage(TenantId tenant, TenantOverageSample overage)`
- `public TenantId Tenant { get; init; }`
- `public TenantOverageSample Overage { get; init; }`

### `Orleans.Lattice.Tenancy.TenantObservabilityOptions`

[Source](../../src/lattice.tenancy/TenantObservabilityOptions.cs) (line 16).

`public sealed class TenantObservabilityOptions`

- `public static readonly TimeSpan DefaultPublishInterval`
- `public bool PublishGauges { get; set; }`
- `public TimeSpan PublishInterval { get; set; }`

### `Orleans.Lattice.Tenancy.TenantObservabilityScope`

[Source](../../src/lattice.tenancy/TenantObservabilityScope.cs) (line 27).

`public readonly record struct TenantObservabilityScope`

- `public bool IsClusterWide { get; }`
- `public LatticeSubject Subject { get; }`
- `public static TenantObservabilityScope ActiveTenant { get; }`
- `public static TenantObservabilityScope ClusterWide(LatticeSubject subject)`

### `Orleans.Lattice.Tenancy.TenantObservabilitySnapshot`

[Source](../../src/lattice.tenancy/TenantObservabilitySnapshot.cs) (line 19).

`public readonly record struct TenantObservabilitySnapshot`

- `public TenantObservabilitySnapshot( TenantId tenant, LocalUsageSample usage, TenantQuotas quotas, TenantOverageSample meteredOverage)`
- `public TenantId Tenant { get; init; }`
- `public LocalUsageSample Usage { get; init; }`
- `public TenantQuotas Quotas { get; init; }`
- `public TenantOverageSample MeteredOverage { get; init; }`
- `public TenantOverageSample InstantaneousOverage`

### `Orleans.Lattice.Tenancy.TenantOverageConcurrencyException`

[Source](../../src/lattice.tenancy/TenantOverageConcurrencyException.cs) (line 18).

`public sealed class TenantOverageConcurrencyException : Exception`

- `public TenantOverageConcurrencyException(TenantId tenant, int attempts)`
- `public TenantId Tenant { get; }`
- `public int Attempts { get; }`

### `Orleans.Lattice.Tenancy.TenantOverageRecord`

[Source](../../src/lattice.tenancy/TenantOverageRecord.cs) (line 31).

`public sealed class TenantOverageRecord`

- `public TenantId Id { get; private init; }`
- `public TenantOverageRecord()`
- `public static TenantOverageRecord Create(TenantId id)`
- `public void MeterLocal(string cluster, TenantOverageSample increment)`
- `public TenantOverageSample LocalOverage(string cluster)`
- `public TenantOverageSample Fold()`
- `public TenantOverageSample Fold(IReadOnlySet<string> residentClusters)`
- `public int ClusterCount { get; }`
- `public TenantOverageRecord Clone()`
- `public TenantOverageRecord MergeFrom(TenantOverageRecord other)`
- `public static TenantOverageRecord Merge(TenantOverageRecord left, TenantOverageRecord right)`

### `Orleans.Lattice.Tenancy.TenantOverageSample`

[Source](../../src/lattice.tenancy/TenantOverageSample.cs) (line 22).

`public readonly record struct TenantOverageSample`

- `public long Bytes { get; init; }`
- `public long Keys { get; init; }`
- `public long MemoryBytes { get; init; }`
- `public long TreeCount { get; init; }`
- `public static TenantOverageSample Empty`
- `public bool IsEmpty`
- `public TenantOverageSample Add(TenantOverageSample other)`
- `public static TenantOverageSample Above(LocalUsageSample usage, TenantQuotas quotas)`

### `Orleans.Lattice.Tenancy.TenantPlacement`

[Source](../../src/lattice.tenancy/TenantPlacement.cs) (line 16).

`public readonly record struct TenantPlacement`

- `public string? WalProviderName { get; init; }`
- `public string? PlacementFilter { get; init; }`
- `public bool DedicatedWal { get; init; }`
- `public static TenantPlacement Shared`
- `public bool IsShared`

### `Orleans.Lattice.Tenancy.TenantQuotas`

[Source](../../src/lattice.tenancy/TenantQuotas.cs) (line 16).

`public readonly record struct TenantQuotas`

- `public long? MaxBytes { get; init; }`
- `public long? MaxKeys { get; init; }`
- `public long? MaxMemoryBytes { get; init; }`
- `public long? MaxTreeCount { get; init; }`
- `public long? MaxOpsPerSecond { get; init; }`
- `public int BurstPercent { get; init; }`
- `public const long DefaultMaxGroups`
- `public const long DefaultMaxMembershipEdges`
- `public const long DefaultMaxMemberSubjects`
- `public const long DefaultMaxTenantRules`
- `public long? MaxGroups { get; init; }`
- `public long? MaxMembershipEdges { get; init; }`
- `public long? MaxMemberSubjects { get; init; }`
- `public long? MaxTenantRules { get; init; }`
- `public long EffectiveMaxGroups`
- `public long EffectiveMaxMembershipEdges`
- `public long EffectiveMaxMemberSubjects`
- `public long EffectiveMaxTenantRules`
- `public static TenantQuotas Unbounded`
- `public bool IsUnbounded`

### `Orleans.Lattice.Tenancy.TenantRateApportionmentStrategy`

[Source](../../src/lattice.tenancy/TenantRateApportionmentStrategy.cs) (line 7).

`public enum TenantRateApportionmentStrategy`

- `Demand = 0`
- `StaticEven = 1`

### `Orleans.Lattice.Tenancy.TenantRecord`

[Source](../../src/lattice.tenancy/TenantRecord.cs) (line 27).

`public sealed class TenantRecord`

- `public TenantId Id { get; private init; }`
- `public TenantRecord()`
- `public TenantStatus Status`
- `public TenantQuotas Quotas`
- `public TenantPlacement Placement`
- `public bool IsActive`
- `public bool IsSuspended`
- `public static TenantRecord Create( TenantId id, TenantStatus status, TenantQuotas quotas, TenantPlacement placement, HybridLogicalClock clock, string? writerId)`
- `public static TenantRecord CreateDefault(HybridLogicalClock clock, string? writerId)`
- `public void SetStatus(TenantStatus status, HybridLogicalClock clock, string? writerId)`
- `public void SetQuotas(TenantQuotas quotas, HybridLogicalClock clock, string? writerId)`
- `public void SetPlacement(TenantPlacement placement, HybridLogicalClock clock, string? writerId)`
- `public void AddAdminSubject(string subjectId, HybridLogicalClock clock, string? writerId)`
- `public void RemoveAdminSubject(string subjectId, HybridLogicalClock clock, string? writerId)`
- `public void AddMemberSubject(string subjectId, HybridLogicalClock clock, string? writerId)`
- `public void RemoveMemberSubject(string subjectId, HybridLogicalClock clock, string? writerId)`
- `public bool HasMemberSubject(string subjectId)`
- `public bool IsAdmin(string subjectId, IReadOnlyCollection<string> groupIds)`
- `public bool IsMember(string subjectId, IReadOnlyCollection<string> groupIds)`
- `public void AddGrant(CrossTenantGrant grant, HybridLogicalClock clock, string? writerId)`
- `public void OfferGrant(CrossTenantGrant grant, HybridLogicalClock clock, string? writerId)`
- `public void TransitionGrant( string grantId, TenantGrantState state, HybridLogicalClock clock, string? writerId)`
- `public void RemoveGrant(string grantId, HybridLogicalClock clock, string? writerId)`
- `public void RemoveGrant(CrossTenantGrant grant, HybridLogicalClock clock, string? writerId)`
- `public bool HasAdminSubject(string subjectId)`
- `public bool TryGetGrant(string grantId, out CrossTenantGrant grant)`
- `public IReadOnlyList<string> AdminSubjects { get; }`
- `public int AdminSubjectCount { get; }`
- `public IReadOnlyList<string> MemberSubjects { get; }`
- `public int MemberSubjectCount { get; }`
- `public int GrantCount { get; }`
- `public IReadOnlyList<CrossTenantGrant> Grants { get; }`
- `public void AuthorizeRegion(string regionId, HybridLogicalClock clock, string? writerId)`
- `public void RevokeRegion(string regionId, HybridLogicalClock clock, string? writerId)`
- `public bool IsRegionAllowed(string regionId)`
- `public void SetRegionStatus(string regionId, TenantRegionStatus status, HybridLogicalClock clock, string? writerId)`
- `public TenantRegionStatus GetRegionStatus(string regionId)`
- `public bool TryPromoteRegionStatus(string regionId, string? writerId, out TenantRegionStatus promoted)`
- `public bool HasResidencyConfiguration { get; }`
- `public int ResidentRegionCount { get; }`
- `public IReadOnlyList<string> AllowedRegionIds { get; }`
- `public IReadOnlyList<KeyValuePair<string, TenantRegionStatus>> RegionStatusEntries { get; }`
- `public TenantRecord Clone()`
- `public TenantRecord MergeFrom(TenantRecord other)`
- `public static TenantRecord Merge(TenantRecord left, TenantRecord right)`

### `Orleans.Lattice.Tenancy.TenantRegionAllowSlot`

[Source](../../src/lattice.tenancy/TenantRegionAllowSlot.cs) (line 12).

`public readonly record struct TenantRegionAllowSlot`

- `public bool Present { get; init; }`
- `public HybridLogicalClock Clock { get; init; }`
- `public string? WriterId { get; init; }`
- `public static TenantRegionAllowSlot Merge(TenantRegionAllowSlot left, TenantRegionAllowSlot right)`

### `Orleans.Lattice.Tenancy.TenantRegionLifecycle`

[Source](../../src/lattice.tenancy/TenantRegionLifecycle.cs) (line 26).

`public static class TenantRegionLifecycle`

- `public static bool IsResident(TenantRegionStatus status)`
- `public static bool IsOnline(TenantRegionStatus status)`
- `public static TenantRegionStatus? NextOnAdd(TenantRegionStatus current)`
- `public static TenantRegionStatus? NextOnRemove(TenantRegionStatus current)`
- `public static bool IsLegalPromotion(TenantRegionStatus from, TenantRegionStatus to)`
- `public static bool TryNextPromotion(TenantRegionStatus current, out TenantRegionStatus next)`

### `Orleans.Lattice.Tenancy.TenantRegionStatus`

[Source](../../src/lattice.tenancy/TenantRegionStatus.cs) (line 19).

`public enum TenantRegionStatus`

- `None = 0`
- `Provisioning = 1`
- `Backfilling = 2`
- `Online = 3`
- `Draining = 4`
- `Offline = 5`
- `Removed = 6`

### `Orleans.Lattice.Tenancy.TenantRegionStatusChange`

[Source](../../src/lattice.tenancy/TenantRegionStatusChange.cs) (line 15).

`public readonly record struct TenantRegionStatusChange( TenantId Tenant, string RegionId, TenantRegionStatus PreviousStatus, TenantRegionStatus CurrentStatus)`

- `Primary constructor / positional members: ( TenantId Tenant, string RegionId, TenantRegionStatus PreviousStatus, TenantRegionStatus CurrentStatus)`

### `Orleans.Lattice.Tenancy.TenantRegionStatusSlot`

[Source](../../src/lattice.tenancy/TenantRegionStatusSlot.cs) (line 13).

`public readonly record struct TenantRegionStatusSlot`

- `public TenantRegionStatus Status { get; init; }`
- `public HybridLogicalClock Clock { get; init; }`
- `public string? WriterId { get; init; }`
- `public static TenantRegionStatusSlot Merge(TenantRegionStatusSlot left, TenantRegionStatusSlot right)`

### `Orleans.Lattice.Tenancy.TenantRegistryConcurrencyException`

[Source](../../src/lattice.tenancy/TenantRegistryConcurrencyException.cs) (line 18).

`public sealed class TenantRegistryConcurrencyException : Exception`

- `public TenantRegistryConcurrencyException(TenantId tenant, int attempts)`
- `public TenantId Tenant { get; }`
- `public int Attempts { get; }`

### `Orleans.Lattice.Tenancy.TenantStatus`

[Source](../../src/lattice.tenancy/TenantStatus.cs) (line 9).

`public enum TenantStatus`

- `Active = 0`
- `Suspended = 1`

### `Orleans.Lattice.Tenancy.TenantSubjectSlot`

[Source](../../src/lattice.tenancy/TenantSubjectSlot.cs) (line 11).

`public readonly record struct TenantSubjectSlot`

- `public bool Present { get; init; }`
- `public HybridLogicalClock Clock { get; init; }`
- `public string? WriterId { get; init; }`
- `public static TenantSubjectSlot Merge(TenantSubjectSlot left, TenantSubjectSlot right)`

### `Orleans.Lattice.Tenancy.TenantUsageAccountingOptions`

[Source](../../src/lattice.tenancy/TenantUsageAccountingOptions.cs) (line 13).

`public sealed class TenantUsageAccountingOptions`

- `public TenantEnforcementScope DefaultEnforcementScope { get; set; }`
- `public long PublishMinAbsoluteDelta { get; set; }`
- `public double PublishMinRelativeDelta { get; set; }`
- `public TimeSpan MeterInterval { get; set; }`

### `Orleans.Lattice.Tenancy.TenantUsageConcurrencyException`

[Source](../../src/lattice.tenancy/TenantUsageConcurrencyException.cs) (line 18).

`public sealed class TenantUsageConcurrencyException : Exception`

- `public TenantUsageConcurrencyException(TenantId tenant, int attempts)`
- `public TenantId Tenant { get; }`
- `public int Attempts { get; }`

### `Orleans.Lattice.Tenancy.TenantUsageReading`

[Source](../../src/lattice.tenancy/TenantUsageReading.cs) (line 21).

`public readonly record struct TenantUsageReading`

- `public TenantUsageReading(TenantObservabilitySnapshot snapshot, TenantEnforcementScope scope)`
- `public TenantObservabilitySnapshot Snapshot { get; init; }`
- `public TenantEnforcementScope Scope { get; init; }`

### `Orleans.Lattice.Tenancy.TenantUsageRecord`

[Source](../../src/lattice.tenancy/TenantUsageRecord.cs) (line 31).

`public sealed class TenantUsageRecord`

- `public TenantId Id { get; private init; }`
- `public TenantUsageRecord()`
- `public static TenantUsageRecord Create(TenantId id)`
- `public void SetLocalSample(string cluster, LocalUsageSample sample, HybridLogicalClock clock, string? writerId)`
- `public LocalUsageSample LocalSample(string cluster)`
- `public int ClusterCount`
- `public LocalUsageSample Fold()`
- `public LocalUsageSample Fold(IReadOnlySet<string> residentClusters)`
- `public TenantUsageRecord Clone()`
- `public TenantUsageRecord MergeFrom(TenantUsageRecord other)`
- `public static TenantUsageRecord Merge(TenantUsageRecord left, TenantUsageRecord right)`

### `Orleans.Lattice.Tenancy.TreeUsageSample`

[Source](../../src/lattice.tenancy/TreeUsageSample.cs) (line 17).

`public readonly record struct TreeUsageSample`

- `public TreeUsageSample(long bytes, long keys, long memoryBytes)`
- `public long Bytes { get; init; }`
- `public long Keys { get; init; }`
- `public long MemoryBytes { get; init; }`

using System.Collections.Frozen;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An immutable, point-in-time snapshot mapping each <b>residency-configured</b>
/// tenant to its status in the local serving region and every configured region.
/// The <see cref="TenantResidencySnapshotMaintainer"/> rebuilds it from the tenant
/// registry off the core change-feed and swaps it atomically, so the hot-path
/// readers can resolve destination and authenticated-source residency from
/// in-memory <see cref="FrozenDictionary{TKey,TValue}"/> lookups - no grain hop,
/// no allocation, O(1).
/// </summary>
/// <remarks>
/// <para>
/// A tenant that has <b>never configured residency</b> is deliberately absent from
/// the map. A miss therefore means "unconfigured" and resolves to online
/// everywhere (backward-compatible admit-all), which is exactly the pre-residency
/// behaviour the integrated T7 gate and T16 apply path relied on. A configured
/// tenant is always present: with its local-region status when that region is in
/// its status map, or with <see cref="TenantRegionStatus.None"/> when the local
/// region is not resident, so a configured-elsewhere tenant is correctly not
/// online here.
/// </para>
/// </remarks>
internal sealed class TenantResidencySnapshot
{
    private readonly FrozenDictionary<TenantId, TenantRegionStatus> _byTenant;
    private readonly FrozenDictionary<TenantId, FrozenDictionary<string, TenantRegionStatus>> _byRegion;

    private TenantResidencySnapshot(
        FrozenDictionary<TenantId, TenantRegionStatus> byTenant,
        FrozenDictionary<TenantId, FrozenDictionary<string, TenantRegionStatus>> byRegion)
    {
        _byTenant = byTenant;
        _byRegion = byRegion;
    }

    /// <summary>
    /// The empty snapshot: every lookup misses, so every tenant resolves to online
    /// (admit-all). This is the cold-start value before the first rebuild lands and
    /// keeps enforcement fail-open on residency grounds only, never denying a tenant
    /// before its record has been observed.
    /// </summary>
    public static TenantResidencySnapshot Empty { get; } =
        new(
            FrozenDictionary<TenantId, TenantRegionStatus>.Empty,
            FrozenDictionary<TenantId, FrozenDictionary<string, TenantRegionStatus>>.Empty);

    /// <summary>The number of residency-configured tenants the snapshot carries a local status for.</summary>
    public int Count => _byTenant.Count;

    /// <summary>
    /// Builds a snapshot from the given per-tenant local-region statuses. Later
    /// entries win on a duplicate key, so the caller may pass an already-deduplicated
    /// map. This overload is for callers that do not need source-region lookup.
    /// </summary>
    /// <remarks>
    /// A dictionary source already guarantees unique keys, so the defensive dedup
    /// pass has nothing to do and is skipped outright - which is the shape the
    /// maintainer always passes, having just scanned the registry into a map. Any
    /// other source is deduplicated as before, into a map presized from the
    /// source's own count where that is available without enumerating it.
    /// </remarks>
    /// <param name="statuses">The per-tenant local-region statuses of configured tenants.</param>
    /// <returns>An immutable snapshot over a copy of <paramref name="statuses"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="statuses"/> is <c>null</c>.</exception>
    public static TenantResidencySnapshot Build(
        IEnumerable<KeyValuePair<TenantId, TenantRegionStatus>> statuses)
    {
        ArgumentNullException.ThrowIfNull(statuses);

        if (statuses is IReadOnlyDictionary<TenantId, TenantRegionStatus>)
        {
            return new TenantResidencySnapshot(
                statuses.ToFrozenDictionary(),
                FrozenDictionary<TenantId, FrozenDictionary<string, TenantRegionStatus>>.Empty);
        }

        var deduped = statuses.TryGetNonEnumeratedCount(out var count) && count > 0
            ? new Dictionary<TenantId, TenantRegionStatus>(count)
            : [];
        foreach (var pair in statuses)
        {
            deduped[pair.Key] = pair.Value;
        }

        return new TenantResidencySnapshot(
            deduped.ToFrozenDictionary(),
            FrozenDictionary<TenantId, FrozenDictionary<string, TenantRegionStatus>>.Empty);
    }

    /// <summary>
    /// Builds a residency snapshot from authoritative tenant records, retaining
    /// the lifecycle status of every region for source-authorization checks.
    /// </summary>
    /// <param name="records">The scanned tenant registry records.</param>
    /// <param name="localRegionId">The region served by this silo.</param>
    /// <returns>An immutable snapshot of local and per-region residency.</returns>
    public static TenantResidencySnapshot Build(
        IEnumerable<TenantRecord> records,
        string localRegionId)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentException.ThrowIfNullOrEmpty(localRegionId);

        var local = new Dictionary<TenantId, TenantRegionStatus>();
        var regions = new Dictionary<TenantId, FrozenDictionary<string, TenantRegionStatus>>();
        foreach (var record in records)
        {
            if (!record.HasResidencyConfiguration)
            {
                continue;
            }

            local[record.Id] = record.GetRegionStatus(localRegionId);
            regions[record.Id] = record.RegionStatusEntries
                .ToFrozenDictionary(static entry => entry.Key, static entry => entry.Value, StringComparer.Ordinal);
        }

        return new TenantResidencySnapshot(local.ToFrozenDictionary(), regions.ToFrozenDictionary());
    }

    /// <summary>
    /// Looks up a tenant's local-region status. Returns <c>false</c> when the tenant
    /// is unconfigured (absent), in which case the caller treats it as online
    /// (admit-all).
    /// </summary>
    /// <param name="tenant">The tenant to resolve a local-region status for.</param>
    /// <param name="status">
    /// The tenant's local-region status when present; otherwise
    /// <see cref="TenantRegionStatus.None"/>.
    /// </param>
    /// <returns><c>true</c> when the tenant is residency-configured (present in the snapshot).</returns>
    public bool TryGetStatus(TenantId tenant, out TenantRegionStatus status) =>
        _byTenant.TryGetValue(tenant, out status);

    /// <summary>
    /// The hot-path residency decision: <c>true</c> when <paramref name="tenant"/>
    /// is online in the local serving region. An unconfigured tenant (a miss)
    /// resolves to <c>true</c> (admit-all); a configured tenant is online only when
    /// its local-region status is exactly <see cref="TenantRegionStatus.Online"/>.
    /// Allocation-free: a single <see cref="FrozenDictionary{TKey,TValue}"/> lookup
    /// and a value-type comparison.
    /// </summary>
    /// <param name="tenant">The tenant to test.</param>
    /// <returns><c>true</c> when the tenant is online in the local region.</returns>
    public bool IsOnlineLocally(TenantId tenant) =>
        !_byTenant.TryGetValue(tenant, out var status) || status == TenantRegionStatus.Online;

    /// <summary>
    /// The inbound replication decision: unconfigured tenants remain compatible
    /// with pre-residency behavior; configured tenants admit replicated writes
    /// during backfill and while online, but not before backfill starts or after
    /// draining begins.
    /// </summary>
    /// <param name="tenant">The tenant to test.</param>
    /// <returns><c>true</c> when inbound replication may be applied locally.</returns>
    public bool IsReplicationAdmissibleLocally(TenantId tenant) =>
        !_byTenant.TryGetValue(tenant, out var status)
        || status is TenantRegionStatus.Backfilling or TenantRegionStatus.Online;

    /// <summary>
    /// Whether <paramref name="regionId"/> is a resident region for the tenant.
    /// An unconfigured tenant is resident everywhere for backwards compatibility.
    /// </summary>
    public bool IsResidentInRegion(TenantId tenant, string regionId)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        if (!_byTenant.ContainsKey(tenant))
        {
            return true;
        }

        return _byRegion.TryGetValue(tenant, out var regions)
            && regions.TryGetValue(regionId, out var status)
            && TenantRegionLifecycle.IsResident(status);
    }
}

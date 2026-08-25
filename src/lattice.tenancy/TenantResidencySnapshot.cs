using System.Collections.Frozen;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An immutable, point-in-time snapshot mapping each <b>residency-configured</b>
/// tenant to its <see cref="TenantRegionStatus"/> <b>in the local serving
/// region</b>. The <see cref="TenantResidencySnapshotMaintainer"/> rebuilds it
/// from the tenant registry off the core change-feed and swaps it atomically, so
/// the hot-path reader (<see cref="TenantResidencyResolver"/>) can answer "is this
/// tenant online here?" with a single in-memory
/// <see cref="FrozenDictionary{TKey,TValue}"/> lookup - no grain hop, no
/// allocation, O(1).
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

    private TenantResidencySnapshot(FrozenDictionary<TenantId, TenantRegionStatus> byTenant) =>
        _byTenant = byTenant;

    /// <summary>
    /// The empty snapshot: every lookup misses, so every tenant resolves to online
    /// (admit-all). This is the cold-start value before the first rebuild lands and
    /// keeps enforcement fail-open on residency grounds only, never denying a tenant
    /// before its record has been observed.
    /// </summary>
    public static TenantResidencySnapshot Empty { get; } =
        new(FrozenDictionary<TenantId, TenantRegionStatus>.Empty);

    /// <summary>The number of residency-configured tenants the snapshot carries a local status for.</summary>
    public int Count => _byTenant.Count;

    /// <summary>
    /// Builds a snapshot from the given per-tenant local-region statuses. Later
    /// entries win on a duplicate key, so the caller may pass an already-deduplicated
    /// map.
    /// </summary>
    /// <param name="statuses">The per-tenant local-region statuses of configured tenants.</param>
    /// <returns>An immutable snapshot over a copy of <paramref name="statuses"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="statuses"/> is <c>null</c>.</exception>
    public static TenantResidencySnapshot Build(
        IEnumerable<KeyValuePair<TenantId, TenantRegionStatus>> statuses)
    {
        ArgumentNullException.ThrowIfNull(statuses);

        var deduped = new Dictionary<TenantId, TenantRegionStatus>();
        foreach (var pair in statuses)
        {
            deduped[pair.Key] = pair.Value;
        }

        return new TenantResidencySnapshot(deduped.ToFrozenDictionary());
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
}

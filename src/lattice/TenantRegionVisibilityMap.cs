using System.Collections.Frozen;

namespace Orleans.Lattice;

/// <summary>
/// An immutable, point-in-time map of one tenant's standing in every region it
/// has a relationship with, keyed by region id. Produced by
/// <see cref="ITenantRegionVisibilityResolver.ResolveAsync"/> and consumed by a
/// region-discovery surface to prune and annotate the regions it advertises.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed by construction.</b> <see cref="Unresolved"/> is the verdict a
/// resolver returns when the tenant's allowed / resident state cannot be
/// established (no tenancy engine reachable, the registry read failed, or no such
/// tenant). It reports <see cref="IsResolved"/> as <c>false</c> and every lookup
/// misses, so a caller that treats a miss as "not visible" degrades to the
/// tenant-scoped minimal answer rather than falling back to the full topology.
/// </para>
/// <para>
/// A <b>resolved</b> map carries one entry per region the tenant is allowed into
/// or carries a status for. A region absent from a resolved map is one the tenant
/// has no relationship with at all, which is exactly the set a discovery surface
/// must omit.
/// </para>
/// </remarks>
public sealed class TenantRegionVisibilityMap
{
    private readonly FrozenDictionary<string, TenantRegionVisibility> _byRegion;

    private TenantRegionVisibilityMap(
        FrozenDictionary<string, TenantRegionVisibility> byRegion, bool isResolved)
    {
        _byRegion = byRegion;
        IsResolved = isResolved;
    }

    /// <summary>
    /// The fail-closed "could not be established" verdict: <see cref="IsResolved"/>
    /// is <c>false</c> and every lookup misses. A shared singleton, so returning it
    /// allocates nothing.
    /// </summary>
    public static TenantRegionVisibilityMap Unresolved { get; } =
        new(FrozenDictionary<string, TenantRegionVisibility>.Empty, isResolved: false);

    /// <summary>
    /// A resolved map with no entries: the tenant is known, and is allowed into and
    /// resident in no region at all. Distinct from <see cref="Unresolved"/>, which
    /// means the state could not be established.
    /// </summary>
    public static TenantRegionVisibilityMap Empty { get; } =
        new(FrozenDictionary<string, TenantRegionVisibility>.Empty, isResolved: true);

    /// <summary>
    /// <c>true</c> when the tenant's region standing was successfully established.
    /// <c>false</c> for <see cref="Unresolved"/>, which a caller must treat as
    /// "nothing is visible" rather than "everything is visible".
    /// </summary>
    public bool IsResolved { get; }

    /// <summary>The number of regions the tenant has a relationship with.</summary>
    public int Count => _byRegion.Count;

    /// <summary>
    /// Builds a resolved map over a copy of <paramref name="regions"/>. Later
    /// entries win on a duplicate key.
    /// </summary>
    /// <param name="regions">The per-region standing of one tenant. Must not be <c>null</c>.</param>
    /// <returns>An immutable, resolved map.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="regions"/> is <c>null</c>.</exception>
    public static TenantRegionVisibilityMap Create(
        IEnumerable<KeyValuePair<string, TenantRegionVisibility>> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);

        var deduped = new Dictionary<string, TenantRegionVisibility>(StringComparer.Ordinal);
        foreach (var pair in regions)
        {
            if (pair.Key is null)
            {
                continue;
            }

            deduped[pair.Key] = pair.Value;
        }

        return deduped.Count == 0
            ? Empty
            : new TenantRegionVisibilityMap(
                deduped.ToFrozenDictionary(StringComparer.Ordinal), isResolved: true);
    }

    /// <summary>
    /// Looks up the tenant's standing in <paramref name="regionId"/>. A miss (and
    /// every lookup against <see cref="Unresolved"/>) yields <c>false</c> with
    /// <paramref name="visibility"/> left at its default "not allowed, no status"
    /// value, so a caller that omits a miss is fail-closed by default.
    /// </summary>
    /// <param name="regionId">The region id to look up. A <c>null</c> id always misses.</param>
    /// <param name="visibility">The tenant's standing in the region when present.</param>
    /// <returns><c>true</c> when the tenant has a relationship with the region.</returns>
    public bool TryGet(string? regionId, out TenantRegionVisibility visibility)
    {
        if (regionId is null)
        {
            visibility = default;
            return false;
        }

        return _byRegion.TryGetValue(regionId, out visibility);
    }
}

using System.Collections.Frozen;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An immutable, point-in-time snapshot mapping each known tenant to its
/// <see cref="TenantPlacement"/> binding. The
/// <see cref="TenantPlacementSnapshotMaintainer"/> rebuilds it from the tenant
/// registry off the core change-feed and swaps it atomically, so a reader
/// (<see cref="TenantWalPlacementResolver"/>) can look a tenant's placement up
/// with a pure in-memory <see cref="FrozenDictionary{TKey,TValue}"/> read - no
/// grain hop, and therefore no re-entrancy into the registry / tree subsystem
/// from inside a tree-registration turn.
/// </summary>
internal sealed class TenantPlacementSnapshot
{
    private readonly FrozenDictionary<TenantId, TenantPlacement> _byTenant;

    private TenantPlacementSnapshot(FrozenDictionary<TenantId, TenantPlacement> byTenant) =>
        _byTenant = byTenant;

    /// <summary>
    /// The empty snapshot: every lookup misses, so every tenant resolves to the
    /// baseline placement. This is the cold-start value before the first rebuild
    /// lands, and it is what keeps a tree registered before its tenant record is
    /// observed fail-safe (baseline, never a wrong provider).
    /// </summary>
    public static TenantPlacementSnapshot Empty { get; } =
        new(FrozenDictionary<TenantId, TenantPlacement>.Empty);

    /// <summary>The number of tenants the snapshot carries a placement for.</summary>
    public int Count => _byTenant.Count;

    /// <summary>
    /// Builds a snapshot from the given per-tenant placements. Later entries win on
    /// a duplicate key, so the caller may pass an already-deduplicated map.
    /// </summary>
    /// <remarks>
    /// A dictionary source already guarantees unique keys, so the defensive dedup
    /// pass has nothing to do and is skipped outright - which is the shape the
    /// maintainer always passes, having just scanned the registry into a map. Any
    /// other source is deduplicated as before, into a map presized from the
    /// source's own count where that is available without enumerating it, so the
    /// copy is not rehashed through the 3/7/17/37/71/... prime bucket chain on
    /// the way to a size the caller already knew.
    /// </remarks>
    /// <param name="placements">The per-tenant placement bindings.</param>
    /// <returns>An immutable snapshot over a copy of <paramref name="placements"/>.</returns>
    public static TenantPlacementSnapshot Build(
        IEnumerable<KeyValuePair<TenantId, TenantPlacement>> placements)
    {
        ArgumentNullException.ThrowIfNull(placements);

        if (placements is IReadOnlyDictionary<TenantId, TenantPlacement>)
        {
            return new TenantPlacementSnapshot(placements.ToFrozenDictionary());
        }

        // Deduplicate last-writer-wins so a duplicate tenant id (which the registry
        // never produces, but a test feed might) cannot throw from the frozen build.
        var deduped = placements.TryGetNonEnumeratedCount(out var count) && count > 0
            ? new Dictionary<TenantId, TenantPlacement>(count)
            : [];
        foreach (var pair in placements)
        {
            deduped[pair.Key] = pair.Value;
        }

        return new TenantPlacementSnapshot(deduped.ToFrozenDictionary());
    }

    /// <summary>
    /// Looks up a tenant's placement binding. Returns <c>false</c> when the tenant
    /// is not (yet) in the snapshot, in which case the caller must fall back to the
    /// baseline placement.
    /// </summary>
    /// <param name="tenant">The tenant to resolve a placement for.</param>
    /// <param name="placement">
    /// The tenant's placement binding when present; otherwise
    /// <see cref="TenantPlacement.Shared"/>.
    /// </param>
    /// <returns><c>true</c> when the tenant has a placement in the snapshot.</returns>
    public bool TryGetPlacement(TenantId tenant, out TenantPlacement placement) =>
        _byTenant.TryGetValue(tenant, out placement);
}

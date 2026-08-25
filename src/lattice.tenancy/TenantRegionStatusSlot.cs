namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A single element of a tenant's LWW-element-map of per-region lifecycle statuses:
/// the <see cref="TenantRegionStatus"/> of one region id, stamped with the clock and
/// writer that last set it. <see cref="Merge"/> keeps the slot with the higher stamp
/// in the <see cref="TenantClock"/> total order, so concurrent lifecycle transitions
/// of the same region from different clusters converge deterministically. This is the
/// tenant-admin / lifecycle-written map, stamped independently of the operator-written
/// <see cref="TenantRegionAllowSlot"/> allowed set so the two writers never clobber
/// each other.
/// </summary>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantRegionStatusSlot)]
[Immutable]
public readonly record struct TenantRegionStatusSlot
{
    /// <summary>The region's resolved lifecycle status.</summary>
    [Id(0)]
    public TenantRegionStatus Status { get; init; }

    /// <summary>The clock this slot's <see cref="Status"/> was written at.</summary>
    [Id(1)]
    public HybridLogicalClock Clock { get; init; }

    /// <summary>The id of the writer that last wrote this slot (may be <c>null</c>).</summary>
    [Id(2)]
    public string? WriterId { get; init; }

    /// <summary>
    /// Merges two slots for the same region, returning the one whose stamp wins the
    /// <see cref="TenantClock"/> total order. Commutative, associative, and
    /// idempotent.
    /// </summary>
    /// <param name="left">One slot.</param>
    /// <param name="right">The other slot.</param>
    /// <returns>The slot with the winning stamp.</returns>
    public static TenantRegionStatusSlot Merge(TenantRegionStatusSlot left, TenantRegionStatusSlot right) =>
        TenantClock.Supersedes(right.Clock, right.WriterId, left.Clock, left.WriterId) ? right : left;
}

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A single element of a tenant's LWW-element-set of operator-authorized allowed
/// regions: the presence bit for one region id, stamped with the clock and writer
/// that last set it. A region is authorized (a member of the allowed set) when its
/// winning slot is <see cref="Present"/>. <see cref="Merge"/> keeps the slot with
/// the higher stamp in the <see cref="TenantClock"/> total order, so a concurrent
/// authorize and revoke of the same region converge deterministically. This is the
/// operator-written set; the tenant admin may only set residency within it.
/// </summary>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantRegionAllowSlot)]
[Immutable]
public readonly record struct TenantRegionAllowSlot
{
    /// <summary><c>true</c> when the region is authorized (allowed); <c>false</c> when revoked.</summary>
    [Id(0)]
    public bool Present { get; init; }

    /// <summary>The clock this slot's <see cref="Present"/> bit was written at.</summary>
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
    public static TenantRegionAllowSlot Merge(TenantRegionAllowSlot left, TenantRegionAllowSlot right) =>
        TenantClock.Supersedes(right.Clock, right.WriterId, left.Clock, left.WriterId) ? right : left;
}

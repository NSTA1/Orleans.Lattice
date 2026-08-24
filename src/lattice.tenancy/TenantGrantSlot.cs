namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A single element of a tenant's LWW-element-map of cross-tenant grants: the
/// grant payload plus its presence bit, stamped with the clock and writer that
/// last set it, keyed in the map by <see cref="CrossTenantGrant.GrantId"/>. A
/// grant is live when its winning slot is <see cref="Present"/>.
/// <see cref="Merge"/> keeps the slot with the higher stamp in the
/// <see cref="TenantClock"/> total order, so a concurrent grant update and
/// revoke converge deterministically to a single payload and presence.
/// </summary>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantGrantSlot)]
[Immutable]
public readonly record struct TenantGrantSlot
{
    /// <summary>The grant payload this slot carries.</summary>
    [Id(0)]
    public CrossTenantGrant Grant { get; init; }

    /// <summary><c>true</c> when the grant is live (issued); <c>false</c> when revoked.</summary>
    [Id(1)]
    public bool Present { get; init; }

    /// <summary>The clock this slot was written at.</summary>
    [Id(2)]
    public HybridLogicalClock Clock { get; init; }

    /// <summary>The id of the writer that last wrote this slot (may be <c>null</c>).</summary>
    [Id(3)]
    public string? WriterId { get; init; }

    /// <summary>
    /// Merges two slots for the same grant id, returning the one whose stamp wins
    /// the <see cref="TenantClock"/> total order. Commutative, associative, and
    /// idempotent.
    /// </summary>
    /// <param name="left">One slot.</param>
    /// <param name="right">The other slot.</param>
    /// <returns>The slot with the winning stamp.</returns>
    public static TenantGrantSlot Merge(TenantGrantSlot left, TenantGrantSlot right) =>
        TenantClock.Supersedes(right.Clock, right.WriterId, left.Clock, left.WriterId) ? right : left;
}

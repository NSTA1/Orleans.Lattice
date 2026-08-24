namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A single element of a tenant's LWW-element-set of tenant-admin subjects: the
/// presence bit for one subject id, stamped with the clock and writer that last
/// set it. A subject is a member of the set when its winning slot is
/// <see cref="Present"/>. <see cref="Merge"/> keeps the slot with the higher
/// stamp in the <see cref="TenantClock"/> total order, so concurrent add and
/// remove of the same subject converge deterministically.
/// </summary>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantSubjectSlot)]
[Immutable]
public readonly record struct TenantSubjectSlot
{
    /// <summary><c>true</c> when the subject is present (added); <c>false</c> when removed.</summary>
    [Id(0)]
    public bool Present { get; init; }

    /// <summary>The clock this slot's <see cref="Present"/> bit was written at.</summary>
    [Id(1)]
    public HybridLogicalClock Clock { get; init; }

    /// <summary>The id of the writer that last wrote this slot (may be <c>null</c>).</summary>
    [Id(2)]
    public string? WriterId { get; init; }

    /// <summary>
    /// Merges two slots for the same subject, returning the one whose stamp wins
    /// the <see cref="TenantClock"/> total order. Commutative, associative, and
    /// idempotent.
    /// </summary>
    /// <param name="left">One slot.</param>
    /// <param name="right">The other slot.</param>
    /// <returns>The slot with the winning stamp.</returns>
    public static TenantSubjectSlot Merge(TenantSubjectSlot left, TenantSubjectSlot right) =>
        TenantClock.Supersedes(right.Clock, right.WriterId, left.Clock, left.WriterId) ? right : left;
}

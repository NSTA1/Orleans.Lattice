namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The single source of the last-writer-wins total order shared by every
/// conflict-free-mergeable field of the tenant registry
/// (<see cref="TenantLwwRegister{T}"/>, <see cref="TenantSubjectSlot"/>, and
/// <see cref="TenantGrantSlot"/>). A stamp is a
/// (<see cref="HybridLogicalClock"/>, writer-id) pair; the order is the clock
/// first, ties broken by the ordinal writer-id (a <c>null</c> writer sorts
/// lowest). The order is total and deterministic, so a merge built on it is
/// commutative, associative, and idempotent.
/// </summary>
internal static class TenantClock
{
    /// <summary>
    /// Returns <c>true</c> when the stamp
    /// (<paramref name="clock"/>, <paramref name="writerId"/>) strictly
    /// supersedes (<paramref name="otherClock"/>, <paramref name="otherWriterId"/>)
    /// in the shared total order. A tie (equal clock and equal writer) is
    /// <b>not</b> a supersession, so a merge keeps its incumbent and is therefore
    /// idempotent.
    /// </summary>
    /// <param name="clock">The candidate stamp's clock.</param>
    /// <param name="writerId">The candidate stamp's writer id (may be <c>null</c>).</param>
    /// <param name="otherClock">The incumbent stamp's clock.</param>
    /// <param name="otherWriterId">The incumbent stamp's writer id (may be <c>null</c>).</param>
    /// <returns><c>true</c> when the candidate strictly supersedes the incumbent.</returns>
    internal static bool Supersedes(
        HybridLogicalClock clock,
        string? writerId,
        HybridLogicalClock otherClock,
        string? otherWriterId)
    {
        var byClock = clock.CompareTo(otherClock);
        if (byClock != 0)
        {
            return byClock > 0;
        }

        return string.CompareOrdinal(writerId, otherWriterId) > 0;
    }
}

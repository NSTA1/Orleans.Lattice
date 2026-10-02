namespace Orleans.Lattice.Membership;

/// <summary>
/// The active <see cref="ITenantGroupClaimFilter"/>: strips every asserted group id
/// in the reserved tenant-tier namespace (<c>t/...</c>) before group expansion, so
/// no identity-provider administrator can join a tenant group by asserting its id
/// in a token claim, a group overage or a claim projection.
/// </summary>
/// <remarks>
/// <para>
/// The whole <c>t/</c> namespace is stripped, not only well-formed
/// <see cref="LatticeTenantGroupId"/> values: <c>t/default/x</c> and every
/// malformed <c>t/...</c> id are removed too, because the namespace is reserved to
/// the tenant tier as a whole.
/// </para>
/// <para>
/// Membership does not reference tenancy, so <see cref="IsActive"/> reads a
/// caller-supplied delegate. The tenancy add-on registers this filter with
/// <c>Replace</c>, passing a delegate that reads its monitored delegated tenant
/// access administration flag. The delegate is invoked on every read, so it must
/// answer from in-memory state without allocating.
/// </para>
/// </remarks>
/// <param name="isActive">Answers whether the filter is enabled. Must not be <c>null</c>.</param>
internal sealed class TenantGroupClaimFilter(Func<bool> isActive) : ITenantGroupClaimFilter
{
    private static readonly Predicate<string> IsTenantTier = static id => TenantGroupNesting.IsTenantTier(id);

    private readonly Func<bool> _isActive = isActive ?? throw new ArgumentNullException(nameof(isActive));

    /// <inheritdoc />
    public bool IsActive => _isActive();

    /// <inheritdoc />
    public void Filter(ICollection<string> assertedGroups)
    {
        ArgumentNullException.ThrowIfNull(assertedGroups);

        switch (assertedGroups)
        {
            case List<string> list:
                list.RemoveAll(IsTenantTier);
                return;
            case HashSet<string> set:
                set.RemoveWhere(IsTenantTier);
                return;
        }

        List<string>? doomed = null;
        foreach (var group in assertedGroups)
        {
            if (TenantGroupNesting.IsTenantTier(group))
            {
                (doomed ??= new List<string>()).Add(group);
            }
        }

        if (doomed is null)
        {
            return;
        }

        foreach (var group in doomed)
        {
            assertedGroups.Remove(group);
        }
    }
}

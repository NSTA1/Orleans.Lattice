namespace Orleans.Lattice.Membership;

/// <summary>
/// The default inactive <see cref="ITenantGroupClaimFilter"/>: the null seam a
/// cluster without the tenancy add-on runs with. <see cref="IsActive"/> is
/// <c>false</c>, so claim resolution never invokes <see cref="Filter"/>; were it
/// invoked it leaves the collection untouched, because tenant groups are inert
/// while the tenant tier is absent. Registered by <c>AddLatticeMembership</c> via
/// <c>TryAddSingleton</c> and displaced by the tenancy add-on via <c>Replace</c>.
/// </summary>
internal sealed class NullTenantGroupClaimFilter : ITenantGroupClaimFilter
{
    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public void Filter(ICollection<string> assertedGroups) =>
        ArgumentNullException.ThrowIfNull(assertedGroups);
}

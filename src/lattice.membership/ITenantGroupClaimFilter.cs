namespace Orleans.Lattice.Membership;

/// <summary>
/// The seam that strips identity-provider-asserted group claims in the reserved
/// tenant-group grammar (<see cref="LatticeTenantGroupId"/>, <c>t/{tenant}/{name}</c>)
/// before group expansion, so no identity-provider administrator can join a
/// tenant group by asserting its id in a token claim, a group overage, or any
/// other external source. Tenant groups are created and populated only through
/// the tenant tier. It is an extension seam owned by the tenancy add-on
/// (<c>Orleans.Lattice.Tenancy</c>), declared here so membership can consult it
/// <b>without</b> a project reference into tenancy: tenancy references this
/// package, never the reverse.
/// </summary>
/// <remarks>
/// <para>
/// The default registration is the internal null filter, whose
/// <see cref="IsActive"/> is <c>false</c>, registered by
/// <c>AddLatticeMembership</c> via <c>TryAddSingleton</c>. The tenancy add-on
/// displaces it with <c>Replace</c> and an implementation whose
/// <see cref="IsActive"/> is always <c>true</c>: once tenancy is registered the
/// <c>t/</c> namespace is reserved to the tenant tier whatever its delegated
/// tenant access administration flag says, because operator rules and app role
/// bindings that name a tenant group are honoured whatever the flag. Claim
/// resolution reads <see cref="IsActive"/> first, so a cluster without tenancy
/// resolves claims exactly as it did before this seam existed and does no
/// prefix work; with tenancy, the cold (cache-miss) resolution path pays one
/// ordinal prefix test per claim-derived group and calls <see cref="Filter"/>
/// only when one is a <c>t/</c> id.
/// </para>
/// </remarks>
public interface ITenantGroupClaimFilter
{
    /// <summary>
    /// <c>true</c> when the filter is wired in and enabled; <c>false</c> for the
    /// null default. Callers skip <see cref="Filter"/> entirely when it is
    /// <c>false</c>, so an implementation must answer from in-memory state without
    /// allocating.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Removes, in place, every id in <paramref name="assertedGroups"/> that is in
    /// the reserved tenant-group grammar, leaving every other asserted group
    /// untouched.
    /// </summary>
    /// <param name="assertedGroups">The externally asserted group ids to filter. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="assertedGroups"/> is <c>null</c>.</exception>
    void Filter(ICollection<string> assertedGroups);
}

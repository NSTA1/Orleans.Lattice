namespace Orleans.Lattice.Membership;

/// <summary>
/// The outcome of evaluating a proposed membership edge against the tenant group
/// nesting invariant (see <see cref="TenantGroupNesting"/>).
/// </summary>
internal enum TenantGroupNestingViolation
{
    /// <summary>The edge is permitted.</summary>
    None = 0,

    /// <summary>
    /// The member id is in the reserved <c>t/</c> namespace but is not a
    /// well-formed tenant group id (including <c>t/default/...</c>).
    /// </summary>
    MalformedTenantMember,

    /// <summary>
    /// The parent group id is in the reserved <c>t/</c> namespace but is not a
    /// well-formed tenant group id (including <c>t/default/...</c>).
    /// </summary>
    MalformedTenantGroup,

    /// <summary>A tenant group would become a member of a cluster group.</summary>
    TenantGroupInClusterGroup,

    /// <summary>A tenant group would become a member of another tenant's group.</summary>
    TenantGroupInOtherTenantGroup,
}

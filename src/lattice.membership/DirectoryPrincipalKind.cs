namespace Orleans.Lattice.Membership;

/// <summary>
/// Distinguishes whether a <see cref="DirectoryPrincipal"/> resolved from the
/// external identity source is an individual user or a group. Mirrors
/// <see cref="MembershipMemberKind"/> but describes principals in the upstream
/// directory rather than membership edges in the local directory.
/// </summary>
public enum DirectoryPrincipalKind
{
    /// <summary>The principal is an individual user.</summary>
    User = 0,

    /// <summary>The principal is a group.</summary>
    Group = 1,
}

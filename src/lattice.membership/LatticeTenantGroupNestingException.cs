namespace Orleans.Lattice.Membership;

/// <summary>
/// Thrown by <see cref="ILatticeMembershipDirectory.AddMemberAsync"/> when the
/// proposed edge would break the tenant group nesting invariant: a tenant group
/// (<see cref="LatticeTenantGroupId"/>, <c>t/{tenant}/{name}</c>) may contain
/// users, groups of the same tenant and cluster groups, but may never become a
/// member of a cluster group or of another tenant's group, and an id in the
/// reserved <c>t/</c> namespace that is not a well-formed tenant group id may
/// never join the tenant tier at all. The invariant is enforced for every caller,
/// platform operators included, and the rejection is fail-closed: nothing is
/// written before this exception is raised.
/// </summary>
/// <remarks>
/// <para>
/// Without the invariant, an operator's rule on a cluster group would extend to
/// people a tenant administrator controls, which is an escalation. The test is
/// keyed on the ids alone, never on the edge's <see cref="MembershipMemberKind"/>,
/// because group resolution walks edges by id.
/// </para>
/// <para>
/// Derives from <see cref="ArgumentException"/> because the rejection is caused by
/// caller-supplied ids; the transport bindings map it to a client-facing
/// invalid-argument status rather than an internal error. It is raised and handled
/// in-process on the directory write path and is deliberately not an Orleans
/// serializable type.
/// </para>
/// </remarks>
public sealed class LatticeTenantGroupNestingException : ArgumentException
{
    /// <summary>
    /// The parent group id of the rejected edge. Empty on the parameterless /
    /// message-only constructors.
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// The member id of the rejected edge. Empty on the parameterless /
    /// message-only constructors.
    /// </summary>
    public string MemberId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and empty ids.
    /// Provided to satisfy the framework's exception-construction contract; the
    /// directory raises the context-carrying form.
    /// </summary>
    public LatticeTenantGroupNestingException()
    {
        GroupId = string.Empty;
        MemberId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and empty ids.</summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    public LatticeTenantGroupNestingException(string message) : base(message)
    {
        GroupId = string.Empty;
        MemberId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeTenantGroupNestingException(string message, Exception innerException)
        : base(message, innerException)
    {
        GroupId = string.Empty;
        MemberId = string.Empty;
    }

    private LatticeTenantGroupNestingException(string message, string paramName, string groupId, string memberId)
        : base(message, paramName)
    {
        GroupId = groupId;
        MemberId = memberId;
    }

    /// <summary>Creates the exception for <paramref name="violation"/>.</summary>
    internal static LatticeTenantGroupNestingException Create(
        TenantGroupNestingViolation violation,
        string groupId,
        string memberId)
    {
        var (message, paramName) = violation switch
        {
            TenantGroupNestingViolation.MalformedTenantMember => (
                $"The member id '{memberId}' is in the reserved '{LatticeTenantTrees.SegmentPrefix}' tenant group namespace "
                + "but is not a valid tenant group id, so it cannot be added to any group.",
                "memberId"),
            TenantGroupNestingViolation.MalformedTenantGroup => (
                $"The group id '{groupId}' is in the reserved '{LatticeTenantTrees.SegmentPrefix}' tenant group namespace "
                + "but is not a valid tenant group id, so it cannot hold members.",
                "groupId"),
            TenantGroupNestingViolation.TenantGroupInClusterGroup => (
                $"The tenant group '{memberId}' cannot be a member of the cluster group '{groupId}': a tenant group may "
                + "contain cluster groups, but may never be nested inside one.",
                "memberId"),
            TenantGroupNestingViolation.TenantGroupInOtherTenantGroup => (
                $"The tenant group '{memberId}' cannot be a member of '{groupId}', which belongs to a different tenant: "
                + "a tenant group may only be nested in groups of its own tenant.",
                "memberId"),
            _ => throw new ArgumentOutOfRangeException(nameof(violation), violation, "Not a nesting violation."),
        };

        return new LatticeTenantGroupNestingException(message, paramName, groupId, memberId);
    }
}

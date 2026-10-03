namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Thrown by <see cref="ILatticeAuthAdmin.UpsertGroupAsync"/> when the group id is in
/// the reserved tenant-tier grammar: any id that starts with
/// <see cref="LatticeTenantTrees.SegmentPrefix"/> (<c>t/</c>), the namespace of
/// tenant groups (<see cref="LatticeTenantGroupId"/>, <c>t/{tenant}/{name}</c>).
/// Tenant groups are created and managed by a tenant's own administrators through
/// the tenant directory administration surface
/// (<c>Orleans.Lattice.Api.TenantAdmin.ILatticeTenantDirectoryAdmin</c>), which
/// confines them to their tenant; the cluster facade refuses the whole namespace,
/// malformed ids included, so an operator cannot mint a group a tenant would
/// otherwise own. The refusal is fail-closed: nothing is read or written before
/// this exception is raised.
/// </summary>
/// <remarks>
/// <para>
/// To manage cluster-wide membership, choose a group id outside the <c>t/</c>
/// namespace. A platform operator can still see every tenant's groups through
/// <see cref="ILatticeAuthAdmin.ListGroupsAsync"/> with
/// <see cref="AuthPageRequest.IncludeTenantGroups"/> set.
/// </para>
/// <para>
/// Derives from <see cref="ArgumentException"/> because the rejection is caused by a
/// caller-supplied group id; the transport bindings map it to a client-facing
/// invalid-argument status rather than an internal error. It is raised and handled
/// in-process by the facade and is deliberately not an Orleans serializable type.
/// </para>
/// </remarks>
public sealed class LatticeTenantOwnedGroupException : ArgumentException
{
    /// <summary>
    /// The reserved group id the rejected write targeted. Empty on the
    /// parameterless / message-only constructors.
    /// </summary>
    public string GroupId { get; }

    /// <summary>
    /// Initialises a new instance with no diagnostic message and an empty
    /// <see cref="GroupId"/>. Provided to satisfy the framework's
    /// exception-construction contract; the facade uses the context-carrying
    /// factory method.
    /// </summary>
    public LatticeTenantOwnedGroupException()
    {
        GroupId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and an empty <see cref="GroupId"/>.</summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    public LatticeTenantOwnedGroupException(string message) : base(message)
    {
        GroupId = string.Empty;
    }

    /// <summary>Initialises a new instance with the specified diagnostic message and wrapped inner exception.</summary>
    /// <param name="message">Diagnostic context describing the rejection.</param>
    /// <param name="innerException">The underlying cause.</param>
    public LatticeTenantOwnedGroupException(string message, Exception innerException)
        : base(message, innerException)
    {
        GroupId = string.Empty;
    }

    private LatticeTenantOwnedGroupException(string message, string paramName, string groupId)
        : base(message, paramName)
    {
        GroupId = groupId;
    }

    /// <summary>
    /// Creates the exception the cluster facade raises for a rejected write of the
    /// reserved group id <paramref name="groupId"/>.
    /// </summary>
    /// <param name="groupId">The reserved group id that was targeted. Must not be <c>null</c>.</param>
    /// <param name="paramName">The name of the offending facade parameter. Must not be <c>null</c>.</param>
    /// <returns>A configured <see cref="LatticeTenantOwnedGroupException"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="groupId"/> or <paramref name="paramName"/> is <c>null</c>.</exception>
    public static LatticeTenantOwnedGroupException Rejected(string groupId, string paramName)
    {
        ArgumentNullException.ThrowIfNull(groupId);
        ArgumentNullException.ThrowIfNull(paramName);
        var message = $"The group id '{groupId}' is in the reserved tenant-group namespace (it starts with "
            + $"'{LatticeTenantTrees.SegmentPrefix}') and cannot be created through the cluster authorization facade. "
            + "Tenant groups are managed by the tenant's own administrators through the tenant directory "
            + "administration surface (ILatticeTenantDirectoryAdmin), which confines them to their tenant. "
            + $"Choose a cluster group id outside the '{LatticeTenantTrees.SegmentPrefix}' namespace instead.";
        return new LatticeTenantOwnedGroupException(message, paramName, groupId);
    }
}

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// One subject entry - a tenant group member or a tenant member-set entry - as the
/// tenant access tools report it.
/// </summary>
internal sealed record McpTenantSubject
{
    /// <summary>
    /// The subject id: a user id, a cluster group id, or a tenant group's
    /// tenant-local name, as distinguished by <see cref="Kind"/>.
    /// </summary>
    public required string SubjectId { get; init; }

    /// <summary>The subject kind: <c>User</c>, <c>TenantGroup</c> or <c>ClusterGroup</c>.</summary>
    public required string Kind { get; init; }
}

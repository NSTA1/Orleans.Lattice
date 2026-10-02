namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the tenant group member and tenant member
/// add / remove tools. Every change is idempotent, so <see cref="Changed"/> reports
/// whether the call actually changed anything.
/// </summary>
internal sealed record McpTenantMembershipChangeResult
{
    /// <summary>The tenant the change was made for.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant-local group name for a group-member change; <see langword="null"/> for a member-set change.</summary>
    public string? GroupName { get; init; }

    /// <summary>The subject that was added or removed.</summary>
    public required string SubjectId { get; init; }

    /// <summary>The subject kind: <c>User</c>, <c>TenantGroup</c> or <c>ClusterGroup</c>.</summary>
    public required string SubjectKind { get; init; }

    /// <summary>Whether the call changed anything.</summary>
    public required bool Changed { get; init; }
}

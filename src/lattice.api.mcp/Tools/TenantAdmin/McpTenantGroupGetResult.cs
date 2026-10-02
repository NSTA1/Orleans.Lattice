namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_group_get</c> tool.
/// A group that does not exist - including another tenant's group, which reads as
/// not found - reports <see cref="Found"/> as <see langword="false"/> rather than
/// failing the call.
/// </summary>
internal sealed record McpTenantGroupGetResult
{
    /// <summary>The tenant the read was made for.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant-local group name that was read.</summary>
    public required string Name { get; init; }

    /// <summary>Whether the tenant owns a group of that name.</summary>
    public required bool Found { get; init; }

    /// <summary>The group's display name, or <see langword="null"/> when none was set or the group was not found.</summary>
    public string? DisplayName { get; init; }
}

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// One tenant group as the tenant access tools report it: the group's
/// tenant-local name and optional display name.
/// </summary>
internal sealed record McpTenantGroup
{
    /// <summary>The group's tenant-local name (never the composed <c>t/{tenant}/{name}</c> id).</summary>
    public required string Name { get; init; }

    /// <summary>The group's display name, or <see langword="null"/> when none was set.</summary>
    public string? DisplayName { get; init; }
}

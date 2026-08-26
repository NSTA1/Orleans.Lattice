namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_create</c> tool:
/// the id of the tenant that was created, the lifecycle status it was created
/// in (always <c>Active</c>), and the tenant-admin subjects seeded onto it.
/// </summary>
internal sealed record McpTenantCreateResult
{
    /// <summary>The tenant id that was created.</summary>
    public required string TenantId { get; init; }

    /// <summary>The lifecycle status name the tenant was created in (always <c>Active</c>).</summary>
    public required string Status { get; init; }

    /// <summary>
    /// The tenant-admin subjects seeded onto the new tenant. These are the
    /// subjects that can immediately see it through <c>lattice_tenant_list</c> /
    /// <c>lattice_tenant_get</c>; an empty list means the tenant is invisible to
    /// every caller until subjects are added.
    /// </summary>
    public required IReadOnlyList<string> AdminSubjects { get; init; }
}

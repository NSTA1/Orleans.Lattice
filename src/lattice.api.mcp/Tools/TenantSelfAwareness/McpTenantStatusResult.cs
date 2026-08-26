namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_tenant_get</c> tool: the
/// read-only lifecycle status and per-region residency of one tenant the caller is
/// authorized to see. It is only ever produced for an accessible tenant; an absent
/// tenant and a tenant the caller cannot see are unified into the same fail-closed
/// "not found" fault at the facade, so this result never confirms a tenant the
/// caller has no right to observe.
/// </summary>
internal sealed record McpTenantStatusResult
{
    /// <summary>The tenant id this report describes.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant's lifecycle status name (for example <c>Active</c> or <c>Suspended</c>).</summary>
    public required string Status { get; init; }

    /// <summary>Whether this is the reserved legacy-adoption default tenant.</summary>
    public required bool IsDefault { get; init; }

    /// <summary>The tenant's per-region residency rows; empty when it has no per-region residency configured.</summary>
    public required IReadOnlyList<McpTenantRegionStatusRow> Regions { get; init; }

    /// <summary>The tenant's resource quotas and burst allowance in effect.</summary>
    public required McpTenantQuotasView Quotas { get; init; }
}

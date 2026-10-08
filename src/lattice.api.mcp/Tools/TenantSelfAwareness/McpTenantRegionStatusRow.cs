namespace Orleans.Lattice.Api.Mcp;

using Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One row of the <c>lattice_tenant_get</c> tool's per-region residency report: a
/// region id, the tenant's current lifecycle status name for that region, and
/// whether the region is in the tenant's operator-authorized allowed set.
/// </summary>
internal sealed record McpTenantRegionStatusRow
{
    /// <summary>The region id this row describes.</summary>
    public required string RegionId { get; init; }

    /// <summary>The region's current lifecycle status name for the tenant (for example <c>Online</c>).</summary>
    public required string Status { get; init; }

    /// <summary>Whether the region is in the tenant's operator-authorized allowed set.</summary>
    public required bool IsAllowed { get; init; }

    /// <summary>Receiver-side tenant backfill progress when available.</summary>
    public TenantRegionBackfillProgress? BackfillProgress { get; init; }
}

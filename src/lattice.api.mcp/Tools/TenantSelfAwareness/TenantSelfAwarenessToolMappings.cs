using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Pure projections from the read-only tenant self-awareness facade's domain
/// results onto the compact MCP structured-content DTOs the tenant self-awareness
/// tools return. Kept side-effect free and allocation-lean so a tool invocation
/// maps a facade result without any I/O of its own.
/// </summary>
internal static class TenantSelfAwarenessToolMappings
{
    /// <summary>Projects a tenant descriptor onto its MCP DTO.</summary>
    /// <param name="descriptor">The tenant descriptor. Must not be <c>null</c>.</param>
    /// <returns>The MCP tenant-descriptor DTO.</returns>
    public static McpTenantDescriptor ToMcp(TenantDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        return new McpTenantDescriptor
        {
            TenantId = descriptor.TenantId,
            Status = descriptor.Status.ToString(),
            IsDefault = descriptor.IsDefault,
        };
    }

    /// <summary>Projects the accessible-tenants list onto its MCP DTO.</summary>
    /// <param name="tenants">The accessible tenants. Must not be <c>null</c>.</param>
    /// <returns>The MCP tenant-list DTO.</returns>
    public static McpTenantListResult ToMcp(IReadOnlyList<TenantDescriptor> tenants)
    {
        ArgumentNullException.ThrowIfNull(tenants);
        var rows = new McpTenantDescriptor[tenants.Count];
        for (var i = 0; i < tenants.Count; i++)
        {
            rows[i] = ToMcp(tenants[i]);
        }

        return new McpTenantListResult { Tenants = rows };
    }

    /// <summary>Projects a tenant status report onto its MCP DTO.</summary>
    /// <param name="report">The tenant status report. Must not be <c>null</c>.</param>
    /// <returns>The MCP tenant-status DTO.</returns>
    public static McpTenantStatusResult ToMcp(TenantStatusReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        var regions = new McpTenantRegionStatusRow[report.Regions.Count];
        for (var i = 0; i < report.Regions.Count; i++)
        {
            var row = report.Regions[i];
            regions[i] = new McpTenantRegionStatusRow
            {
                RegionId = row.RegionId,
                Status = row.Status.ToString(),
                IsAllowed = row.IsAllowed,
            };
        }

        return new McpTenantStatusResult
        {
            TenantId = report.TenantId,
            Status = report.Status.ToString(),
            IsDefault = report.IsDefault,
            Regions = regions,
            Quotas = new McpTenantQuotasView
            {
                MaxBytes = report.Quotas.MaxBytes,
                MaxKeys = report.Quotas.MaxKeys,
                MaxMemoryBytes = report.Quotas.MaxMemoryBytes,
                MaxTreeCount = report.Quotas.MaxTreeCount,
                MaxOpsPerSecond = report.Quotas.MaxOpsPerSecond,
                BurstPercent = report.Quotas.BurstPercent,
                IsUnbounded = report.Quotas.IsUnbounded,
            },
        };
    }
}

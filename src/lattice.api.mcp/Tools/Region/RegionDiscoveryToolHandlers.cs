using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The handler behind the <c>lattice_list_regions</c> discovery meta-tool. It is a
/// thin adapter over the transport-agnostic
/// <see cref="Orleans.Lattice.Api.Region.ILatticeRegionCatalog"/>:
/// it resolves the catalog from the invocation's request service provider and
/// projects the catalog's descriptors into the tool result, so the region model
/// stays owned by the shared contract rather than by the MCP surface.
/// </summary>
internal static class RegionDiscoveryToolHandlers
{
    /// <summary>
    /// Lists the regions the server can route to, current region first, each with
    /// per-group reachability.
    /// </summary>
    /// <param name="context">The tool invocation context.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>The region discovery result.</returns>
    public static async Task<LatticeApiMcpRegionsToolResult> ListRegionsAsync(
        RequestContext<CallToolRequestParams> context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; lattice_list_regions cannot resolve the region catalog.");

        var catalog = services.GetRequiredService<Region.ILatticeRegionCatalog>();
        var router = services.GetRequiredService<ILatticeApiMcpRegionRouter>();

        // The discovery meta-tool is not wrapped by the credential-stamping
        // decorator (it is reachable before a caller has selected a facade group),
        // so it lifts the caller's asserted tenant onto the ambient context itself.
        // The scope is the shared allocation-free no-op when no tenant is asserted,
        // which keeps a tenancy-off head byte-for-byte unchanged.
        using var tenantScope = McpToolActiveTenantScope.Stamp(services);

        var regions = await catalog.ListRegionsAsync(cancellationToken).ConfigureAwait(false);
        return new LatticeApiMcpRegionsToolResult
        {
            CurrentRegion = router.DefaultRegionId,
            Regions = regions,
        };
    }
}

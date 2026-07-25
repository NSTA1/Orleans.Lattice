using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Region;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The <see cref="ILatticeRegionCatalog"/> the MCP binding advertises regions
/// from, projected from the <see cref="ILatticeApiMcpRegionRouter"/> so discovery
/// and routing share a single source of truth. The router owns the static
/// per-region reachability; this adapter enriches the current region's cluster id
/// from the state facade at read time when the router did not already know it (the
/// in-silo topology resolves the cluster id only at runtime).
/// </summary>
internal sealed class LatticeApiMcpRegionCatalog : ILatticeRegionCatalog
{
    private readonly ILatticeApiMcpRegionRouter _router;
    private readonly IServiceProvider _services;

    /// <summary>Initialises the catalog over the region router and service provider.</summary>
    public LatticeApiMcpRegionCatalog(ILatticeApiMcpRegionRouter router, IServiceProvider services)
    {
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _services = services ?? throw new ArgumentNullException(nameof(services));
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<LatticeRegionDescriptor>> ListRegionsAsync(
        CancellationToken cancellationToken = default)
    {
        var snapshot = _router.Snapshot();

        // Fast path: the router already knows every cluster id (the remote-host
        // topology names them in configuration), so return the frozen snapshot
        // verbatim with no allocation.
        if (!NeedsClusterIdEnrichment(snapshot))
        {
            return snapshot;
        }

        var clusterId = await ResolveCurrentClusterIdAsync(cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(clusterId))
        {
            return snapshot;
        }

        var enriched = new LatticeRegionDescriptor[snapshot.Count];
        for (var i = 0; i < snapshot.Count; i++)
        {
            var descriptor = snapshot[i];
            enriched[i] = descriptor.IsCurrent && string.IsNullOrEmpty(descriptor.ClusterId)
                ? descriptor with { ClusterId = clusterId }
                : descriptor;
        }

        return enriched;
    }

    private static bool NeedsClusterIdEnrichment(IReadOnlyList<LatticeRegionDescriptor> snapshot)
    {
        for (var i = 0; i < snapshot.Count; i++)
        {
            if (snapshot[i].IsCurrent && string.IsNullOrEmpty(snapshot[i].ClusterId))
            {
                return true;
            }
        }

        return false;
    }

    private async Task<string?> ResolveCurrentClusterIdAsync(CancellationToken cancellationToken)
    {
        var stateQuery = _services.GetService<ILatticeStateQuery>();
        if (stateQuery is null)
        {
            return null;
        }

        try
        {
            var info = await stateQuery.GetClusterInfoAsync(cancellationToken).ConfigureAwait(false);
            return info.ClusterId;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Discovery is best-effort on the cluster-id decoration only; the
            // region set itself is authoritative from the router.
            return null;
        }
    }
}

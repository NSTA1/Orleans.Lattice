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
        var verifier = _services.GetService<ILatticeApiMcpRegionIdentityVerifier>();
        var needsEnrichment = NeedsClusterIdEnrichment(snapshot);

        // Fast path: no verification configured and every cluster id already known,
        // so return the frozen snapshot verbatim with no allocation.
        if (verifier is null && !needsEnrichment)
        {
            return snapshot;
        }

        var clusterId = needsEnrichment
            ? await ResolveCurrentClusterIdAsync(cancellationToken).ConfigureAwait(false)
            : null;

        var result = new List<LatticeRegionDescriptor>(snapshot.Count);
        for (var i = 0; i < snapshot.Count; i++)
        {
            var descriptor = snapshot[i];

            if (descriptor.IsCurrent)
            {
                if (string.IsNullOrEmpty(descriptor.ClusterId) && !string.IsNullOrEmpty(clusterId))
                {
                    descriptor = descriptor with { ClusterId = clusterId };
                }

                result.Add(descriptor);
                continue;
            }

            // Fail-closed discovery: a peer whose endpoint does not provably reach
            // its own cluster (or is unreachable) is omitted, so a caller is never
            // pointed at a region a subsequent tool call could not honour.
            if (verifier is not null)
            {
                var verdict = await verifier.VerifyAsync(descriptor.RegionId, cancellationToken)
                    .ConfigureAwait(false);
                if (verdict is RegionIdentityVerdict.Mismatch or RegionIdentityVerdict.Unreachable)
                {
                    continue;
                }
            }

            result.Add(descriptor);
        }

        return result;
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

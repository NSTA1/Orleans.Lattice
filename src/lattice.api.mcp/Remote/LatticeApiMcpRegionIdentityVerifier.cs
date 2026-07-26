using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Region;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The default <see cref="ILatticeApiMcpRegionIdentityVerifier"/>: it derives each
/// region's advertised identity from the <see cref="ILatticeApiMcpRegionRouter"/>
/// snapshot and proves it by probing that region's state facade
/// (<see cref="ILatticeStateQuery.GetClusterInfoAsync"/>) under the region's own
/// scope, comparing the reported cluster id to the advertised one. The result is
/// memoised per region so verification costs a single probe, then a frozen
/// dictionary hit.
/// </summary>
internal sealed class LatticeApiMcpRegionIdentityVerifier : ILatticeApiMcpRegionIdentityVerifier
{
    // Bounds a single probe so a black-holed endpoint cannot hang the shared task
    // (and every caller awaiting it) indefinitely; a timeout is treated as
    // unreachable and re-probed on the next attempt.
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(10);

    private readonly ILatticeApiMcpRegionRouter _router;
    private readonly IServiceProvider _services;
    private readonly ConcurrentDictionary<string, Task<RegionIdentityVerdict>> _cache =
        new(StringComparer.Ordinal);

    /// <summary>Initialises the verifier over the region router and service provider.</summary>
    public LatticeApiMcpRegionIdentityVerifier(ILatticeApiMcpRegionRouter router, IServiceProvider services)
    {
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _services = services ?? throw new ArgumentNullException(nameof(services));
    }

    /// <inheritdoc />
    public async ValueTask<RegionIdentityVerdict> VerifyAsync(
        string regionId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);

        var probe = _cache.GetOrAdd(regionId, static (id, self) => self.ProbeAsync(id), this);

        // Wait on the shared probe but honour the caller's own cancellation without
        // cancelling the probe itself, so a cancelled caller never poisons the
        // verdict another caller (or a later call) is relying on.
        var verdict = await probe.WaitAsync(cancellationToken).ConfigureAwait(false);

        // An unreachable region is transient - drop it from the cache so the next
        // call re-probes once the region (or the network to it) recovers. A stable
        // verified/mismatch verdict is kept for the process lifetime.
        if (verdict == RegionIdentityVerdict.Unreachable)
        {
            _cache.TryRemove(new KeyValuePair<string, Task<RegionIdentityVerdict>>(regionId, probe));
        }

        return verdict;
    }

    private async Task<RegionIdentityVerdict> ProbeAsync(string regionId)
    {
        var descriptor = FindDescriptor(regionId);
        if (descriptor is null || descriptor.IsCurrent)
        {
            // The current region is local and authoritative; an unknown region is
            // rejected by the router before it is ever routed to.
            return RegionIdentityVerdict.Skipped;
        }

        var expectedClusterId = descriptor.ClusterId;
        if (string.IsNullOrEmpty(expectedClusterId) || !ServesState(descriptor))
        {
            // Nothing to assert against: no advertised cluster id, or no state facade
            // in the region to probe. It cannot be proven wrong, so it stays routable.
            return RegionIdentityVerdict.Skipped;
        }

        var stateQuery = _services.GetService<ILatticeStateQuery>();
        if (stateQuery is null)
        {
            return RegionIdentityVerdict.Skipped;
        }

        try
        {
            using var timeout = new CancellationTokenSource(ProbeTimeout);
            using (LatticeApiMcpRegionScope.Enter(regionId))
            {
                var info = await stateQuery.GetClusterInfoAsync(timeout.Token).ConfigureAwait(false);
                return string.Equals(info.ClusterId, expectedClusterId, StringComparison.Ordinal)
                    ? RegionIdentityVerdict.Verified
                    : RegionIdentityVerdict.Mismatch;
            }
        }
        catch (Exception)
        {
            // Any failure - transport error, timeout (our own token), or a faulting
            // facade - leaves the identity unproven. Fail-closed for this attempt.
            return RegionIdentityVerdict.Unreachable;
        }
    }

    private LatticeRegionDescriptor? FindDescriptor(string regionId)
    {
        var snapshot = _router.Snapshot();
        for (var i = 0; i < snapshot.Count; i++)
        {
            if (string.Equals(snapshot[i].RegionId, regionId, StringComparison.Ordinal))
            {
                return snapshot[i];
            }
        }

        return null;
    }

    private static bool ServesState(LatticeRegionDescriptor descriptor)
    {
        var stateName = LatticeApiMcpGroupCapabilityMap.DisplayName(LatticeApiMcpGroup.State);
        var groups = descriptor.Groups;
        for (var i = 0; i < groups.Count; i++)
        {
            if (groups[i].Available && string.Equals(groups[i].Group, stateName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}

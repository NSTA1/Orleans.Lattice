using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Records a cross-tree purge frontier an origin advertised on an
/// authenticated push (issue #4733) on this receiver's per-origin frontier,
/// which drops every decided barrier tombstone the frontier has passed on all
/// its participants. The transport binding owns the authentication; this only
/// forwards.
/// </summary>
internal static class CrossTreePurgeFrontierRecorder
{
    /// <summary>Raises <paramref name="originClusterId"/>'s recorded frontier to <paramref name="frontier"/>.</summary>
    public static Task RecordAsync(IGrainFactory grainFactory, string originClusterId, CrossTreePurgeFrontier frontier)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        ArgumentNullException.ThrowIfNull(frontier);
        return grainFactory.GetGrain<ICrossTreePurgeFrontierGrain>(originClusterId).AdvanceAsync(frontier.Frontiers);
    }
}

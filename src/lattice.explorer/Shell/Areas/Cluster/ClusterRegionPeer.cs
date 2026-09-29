using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>One peer region in the region picture: every link this region has with it, rolled up.</summary>
/// <param name="RegionId">The peer's region id.</param>
/// <param name="Links">The (tree, direction) links with the peer.</param>
/// <param name="Healthy">Links reported healthy.</param>
/// <param name="Lagging">Links reported lagging.</param>
/// <param name="Stalled">Links reported stalled.</param>
/// <param name="Trees">The distinct trees replicated with the peer.</param>
internal sealed record ClusterRegionPeer(string RegionId, int Links, int Healthy, int Lagging, int Stalled, int Trees)
{
    /// <summary>
    /// Whether every link with the peer is stalled. Only then is the peer drawn
    /// dashed and labelled stalled; one live link keeps it connected.
    /// </summary>
    public bool IsStalled => Links > 0 && Stalled == Links;

    /// <summary>The peer's state for its pill: stalled, lagging when any link lags or stalls, healthy, or unknown.</summary>
    public ReplicationLinkHealth Health => this switch
    {
        { IsStalled: true } => ReplicationLinkHealth.Stalled,
        { Lagging: > 0 } or { Stalled: > 0 } => ReplicationLinkHealth.Lagging,
        { Healthy: > 0 } => ReplicationLinkHealth.Healthy,
        _ => ReplicationLinkHealth.Unknown,
    };
}

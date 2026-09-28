using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>One tree as the Cluster area lists it: by logical id only.</summary>
/// <param name="Name">The logical name and its owners.</param>
/// <param name="IsAliased">Whether the logical id resolves to another physical tree (after a resize or restore). The physical id is never carried.</param>
/// <param name="Lifecycle">Live or soft-deleted.</param>
/// <param name="ShardCount">The physical shard count.</param>
/// <param name="VirtualShardCount">The virtual routing space.</param>
/// <param name="WalPartitions">The WAL partition count.</param>
internal sealed record ClusterTreeEntry(
    ClusterTreeName Name,
    bool IsAliased,
    TreeLifecycleState Lifecycle,
    int ShardCount,
    int VirtualShardCount,
    int WalPartitions)
{
    /// <summary>The logical tree id.</summary>
    public string TreeId => Name.TreeId;
}

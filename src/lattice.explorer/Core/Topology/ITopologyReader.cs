namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// Fetches tree structure from the state API for the topology graph. View ids
/// are passed through unchanged, exactly like tree ids.
/// </summary>
public interface ITopologyReader
{
    /// <summary>
    /// Fetches the bounded structure for <paramref name="treeId"/> from the
    /// shard roots down, within the depth and node budget.
    /// </summary>
    Task<TopologyFetch> GetAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lazily expands the subtree rooted at <paramref name="subPathNodeId"/>
    /// within <paramref name="shardIndex"/>, for nodes that reported
    /// <see cref="TopologyNode.HasMoreChildren"/>.
    /// </summary>
    Task<TopologyFetch> ExpandAsync(
        string treeId,
        int shardIndex,
        string subPathNodeId,
        CancellationToken cancellationToken = default);
}

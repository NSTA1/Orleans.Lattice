using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Plugins.Topology;

/// <summary>
/// The controlled domain model of the tree-topology surface: the bounded
/// structure fetch and the lazy subtree expansion, and nothing else.
/// <para>
/// This is the whole of the plugin's reach (epic decision D3). The surface never
/// receives the state-API connection, a gRPC channel, or a service locator - the
/// host resolves exactly this contract for it.
/// </para>
/// </summary>
public interface ITopologySurface
{
    /// <summary>
    /// Fetches the bounded structure for <paramref name="treeId"/> from the
    /// shard roots down, within the depth and node budget.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<TopologyFetch> GetAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lazily expands the subtree rooted at <paramref name="subPathNodeId"/>
    /// within <paramref name="shardIndex"/>, for a node that reported more
    /// children than the budget returned.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="shardIndex">The shard the node belongs to.</param>
    /// <param name="subPathNodeId">The node to expand. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<TopologyFetch> ExpandAsync(
        string treeId,
        int shardIndex,
        string subPathNodeId,
        CancellationToken cancellationToken = default);
}

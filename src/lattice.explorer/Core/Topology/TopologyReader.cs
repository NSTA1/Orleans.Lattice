using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// Default <see cref="ITopologyReader"/> over the state-API structure surface.
/// </summary>
public sealed class TopologyReader(ILatticeStateClient client) : ITopologyReader
{
    private readonly ILatticeStateClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public Task<TopologyFetch> GetAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return FetchAsync(new StructureRequest { TreeId = treeId }, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TopologyFetch> ExpandAsync(
        string treeId,
        int shardIndex,
        string subPathNodeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(subPathNodeId);

        var request = new StructureRequest
        {
            TreeId = treeId,
            ShardIndex = shardIndex,
            SubPathNodeId = subPathNodeId,
        };
        return FetchAsync(request, cancellationToken);
    }

    private async Task<TopologyFetch> FetchAsync(StructureRequest request, CancellationToken cancellationToken)
    {
        var response = await _client.GetTreeStructureAsync(request, cancellationToken).ConfigureAwait(false);

        var roots = response.Roots.Count == 0
            ? Array.Empty<TopologyNode>()
            : response.Roots.Select(TopologyNode.From).ToArray();

        return new TopologyFetch { Roots = roots, Truncated = response.Truncated };
    }
}

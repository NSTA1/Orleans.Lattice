using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Plugins.Topology;

/// <summary>
/// The one place in this package that touches an Explorer service. It adapts the
/// shared topology reader onto <see cref="ITopologySurface"/>, so the view
/// depends on the narrow contract rather than on the Explorer core.
/// </summary>
/// <param name="reader">The shared topology reader.</param>
internal sealed class TopologySurface(ITopologyReader reader) : ITopologySurface
{
    private readonly ITopologyReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));

    /// <inheritdoc />
    public Task<TopologyFetch> GetAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _reader.GetAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TopologyFetch> ExpandAsync(
        string treeId,
        int shardIndex,
        string subPathNodeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(subPathNodeId);
        return _reader.ExpandAsync(treeId, shardIndex, subPathNodeId, cancellationToken);
    }
}

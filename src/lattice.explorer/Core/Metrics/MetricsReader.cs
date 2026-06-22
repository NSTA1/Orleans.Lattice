using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Metrics;

/// <summary>
/// Default <see cref="IMetricsReader"/> over the state-API metrics surface.
/// </summary>
public sealed class MetricsReader(ILatticeStateClient client) : IMetricsReader
{
    private readonly ILatticeStateClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<TreeMetrics?> GetAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var request = new TreeMetricsRequest
        {
            TreeIds = new[] { treeId },
            IncludeShardHotness = true,
            IncludeViewLag = true,
            IncludeSystemTrees = true,
        };

        var snapshot = await _client.GetMetricsSnapshotAsync(request, cancellationToken).ConfigureAwait(false);

        foreach (var tree in snapshot.Trees)
        {
            if (string.Equals(tree.TreeId, treeId, StringComparison.Ordinal))
            {
                return tree;
            }
        }

        return null;
    }
}

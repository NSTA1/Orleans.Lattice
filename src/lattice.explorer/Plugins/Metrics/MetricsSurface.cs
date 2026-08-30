using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Metrics;

namespace Orleans.Lattice.Explorer.Plugins.Metrics;

/// <summary>
/// The one place in this package that touches an Explorer service. It adapts the
/// shared metrics reader and the shell's selection onto
/// <see cref="IMetricsSurface"/>, so the view - and every future view in this
/// package - depends on the narrow contract rather than on the Explorer core.
/// </summary>
/// <param name="reader">The shared metrics reader.</param>
/// <param name="selection">The shell's current-selection service.</param>
internal sealed class MetricsSurface(IMetricsReader reader, IExplorerSelection selection) : IMetricsSurface
{
    private readonly IMetricsReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));

    private readonly IExplorerSelection _selection =
        selection ?? throw new ArgumentNullException(nameof(selection));

    /// <inheritdoc />
    public Task<TreeMetrics?> GetAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _reader.GetAsync(treeId, cancellationToken);
    }

    /// <inheritdoc />
    public void GoToTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        _selection.Select(new CatalogItem { Id = treeId, Kind = CatalogKind.Trees });
    }
}

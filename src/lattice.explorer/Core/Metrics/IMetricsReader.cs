using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Core.Metrics;

/// <summary>
/// Fetches the metrics snapshot for a single selected tree or view from the
/// state-API metrics surface. View ids are passed through unchanged, exactly
/// like tree ids.
/// </summary>
public interface IMetricsReader
{
    /// <summary>
    /// Requests a one-shot metrics snapshot for <paramref name="treeId"/>,
    /// opting into per-shard hotness and view lag, and returns the matching
    /// <see cref="TreeMetrics"/>, or <see langword="null"/> when the surface
    /// reports none for that id.
    /// </summary>
    Task<TreeMetrics?> GetAsync(string treeId, CancellationToken cancellationToken = default);
}

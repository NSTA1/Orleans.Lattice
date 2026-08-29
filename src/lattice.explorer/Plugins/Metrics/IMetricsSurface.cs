using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Plugins.Metrics;

/// <summary>
/// The controlled domain model of the live-metrics surface: one snapshot read
/// and one navigation, and nothing else.
/// <para>
/// This is the whole of the plugin's reach (epic decision D3). The surface never
/// receives the state-API connection, a gRPC channel, or a service locator - the
/// host resolves exactly this contract for it, so what the plugin can touch is
/// readable from its own source and reviewable in isolation.
/// </para>
/// </summary>
public interface IMetricsSurface
{
    /// <summary>
    /// Reads a one-shot metrics snapshot for <paramref name="treeId"/>, or
    /// <see langword="null"/> when the cluster reports none for that id.
    /// </summary>
    /// <param name="treeId">The selected tree or view id. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancelled when the view is torn down.</param>
    Task<TreeMetrics?> GetAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Selects <paramref name="treeId"/> as a tree, so a change-history view can
    /// route the operator to the source table that actually reports metrics.
    /// </summary>
    /// <param name="treeId">The source tree id to select. Must not be <see langword="null"/>.</param>
    void GoToTree(string treeId);
}

namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Drives every registered plugin's access gate and files the results in the
/// keyed access store, with per-plugin fault isolation.
/// <para>
/// The host owns no per-plugin gating knowledge: it probes whatever plugins are
/// registered, in isolation from one another. A gate that throws is contained
/// and resolves to <see cref="ExplorerPluginAccess.Denied"/> - never to allowed
/// - and a gate that never completes leaves only its own key at the fail-closed
/// default while every sibling's decision still lands.
/// </para>
/// </summary>
public interface IExplorerPluginAccessRefresher
{
    /// <summary>
    /// Probes every registered plugin's gate and files each result as it
    /// arrives. Probes run concurrently and independently, so a slow or hung
    /// gate delays only its own key. The returned task completes when every
    /// probe has settled or <paramref name="cancellationToken"/> has cancelled
    /// them; a cancelled probe leaves its key denied.
    /// </summary>
    /// <param name="cancellationToken">Cancels the outstanding probes.</param>
    Task RefreshAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes a single plugin's gate and files its result, with the same fault
    /// isolation. Does nothing when no plugin is registered under
    /// <paramref name="pluginId"/>.
    /// </summary>
    /// <param name="pluginId">The plugin to re-probe. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    Task RefreshAsync(string pluginId, CancellationToken cancellationToken = default);
}

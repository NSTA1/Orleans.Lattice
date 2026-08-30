namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// A plugin's own access gate: the side-effect-free probe that decides whether
/// the plugin is reachable for the current caller.
/// <para>
/// A gate is owned by its plugin, never by the host. The host probes every gate
/// independently and fault-isolates each one, so a gate that throws, or never
/// completes, cannot disturb another plugin's decision. A gate that faults
/// resolves to <see cref="ExplorerPluginAccess.Denied"/>, never to allowed.
/// </para>
/// <para>
/// Implementations should be cheap and free of side effects: the host may probe
/// on mount, on a sign-in change, and on every reconnect.
/// </para>
/// </summary>
public interface IExplorerPluginAccessGate
{
    /// <summary>
    /// Probes access for the caller described by <paramref name="context"/>.
    /// <para>
    /// Returns <see cref="ValueTask{TResult}"/> so a gate with a synchronous
    /// answer - a fixed decision, or one already cached from an earlier probe -
    /// completes without allocating a task.
    /// </para>
    /// </summary>
    /// <param name="context">
    /// The probing plugin's own host context. Bound to this plugin's id, so a
    /// gate cannot reach another plugin's preferences or domain model. Never
    /// <see langword="null"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The resolved access decision.</returns>
    ValueTask<ExplorerPluginAccess> ProbeAsync(
        IExplorerPluginHostContext context,
        CancellationToken cancellationToken = default);
}

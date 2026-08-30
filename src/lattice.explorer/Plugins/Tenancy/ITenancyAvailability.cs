using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// Decides whether the Explorer's tenancy surfaces exist for the current caller
/// at all, resolving onto the shell's four-state
/// <see cref="ExplorerPluginAccess"/> so a tenancy plugin's gate is a delegation
/// rather than a re-implementation.
/// <para>
/// The state that matters here is
/// <see cref="ExplorerPluginAccessState.Unavailable"/>: it means the cluster
/// does not serve tenancy, so the shell renders no entry at all rather than a
/// greyed-out one the operator could never be granted. The other three states
/// keep the surface visible, because a caller who is merely refused - or merely
/// not signed in - should see that a tenancy surface exists.
/// </para>
/// </summary>
public interface ITenancyAvailability
{
    /// <summary>
    /// Probes whether the tenancy surfaces are reachable for the current caller.
    /// <para>
    /// Never throws: a refusal, a transport failure, or an unconfigured endpoint
    /// all resolve to a decision, so a probe can never break the shell.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>
    /// <see cref="ExplorerPluginAccess.Unavailable"/> when the cluster serves no
    /// tenancy, <see cref="ExplorerPluginAccess.AuthenticationRequired"/> when
    /// the caller presented no accepted credential,
    /// <see cref="ExplorerPluginAccess.Allowed"/> when the surface answered, and
    /// <see cref="ExplorerPluginAccess.Denied"/> otherwise.
    /// </returns>
    ValueTask<ExplorerPluginAccess> ProbeAsync(CancellationToken cancellationToken = default);
}

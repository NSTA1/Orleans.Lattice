using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The Backups plugin's own access gate. It probes the backend backup
/// capability surface and answers the plugin-level decision the host files
/// under <see cref="BackupsPluginKeys.PluginId"/>, and additionally files the
/// per-tree decisions a scope probe produces.
/// <para>
/// The gate is owned by the plugin rather than by the shell: the host probes it
/// through the uniform, fault-isolated
/// <see cref="IExplorerPluginAccessRefresher"/> path and carries no per-plugin
/// knowledge. The result is a UX affordance only; the server remains the
/// fail-closed enforcement point, so every backup action must still handle a
/// runtime denial.
/// </para>
/// </summary>
public interface IBackupCapabilityService : IExplorerPluginAccessGate
{
    /// <summary>
    /// Probes the per-scope capabilities for <paramref name="treeId"/>, files the
    /// resulting per-tree decisions in the keyed access store, and returns the
    /// snapshot. A scope that grants list access also opens the plugin-level
    /// gate, exactly as the coarse catalog probe does. Never throws for a
    /// denial.
    /// </summary>
    /// <param name="treeId">The tree id whose scope to probe. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupScopeCapabilitySnapshot> ProbeScopeAsync(string treeId, CancellationToken cancellationToken = default);
}

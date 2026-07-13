using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// Probes the backend backup capability surface and publishes the advisory
/// <see cref="ExplorerCapabilities"/> into the shell's
/// <see cref="IExplorerCapabilityStore"/>. Probed once after sign-in / reconnect
/// (coarse), and per scope as the user opens a tree (fine). The result is a UX
/// affordance only; the server remains the fail-closed enforcement point.
/// </summary>
public interface IBackupCapabilityService
{
    /// <summary>
    /// Refreshes the coarse top-level gate for the Backups area by attempting a
    /// light catalog read, and publishes the result. A denial or an unreachable
    /// endpoint leaves the area disabled. Never throws for an auth or transport
    /// failure.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RefreshAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes the per-scope capabilities for <paramref name="treeId"/>, merges the
    /// result into the published map, and returns it. Never throws for a denial.
    /// </summary>
    /// <param name="treeId">The tree id whose scope to probe. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<BackupScopeCapabilitySnapshot> ProbeScopeAsync(string treeId, CancellationToken cancellationToken = default);
}

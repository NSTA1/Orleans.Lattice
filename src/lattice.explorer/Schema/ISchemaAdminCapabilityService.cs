using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// Probes the schema control plane and publishes the advisory
/// <see cref="ExplorerCapabilities.SchemaAllowed"/> gate into the shell's
/// <see cref="IExplorerCapabilityStore"/>, and answers a per-tree capability probe
/// the panel uses to grey out individual actions. Probed once after sign-in /
/// reconnect. The result is a UX affordance only; the server remains the fail-closed
/// enforcement point, so every schema action must still handle a runtime denial.
/// </summary>
public interface ISchemaAdminCapabilityService
{
    /// <summary>
    /// Refreshes the coarse top-level gate for the Schema area by probing whether the
    /// schema control endpoint is reachable, and publishes the result. An unreachable
    /// endpoint leaves the area disabled. Never throws for an auth or transport
    /// failure.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RefreshAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes which schema-management operations the caller may perform over
    /// <paramref name="treeId"/>. Fails closed to <see cref="SchemaCapabilitySnapshot.None"/>
    /// on a denial or transport failure; never throws.
    /// </summary>
    /// <param name="treeId">The tree to probe. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaCapabilitySnapshot> ProbeTreeAsync(string treeId, CancellationToken cancellationToken = default);
}

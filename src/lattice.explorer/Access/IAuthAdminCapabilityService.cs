using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// Probes the auth-admin control plane and publishes the advisory
/// <see cref="ExplorerCapabilities.AuthAdminAllowed"/> gate into the shell's
/// <see cref="IExplorerCapabilityStore"/>. Probed once after sign-in / reconnect.
/// The result is a UX affordance only; the server remains the fail-closed
/// enforcement point, so every admin action must still handle a runtime denial.
/// </summary>
public interface IAuthAdminCapabilityService
{
    /// <summary>
    /// Refreshes the coarse top-level gate for the Access area by attempting a
    /// light, side-effect-free administrator probe (a single-entry user list),
    /// and publishes the result. A denial or an unreachable endpoint leaves the
    /// area disabled. Never throws for an auth or transport failure.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RefreshAsync(CancellationToken cancellationToken = default);
}

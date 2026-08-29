using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The Access plugin's own access gate. It probes the auth-admin control plane
/// and answers the plugin-level decision the host files under
/// <see cref="AccessPluginKeys.PluginId"/>, distinguishing a genuine denial
/// (advisory grey-out) from an <em>unauthenticated</em> connection
/// (<see cref="ExplorerPluginAccessState.AuthenticationRequired"/>, so the
/// shell can offer a sign-in instead of an inert grey-out). It also files the
/// directory-availability sub-capability under
/// <see cref="AccessPluginKeys.DirectoryScope"/>.
/// <para>
/// The gate is owned by the plugin rather than by the shell: the host probes it
/// through the uniform, fault-isolated
/// <see cref="IExplorerPluginAccessRefresher"/> path and carries no per-plugin
/// knowledge. The result is a UX affordance only; the server remains the
/// fail-closed enforcement point, so every admin action must still handle a
/// runtime denial.
/// </para>
/// </summary>
public interface IAuthAdminCapabilityService : IExplorerPluginAccessGate
{
    /// <summary>
    /// The cluster's best-effort active authentication mode, read from the
    /// access-model probe. <see cref="ExplorerAccessAuthenticationMode.Unknown"/>
    /// until a probe has admitted the caller and read the model, and whenever
    /// the model read was denied or unreachable. Advisory only, surfaced so the
    /// Access area can render the right create-form guidance. Not an access
    /// decision, so it is the plugin's own state rather than a store entry.
    /// </summary>
    ExplorerAccessAuthenticationMode AuthenticationMode { get; }
}

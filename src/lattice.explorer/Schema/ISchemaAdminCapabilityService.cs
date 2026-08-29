using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The Schema plugin's own access gate. It probes the schema control plane and
/// answers the plugin-level decision the host files under
/// <see cref="SchemaPluginKeys.PluginId"/>, and answers the per-tree capability
/// probe the panel uses to grey out individual actions.
/// <para>
/// The gate is owned by the plugin rather than by the shell: the host probes it
/// through the uniform, fault-isolated
/// <see cref="IExplorerPluginAccessRefresher"/> path and carries no per-plugin
/// knowledge. The result is a UX affordance only; the server remains the
/// fail-closed enforcement point, so every schema action must still handle a
/// runtime denial.
/// </para>
/// </summary>
public interface ISchemaAdminCapabilityService : IExplorerPluginAccessGate
{
    /// <summary>
    /// Probes which schema-management operations the caller may perform over
    /// <paramref name="treeId"/>. Fails closed to <see cref="SchemaCapabilitySnapshot.None"/>
    /// on a denial or transport failure; never throws.
    /// </summary>
    /// <param name="treeId">The tree to probe. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<SchemaCapabilitySnapshot> ProbeTreeAsync(string treeId, CancellationToken cancellationToken = default);
}

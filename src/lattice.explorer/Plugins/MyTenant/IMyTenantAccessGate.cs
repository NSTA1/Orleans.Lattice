using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.MyTenant;

/// <summary>
/// The My Tenant plugin's own access gate: the probe that decides whether the
/// tenant self-service area exists for the current caller.
/// <para>
/// It is a plugin-owned contract rather than a bare
/// <see cref="IExplorerPluginAccessGate"/> so the plugin can register and
/// substitute its own gate without the shell knowing the difference.
/// </para>
/// </summary>
public interface IMyTenantAccessGate : IExplorerPluginAccessGate
{
}

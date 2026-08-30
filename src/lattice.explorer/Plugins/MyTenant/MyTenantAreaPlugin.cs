using Orleans.Lattice.Explorer.Plugins.MyTenant.Components;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The My Tenant area as a plugin: its descriptor, the panel the shell renders
/// for it, the controlled domain contract it operates against, and the access
/// gate this package owns. The shell learns of it only through
/// <see cref="IExplorerPlugin"/>, so registering or withholding this type is the
/// whole of the head's opt-in.
/// <para>
/// It declares <see cref="ITenancyDomain"/> through
/// <see cref="IExplorerPlugin{TDomain}"/>, so the reach of the whole surface is
/// a compile-time fact stated once in this signature (epic decision D3): the
/// panel resolves that one contract from its bound host context and receives
/// nothing else from the host - no cluster connection, no gRPC channel, no
/// tenant-administration wire type.
/// </para>
/// </summary>
/// <param name="gate">The My Tenant package's own access gate.</param>
public sealed class MyTenantAreaPlugin(IMyTenantAccessGate gate) : IExplorerPlugin<ITenancyDomain>
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = MyTenantPluginKeys.PluginId,
        Label = "My Tenant",
        Surface = ExplorerPluginSurface.Area,

        // Sorted after the operator-facing areas: this is the surface a tenant
        // admin lives in, and it is the one that disappears entirely on a
        // deployment without the tenancy add-on, so it sits at the end rather
        // than leaving a gap mid-strip when it does. 500 rather than 400
        // because the platform-operator Tenants area already claims 400, and
        // two areas sharing an order leaves their relative position to an
        // arbitrary tie-break instead of to intent.
        Order = 500,
    };

    private readonly IMyTenantAccessGate _gate = gate ?? throw new ArgumentNullException(nameof(gate));

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(MyTenantPanel);

    /// <inheritdoc />
    public IExplorerPluginAccessGate AccessGate => _gate;
}

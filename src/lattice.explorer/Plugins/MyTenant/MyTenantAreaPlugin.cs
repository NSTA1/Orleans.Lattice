using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins.MyTenant.Components;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The My tenant area as a plugin: its descriptor, the panel the shell renders
/// for it, the controlled domain contract it operates against, and the access
/// gate this package owns. The shell learns of it only through
/// <see cref="IExplorerPlugin"/>, so registering or withholding this type is the
/// whole of the head's opt-in.
/// <para>
/// It declares <see cref="IMyTenantDomain"/> through
/// <see cref="IExplorerPlugin{TDomain}"/>, so the reach of the whole surface is
/// a compile-time fact stated once in this signature (epic decision D3): the
/// panel resolves that one contract from its bound host context and receives
/// nothing else from the host - no cluster connection, no gRPC channel, no
/// tenant-administration wire type.
/// </para>
/// <para>
/// The contract is deliberately the <em>narrow</em> one. This is a tenant
/// administrator's surface, so it gets the tenant's own operations and not the
/// operator-only ones the platform-operator tenant administration plugin
/// receives through <see cref="ITenancyDomain"/> (issue #1785).
/// </para>
/// </summary>
/// <remarks>
/// The area is labelled <see cref="ExplorerVocabulary.MyTenantArea"/> - sentence
/// case, so it does not read as a proper noun beside
/// <see cref="ExplorerVocabulary.TenantAdministrationArea"/>, and so the pair
/// says which administers whose tenants at a glance.
/// </remarks>
/// <param name="gate">The My tenant package's own access gate.</param>
public sealed class MyTenantAreaPlugin(IMyTenantAccessGate gate) : IExplorerPlugin<IMyTenantDomain>
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = MyTenantPluginKeys.PluginId,
        Label = ExplorerVocabulary.MyTenantArea,
        Surface = ExplorerPluginSurface.Area,

        // Sorted after the operator-facing areas: this is the surface a tenant
        // admin lives in, and it is the one that disappears entirely on a
        // deployment without the tenancy add-on, so it sits at the end rather
        // than leaving a gap mid-strip when it does. 500 rather than 400
        // because the platform-operator tenant administration area already
        // claims 400, and two areas sharing an order leaves their relative
        // position to an arbitrary tie-break instead of to intent.
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

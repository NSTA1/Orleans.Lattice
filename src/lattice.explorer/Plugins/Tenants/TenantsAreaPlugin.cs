using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants.Views;

namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// The Tenants (platform-operator tenant management) area as a plugin: its
/// descriptor, the panel the shell renders for it, its own access gate, and the
/// single domain contract its views operate against.
/// <para>
/// The shell learns of it only through <see cref="IExplorerPlugin"/>, so
/// registering or withholding this type through
/// <see cref="ExplorerTenantsServiceCollectionExtensions.AddExplorerTenantsPlugin"/>
/// is the whole of a head's opt-in. It ships in the plugin's own package
/// alongside the services and the Razor views it renders, so the shared UI
/// library references nothing of it (epic decision D5).
/// </para>
/// </summary>
/// <remarks>
/// Declaring <see cref="IExplorerPlugin{TDomain}"/> states the plugin's reach in
/// the type system: the host resolves <see cref="ITenancyDomain"/> for it and
/// nothing else, so what the Tenants surface can touch is a compile-time fact
/// (epic decision D3). It reuses the shared tenancy seam's domain model rather
/// than declaring one of its own, because that contract is precisely "the whole
/// of what a tenancy plugin may reach" and a second copy would be a second place
/// to widen it.
/// </remarks>
/// <param name="gate">The Tenants plugin's own four-state access gate.</param>
public sealed class TenantsAreaPlugin(TenantsAccessGate gate) : IExplorerPlugin<ITenancyDomain>
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = TenantsPluginKeys.PluginId,
        Label = "Tenants",
        Surface = ExplorerPluginSurface.Area,
        // Last of the area tier: after Backups (100), Access (200), and Schema
        // (300). An operator reaches tenant administration less often than any
        // of them, and it is the widest-blast-radius surface of the four.
        Order = 400,
    };

    private readonly TenantsAccessGate _gate = gate ?? throw new ArgumentNullException(nameof(gate));

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(TenantsPanel);

    /// <inheritdoc />
    /// <remarks>
    /// The gate resolves all four states: allowed for a validated platform
    /// operator, a denial for anyone else, an
    /// <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> for a
    /// connection carrying no accepted credential - so the shell offers a sign-in
    /// rather than an inert grey-out - and
    /// <see cref="ExplorerPluginAccessState.Unavailable"/> on a cluster without
    /// the tenancy add-on, which renders no entry at all.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => _gate;
}

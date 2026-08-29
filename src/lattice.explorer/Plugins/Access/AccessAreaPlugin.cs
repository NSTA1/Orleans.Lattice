using Orleans.Lattice.Explorer.Access.Views;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The Access (membership and access-control) area as a plugin: its descriptor,
/// the panel the shell renders for it, its own access gate, and the single
/// domain contract its views operate against.
/// <para>
/// The shell learns of it only through <see cref="IExplorerPlugin"/>, so
/// registering or withholding this type through
/// <see cref="ExplorerAccessServiceCollectionExtensions.AddExplorerAccess"/> is
/// the whole of a head's opt-in. It ships in the plugin's own package alongside
/// the services and the Razor views it renders, so the shared UI library
/// references nothing of it (epic decision D5).
/// </para>
/// </summary>
/// <remarks>
/// Declaring <see cref="IExplorerPlugin{TDomain}"/> states the plugin's reach in
/// the type system: the host resolves <see cref="IAccessDomain"/> for it and
/// nothing else, so what the Access surface can touch is a compile-time fact
/// (epic decision D3).
/// </remarks>
/// <param name="gate">The Access plugin's own four-state access gate.</param>
public sealed class AccessAreaPlugin(IAuthAdminCapabilityService gate) : IExplorerPlugin<IAccessDomain>
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = AccessPluginKeys.PluginId,
        Label = "Access",
        Surface = ExplorerPluginSurface.Area,
        Order = 200,
    };

    private readonly IAuthAdminCapabilityService _gate = gate ?? throw new ArgumentNullException(nameof(gate));

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(AccessPanel);

    /// <inheritdoc />
    /// <remarks>
    /// The gate resolves all four states: allowed, a genuine denial, an
    /// <see cref="ExplorerPluginAccessState.AuthenticationRequired"/> for an
    /// unauthenticated connection - so the shell offers a sign-in rather than an
    /// inert grey-out - and it additionally files the cluster's
    /// directory-availability sub-capability under
    /// <see cref="AccessPluginKeys.DirectoryScope"/>.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => _gate;
}

using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Access;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The Access (membership and access-control) area as a plugin: its descriptor,
/// the panel the shell renders for it, and the access gate the Access feature
/// owns. The shell learns of it only through <see cref="IExplorerPlugin"/>, so
/// registering or withholding this type is the whole of the head's opt-in.
/// </summary>
/// <param name="gate">The Access feature's own access gate.</param>
public sealed class AccessAreaPlugin(IAuthAdminCapabilityService gate) : IExplorerPlugin
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
    /// The panel still resolves its own feature services directly, so it
    /// declares no controlled domain contract yet; that lands with its
    /// conversion to a standalone plugin project.
    /// </remarks>
    public Type? DomainContract => null;

    /// <inheritdoc />
    public IExplorerPluginAccessGate AccessGate => _gate;
}

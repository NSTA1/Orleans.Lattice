using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.UI.Schema;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The Schema management area as a plugin: its descriptor, the panel the shell
/// renders for it, and the access gate the Schema feature owns. The shell
/// learns of it only through <see cref="IExplorerPlugin"/>, so registering or
/// withholding this type is the whole of the head's opt-in - which is what the
/// former per-area navigation flag was emulating.
/// </summary>
/// <param name="gate">The Schema feature's own access gate.</param>
public sealed class SchemaAreaPlugin(ISchemaAdminCapabilityService gate) : IExplorerPlugin
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = SchemaPluginKeys.PluginId,
        Label = "Schema",
        Surface = ExplorerPluginSurface.Area,
        Order = 300,
    };

    private readonly ISchemaAdminCapabilityService _gate = gate ?? throw new ArgumentNullException(nameof(gate));

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(SchemaPanel);

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

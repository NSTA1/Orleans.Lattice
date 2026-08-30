using Orleans.Lattice.Explorer.Schema.Components;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The Schema management area as a plugin: its descriptor, the panel the shell
/// renders for it, the access gate the Schema feature owns, and the single
/// controlled domain contract it is allowed to reach.
/// <para>
/// The shell learns of this area only through <see cref="IExplorerPlugin"/>, so
/// registering or withholding this type <em>is</em> the whole of a head's
/// opt-in. That replaces the retired <c>EnableSchemaArea</c> flag, which existed
/// only to let a head withhold this one area and had to be special-cased by name
/// inside the shared navigation layer.
/// </para>
/// </summary>
/// <param name="gate">The Schema feature's own access gate.</param>
public sealed class SchemaAreaPlugin(ISchemaAdminCapabilityService gate) : IExplorerPlugin<ISchemaPluginDomain>
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
    public IExplorerPluginAccessGate AccessGate => _gate;
}

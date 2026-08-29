using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Detail.Tabs;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The tree-topology surface as a per-selection plugin.
/// </summary>
public sealed class TopologySelectionPlugin : IExplorerPlugin
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = SelectionPluginKeys.Topology,
        Label = "Topology",
        Surface = ExplorerPluginSurface.Selection,
        Order = 200,
        SelectionKinds = ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View,
    };

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(TopologyTab);

    /// <inheritdoc />
    /// <remarks>
    /// The view still resolves its own reader directly, so it declares no
    /// controlled domain contract yet; that lands with its conversion to a
    /// standalone plugin project.
    /// </remarks>
    public Type? DomainContract => null;

    /// <inheritdoc />
    /// <remarks>
    /// Topology surfaces no capability of its own to probe, so the gate admits
    /// whoever reaches the panel; the server remains the sole enforcement point.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}

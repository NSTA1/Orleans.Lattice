using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Detail.Tabs;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The key and value drill-down surface as a per-selection plugin. It also hosts
/// the per-key revision timeline, which is opened from a row's detail panel
/// rather than from the tab strip, so the timeline is not a plugin of its own.
/// </summary>
public sealed class DataSelectionPlugin : IExplorerPlugin
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = SelectionPluginKeys.Data,
        Label = "Data",
        Surface = ExplorerPluginSurface.Selection,
        Order = 300,
        SelectionKinds = ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View,
    };

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(DataTab);

    /// <inheritdoc />
    /// <remarks>
    /// The view still resolves its own readers directly, so it declares no
    /// controlled domain contract yet; that lands with its conversion to a
    /// standalone plugin project.
    /// </remarks>
    public Type? DomainContract => null;

    /// <inheritdoc />
    /// <remarks>
    /// The data surface exposes no capability of its own to probe, so the gate
    /// admits whoever reaches the panel; the server remains the sole enforcement
    /// point for every read it then issues.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}

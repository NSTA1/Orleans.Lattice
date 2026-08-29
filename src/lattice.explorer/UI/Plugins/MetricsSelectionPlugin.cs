using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Detail.Tabs;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The live-metrics surface as a per-selection plugin: its descriptor, the view
/// the detail panel renders for it, and its access gate. The panel learns of it
/// only through <see cref="IExplorerPlugin"/>, exactly as the shell learns of an
/// area plugin, so both navigation tiers run on one model.
/// </summary>
public sealed class MetricsSelectionPlugin : IExplorerPlugin
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = SelectionPluginKeys.Metrics,
        Label = "Metrics",
        Surface = ExplorerPluginSurface.Selection,
        Order = 100,
        SelectionKinds = ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View,
    };

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(MetricsTab);

    /// <inheritdoc />
    /// <remarks>
    /// The view still resolves its own reader directly, so it declares no
    /// controlled domain contract yet; that lands with its conversion to a
    /// standalone plugin project.
    /// </remarks>
    public Type? DomainContract => null;

    /// <inheritdoc />
    /// <remarks>
    /// Metrics surfaces no capability of its own to probe, so the gate admits
    /// whoever reaches the panel. The server remains the sole enforcement point
    /// (epic decision D6): an allowed gate is a UX affordance, never a promise
    /// that a read will succeed.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}

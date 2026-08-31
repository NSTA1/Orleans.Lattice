using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.Metrics;

/// <summary>
/// The live-metrics surface as a per-selection plugin: its descriptor, the view
/// the tier renders for it, the domain contract that is the whole of its reach,
/// and its access gate.
/// <para>
/// It declares tree and view and not tag index, so a tag-index selection
/// resolves to a different plugin set through ordinary applicability rather than
/// through a special case in the panel.
/// </para>
/// </summary>
public sealed class MetricsSelectionPlugin : IExplorerPlugin<IMetricsSurface>
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = SelectionPluginKeys.Metrics,
        Label = "Metrics",
        Surface = ExplorerPluginSurface.Selection,
        Order = 300,
        SelectionKinds = ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View,
    };

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(MetricsTab);

    /// <inheritdoc />
    /// <remarks>
    /// The metrics surface exposes no capability of its own to probe, so the gate
    /// admits whoever reaches the panel; the server remains the sole enforcement
    /// point for every read it then issues.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}

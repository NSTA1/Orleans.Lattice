using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Detail.Tabs;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The strict-mode dead-letter surface as a per-selection plugin.
/// </summary>
public sealed class DeadLetterSelectionPlugin : IExplorerPlugin
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = SelectionPluginKeys.DeadLetter,
        Label = "Dead-letter",
        Surface = ExplorerPluginSurface.Selection,
        Order = 400,
        SelectionKinds = ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View,
    };

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(DeadLetterTab);

    /// <inheritdoc />
    /// <remarks>
    /// The view still resolves its own reader directly, so it declares no
    /// controlled domain contract yet; that lands with its conversion to a
    /// standalone plugin project.
    /// </remarks>
    public Type? DomainContract => null;

    /// <inheritdoc />
    /// <remarks>
    /// The dead-letter queue is read-only and surfaces no capability of its own
    /// to probe, so the gate admits whoever reaches the panel; the server
    /// remains the sole enforcement point.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}

using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.TagIndex;

/// <summary>
/// The tag-index browsing surface as a per-selection plugin: covered trees,
/// tags, and per-tag members.
/// <para>
/// It declares <see cref="ExplorerPluginSelectionKinds.TagIndex"/> and nothing
/// else, and the generic surfaces declare tree and view and not tag index. That
/// single fact replaces the panel's former hard-coded tag-index branch: a
/// tag-index selection resolves to a different plugin set through ordinary
/// applicability, rather than bypassing the tier altogether.
/// </para>
/// </summary>
public sealed class TagIndexSelectionPlugin : IExplorerPlugin<ITagIndexSurface>
{
    private static readonly ExplorerPluginDescriptor Registration = new()
    {
        PluginId = SelectionPluginKeys.TagIndex,
        Label = "Tag index",
        Surface = ExplorerPluginSurface.Selection,
        Order = 100,
        SelectionKinds = ExplorerPluginSelectionKinds.TagIndex,
    };

    /// <inheritdoc />
    public ExplorerPluginDescriptor Descriptor => Registration;

    /// <inheritdoc />
    public Type ViewType => typeof(TagIndexDetailTab);

    /// <inheritdoc />
    /// <remarks>
    /// The tag-index browser surfaces no capability of its own to probe, so the
    /// gate admits whoever reaches the panel; the server remains the sole
    /// enforcement point.
    /// </remarks>
    public IExplorerPluginAccessGate AccessGate => ExplorerPluginAccessGates.Allowed;
}

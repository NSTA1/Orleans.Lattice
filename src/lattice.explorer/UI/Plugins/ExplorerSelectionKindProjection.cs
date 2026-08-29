using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The one place the Explorer's own <see cref="CatalogKind"/> is projected onto
/// the plugin contract's <see cref="ExplorerPluginSelectionKind"/>.
/// <para>
/// Shared by the host-state adapter (which publishes the projected selection to
/// every plugin gate) and the detail panel (which resolves the applicable plugin
/// set from it), so the two can never disagree about what kind a selection is.
/// </para>
/// </summary>
internal static class ExplorerSelectionKindProjection
{
    /// <summary>
    /// The plugin-facing kind for <paramref name="kind"/>. Anything that is not
    /// a view or a tag index is a tree, so an unrecognised catalog kind resolves
    /// to the ordinary surfaces rather than to none.
    /// </summary>
    /// <param name="kind">The catalog kind of the selected entry.</param>
    public static ExplorerPluginSelectionKind ToPluginKind(CatalogKind kind) => kind switch
    {
        CatalogKind.Views => ExplorerPluginSelectionKind.View,
        CatalogKind.TagIndexes => ExplorerPluginSelectionKind.TagIndex,
        _ => ExplorerPluginSelectionKind.Tree,
    };
}

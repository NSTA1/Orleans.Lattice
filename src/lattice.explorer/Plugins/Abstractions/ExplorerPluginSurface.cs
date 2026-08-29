namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The navigation tier a plugin occupies. The Explorer has two, and both are
/// driven by the same plugin model so a single access gate covers each: the
/// app-level switcher and the per-selection strip below it.
/// </summary>
public enum ExplorerPluginSurface
{
    /// <summary>
    /// The top-level area switcher: a plugin that owns the whole working
    /// surface (the backup catalogue, the access-control administration
    /// surface, and so on) and does not depend on a selected tree or view.
    /// </summary>
    Area = 0,

    /// <summary>
    /// The per-selection tier: a plugin rendered for the currently selected
    /// tree or view (metrics, topology, data, and so on). A selection plugin
    /// reads
    /// <see cref="IExplorerPluginHostContext.Selection"/> and is not rendered
    /// when nothing is selected.
    /// </summary>
    Selection = 1,
}

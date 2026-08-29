namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Which ambient fact changed, carried by
/// <see cref="IExplorerPluginHostContext.Changed"/> so a plugin can re-render
/// only for what it actually reads rather than on every host transition.
/// </summary>
public enum ExplorerPluginHostChange
{
    /// <summary><see cref="IExplorerPluginHostContext.Selection"/> changed.</summary>
    Selection = 0,

    /// <summary><see cref="IExplorerPluginHostContext.Connection"/> changed.</summary>
    Connection = 1,

    /// <summary><see cref="IExplorerPluginHostContext.Tenant"/> changed.</summary>
    Tenant = 2,
}

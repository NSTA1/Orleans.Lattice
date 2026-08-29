namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// What kind of catalog entry a selection refers to. A plugin uses this to
/// decide whether it applies to the current selection at all - a dead-letter
/// surface, for example, is meaningful for a tree and not for a view.
/// </summary>
public enum ExplorerPluginSelectionKind
{
    /// <summary>A registered tree.</summary>
    Tree = 0,

    /// <summary>A materialised view.</summary>
    View = 1,

    /// <summary>A tag-index membership tree.</summary>
    TagIndex = 2,
}

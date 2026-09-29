namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>What a row of the Data directory is.</summary>
internal enum DataTreeKind
{
    /// <summary>An ordinary tree, including one an app owns.</summary>
    Tree = 0,

    /// <summary>A materialised view, read through its logical view tree.</summary>
    View = 1,

    /// <summary>
    /// A tree-name prefix another tenant shares with this one. It names no tree of
    /// its own: a tree under it is opened by its full id.
    /// </summary>
    Prefix = 2,
}

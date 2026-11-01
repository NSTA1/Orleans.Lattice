namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>Which keys of a tree a data export covers.</summary>
internal enum DataExportScopeKind
{
    /// <summary>Every key in the tree.</summary>
    Tree = 0,

    /// <summary>Every key that starts with a prefix.</summary>
    Prefix = 1,

    /// <summary>Every key in a half-open range, from an inclusive lower key to an exclusive upper key.</summary>
    Range = 2,
}

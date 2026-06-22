namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// Selects which discovery call feeds the navigation list. Trees and views are
/// surfaced uniformly: the only difference is the state-API call that produces
/// the ids, which are then used as-is by the detail tabs.
/// </summary>
public enum CatalogKind
{
    /// <summary>List registered trees via <c>ListTreesAsync</c>.</summary>
    Trees,

    /// <summary>List materialised views via <c>ListViewsAsync</c>.</summary>
    Views,

    /// <summary>List tag-index membership trees via <c>ListTagIndexesAsync</c>.</summary>
    TagIndexes,
}

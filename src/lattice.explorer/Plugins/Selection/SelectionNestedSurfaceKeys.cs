namespace Orleans.Lattice.Explorer.Plugins.Selection;

/// <summary>
/// The stable ids a nested per-selection view is contributed and looked up
/// under. Two packages name each id - the one that contributes the view and the
/// one that renders it - and a literal in either would drift.
/// </summary>
public static class SelectionNestedSurfaceKeys
{
    /// <summary>
    /// The per-key revision timeline the value drill-down surface renders in its
    /// selected-row detail panel, behind that row's History button. It is not a
    /// tier tab: the operator reaches it from a row, exactly as before this
    /// surface became its own package.
    /// </summary>
    public const string EntryHistory = "orleans.lattice.history.entry";
}

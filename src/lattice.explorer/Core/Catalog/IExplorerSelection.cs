namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// Holds the currently selected tree or view and publishes selection changes so
/// the navigation panel and the detail panel stay in sync. One instance backs a
/// single user session - it is registered per Blazor circuit on the multi-user
/// web head, so a selection is never shared between signed-in operators.
/// </summary>
public interface IExplorerSelection
{
    /// <summary>The currently selected item, or <see langword="null"/> when none.</summary>
    CatalogItem? Selected { get; }

    /// <summary>Raised whenever <see cref="Selected"/> changes.</summary>
    event Action? SelectionChanged;

    /// <summary>
    /// Sets the selection. Passing the already-selected item is a no-op;
    /// passing <see langword="null"/> clears the selection.
    /// </summary>
    void Select(CatalogItem? item);
}

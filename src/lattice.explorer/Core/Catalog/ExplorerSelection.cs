namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// Default in-memory <see cref="IExplorerSelection"/>. Comparison uses record
/// value equality so re-selecting an equivalent item does not re-raise.
/// </summary>
public sealed class ExplorerSelection : IExplorerSelection
{
    /// <inheritdoc />
    public CatalogItem? Selected { get; private set; }

    /// <inheritdoc />
    public event Action? SelectionChanged;

    /// <inheritdoc />
    public void Select(CatalogItem? item)
    {
        if (Equals(Selected, item))
        {
            return;
        }

        Selected = item;
        SelectionChanged?.Invoke();
    }
}

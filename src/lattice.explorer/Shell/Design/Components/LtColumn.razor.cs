using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// One column of an <see cref="LtTable{TItem}"/>. It renders nothing itself: it
/// registers with its table, which draws its header and its cells.
/// </summary>
/// <typeparam name="TItem">The type of one row.</typeparam>
public partial class LtColumn<TItem> : IDisposable
{
    /// <summary>The column header's text.</summary>
    [Parameter, EditorRequired]
    public string Title { get; set; } = string.Empty;

    /// <summary>The cell for a row. When absent, the cell shows <see cref="Value"/> as text.</summary>
    [Parameter]
    public RenderFragment<TItem>? ChildContent { get; set; }

    /// <summary>The cell's text for a row, used when there is no <see cref="ChildContent"/>.</summary>
    [Parameter]
    public Func<TItem, object?>? Value { get; set; }

    /// <summary>
    /// The value a row sorts by. When set, the header is a sort button; leave it
    /// <see langword="null"/> for a column that cannot be sorted.
    /// </summary>
    [Parameter]
    public Func<TItem, IComparable?>? SortBy { get; set; }

    /// <summary>Whether the column holds data - ids, keys, digests - set in Cascadia Mono.</summary>
    [Parameter]
    public bool Mono { get; set; }

    /// <summary>How the column's cells align. Numbers align to the end.</summary>
    [Parameter]
    public LtColumnAlign Align { get; set; } = LtColumnAlign.Start;

    /// <summary>Whether this column names its row, and is rendered as a row header (<c>th scope="row"</c>).</summary>
    [Parameter]
    public bool RowHeader { get; set; }

    [CascadingParameter]
    private LtTable<TItem>? Table { get; set; }

    /// <summary>Whether the column can be sorted.</summary>
    internal bool IsSortable => SortBy is not null;

    /// <summary>The cell content for <paramref name="item"/>.</summary>
    /// <param name="item">The row.</param>
    internal RenderFragment RenderCell(TItem item) =>
        ChildContent?.Invoke(item) ?? (builder => builder.AddContent(0, Value?.Invoke(item)?.ToString()));

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        if (Table is null)
        {
            throw new InvalidOperationException($"An {nameof(LtColumn<TItem>)} must be declared inside an {nameof(LtTable<TItem>)}.");
        }

        Table.AddColumn(this);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Table?.RemoveColumn(this);
        GC.SuppressFinalize(this);
    }
}

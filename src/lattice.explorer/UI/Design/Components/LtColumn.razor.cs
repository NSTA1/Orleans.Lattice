using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

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
    /// <remarks>
    /// A mono cell never breaks inside an id: a value too long for its column is cut
    /// short with an ellipsis, and its full text is the cell's tooltip (see
    /// <see cref="FullText"/>).
    /// </remarks>
    [Parameter]
    public bool Mono { get; set; }

    /// <summary>
    /// The full text of a <see cref="Mono"/> cell, shown as its tooltip so an id cut
    /// short is still readable. Defaults to the text of <see cref="Value"/>, and for
    /// a cell drawn by <see cref="ChildContent"/> to the <see cref="SortBy"/> key when
    /// that is a string, as it is for an id column; set it when neither is the text.
    /// </summary>
    [Parameter]
    public Func<TItem, string?>? FullText { get; set; }

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

    /// <summary>The tooltip of a mono cell for <paramref name="item"/>: its full text, or <see langword="null"/>.</summary>
    /// <param name="item">The row.</param>
    internal string? TooltipOf(TItem item)
    {
        // A figure (end-aligned) is never long enough to be cut, so it needs no tooltip.
        if (!Mono || Align == LtColumnAlign.End)
        {
            return null;
        }

        if (FullText is { } fullText)
        {
            return fullText(item);
        }

        if (Value is { } value)
        {
            return value(item)?.ToString();
        }

        return ChildContent is not null && SortBy?.Invoke(item) is string key ? key : null;
    }

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

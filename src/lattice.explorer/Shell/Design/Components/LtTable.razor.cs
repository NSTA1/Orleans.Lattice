using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// A booktabs table: a heavy rule above and below, a lighter rule under the
/// header, hairlines between rows, and no vertical rules, stripes or cell boxes.
/// Columns are declared as <see cref="LtColumn{TItem}"/> children; a column with
/// a sort key gets a sortable header, and long lists can be virtualised.
/// </summary>
/// <remarks>
/// <para>
/// A sortable header is a button inside the <c>th</c>, so it is reached with Tab
/// and pressed with Enter or Space; the sorted column carries <c>aria-sort</c>.
/// Pressing a header sorts ascending, pressing it again sorts descending. Sorting
/// is stable, and a missing value sorts first.
/// </para>
/// <para>
/// The frame around the table is a labelled, focusable region, so a table wider
/// than its column scrolls inside its own frame and can be scrolled from the
/// keyboard. The current row - "you are here" - carries <c>aria-current</c>, the
/// soft marker band, a marker bar and a heavier weight.
/// </para>
/// </remarks>
/// <typeparam name="TItem">The type of one row.</typeparam>
public partial class LtTable<TItem>
{
    private readonly string _id = LtIds.Next("lt-table");
    private readonly List<LtColumn<TItem>> _columns = [];
    private IReadOnlyList<TItem>? _source;
    private List<TItem> _rows = [];
    private LtColumn<TItem>? _sortColumn;
    private LtSortDirection _sortDirection;

    /// <summary>The rows, in their natural order.</summary>
    [Parameter, EditorRequired]
    public IReadOnlyList<TItem> Items { get; set; } = [];

    /// <summary>The table's caption, which also names its scrolling region.</summary>
    [Parameter, EditorRequired]
    public string Caption { get; set; } = string.Empty;

    /// <summary>Whether the caption is visually hidden because a heading above already says it.</summary>
    [Parameter]
    public bool CaptionHidden { get; set; }

    /// <summary>The columns: <see cref="LtColumn{TItem}"/> components, in display order.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>What the body shows when <see cref="Items"/> is empty. Defaults to "No rows.".</summary>
    [Parameter]
    public RenderFragment? EmptyContent { get; set; }

    /// <summary>
    /// Whether only the rows in view are rendered. Use it for lists that can run
    /// to thousands of rows; every row must then be <see cref="RowHeight"/> tall.
    /// </summary>
    [Parameter]
    public bool Virtualize { get; set; }

    /// <summary>The height of one row in CSS pixels, used by virtualisation. Defaults to 36.</summary>
    [Parameter]
    public float RowHeight { get; set; } = 36f;

    /// <summary>Identifies a row across renders, so sorting moves rows rather than rewriting them.</summary>
    [Parameter]
    public Func<TItem, object>? RowKey { get; set; }

    /// <summary>Whether a row is the current one ("you are here"), such as the object the page is about.</summary>
    [Parameter]
    public Func<TItem, bool>? IsCurrent { get; set; }

    private string CaptionId => _id + "-caption";

    /// <summary>The rows in their displayed order, after sorting.</summary>
    internal IReadOnlyList<TItem> DisplayedRows => _rows;

    internal void AddColumn(LtColumn<TItem> column)
    {
        _columns.Add(column);
        StateHasChanged();
    }

    internal void RemoveColumn(LtColumn<TItem> column)
    {
        if (_columns.Remove(column))
        {
            if (ReferenceEquals(_sortColumn, column))
            {
                _sortColumn = null;
                ApplySort();
            }

            StateHasChanged();
        }
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (!ReferenceEquals(_source, Items))
        {
            _source = Items;
            ApplySort();
        }
    }

    private static string HeaderClass(LtColumn<TItem> column) =>
        column.Align == LtColumnAlign.End ? "lt-table__header lt-table__cell--end" : "lt-table__header";

    private static string CellClass(LtColumn<TItem> column) => (column.Align, column.Mono) switch
    {
        (LtColumnAlign.End, true) => "lt-table__cell lt-table__cell--end lt-table__cell--mono",
        (LtColumnAlign.End, false) => "lt-table__cell lt-table__cell--end",
        (_, true) => "lt-table__cell lt-table__cell--mono",
        _ => "lt-table__cell",
    };

    private string? AriaSort(LtColumn<TItem> column) =>
        ReferenceEquals(column, _sortColumn)
            ? _sortDirection == LtSortDirection.Ascending ? "ascending" : "descending"
            : null;

    private string SortState(LtColumn<TItem> column) =>
        ReferenceEquals(column, _sortColumn)
            ? _sortDirection == LtSortDirection.Ascending ? "ascending" : "descending"
            : "none";

    private object? RowKeyOf(TItem item) => RowKey is null ? item : RowKey(item);

    private Task SortByAsync(LtColumn<TItem> column)
    {
        if (ReferenceEquals(column, _sortColumn))
        {
            _sortDirection = _sortDirection == LtSortDirection.Ascending
                ? LtSortDirection.Descending
                : LtSortDirection.Ascending;
        }
        else
        {
            _sortColumn = column;
            _sortDirection = LtSortDirection.Ascending;
        }

        ApplySort();
        return Task.CompletedTask;
    }

    private void ApplySort()
    {
        var source = Items ?? [];
        if (_sortColumn?.SortBy is not { } key)
        {
            _rows = [.. source];
            return;
        }

        var ordered = _sortDirection == LtSortDirection.Ascending
            ? source.OrderBy(key, NullFirstComparer.Instance)
            : source.OrderByDescending(key, NullFirstComparer.Instance);
        _rows = [.. ordered];
    }

    /// <summary>Compares sort keys with a missing key before any present one.</summary>
    private sealed class NullFirstComparer : IComparer<IComparable?>
    {
        public static readonly NullFirstComparer Instance = new();

        public int Compare(IComparable? x, IComparable? y) => (x, y) switch
        {
            (null, null) => 0,
            (null, _) => -1,
            (_, null) => 1,
            _ => x.CompareTo(y),
        };
    }
}

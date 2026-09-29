using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

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
/// <para>
/// Below the small breakpoint (the width band the Shell's layout cascades), the
/// table becomes a list of two-line rows unless <see cref="Compact"/> is
/// <see cref="LtTableCompact.ScrollFrame"/>. Line one is the row's identifier and
/// line two a summary led by its state: supply them through
/// <see cref="CompactRow"/> with an <see cref="LtCompactRow"/>, or accept the
/// default, which takes the row-header column (or the first) as line one and the
/// next three columns as line two. Each row is a button that opens a detail sheet
/// - a full-height dialog that traps focus and returns it to the row - holding
/// <see cref="Detail"/> (by default every column as a definition list) and
/// <see cref="DetailActions"/>. Sorting is offered as a "Sort by" select, since a
/// list has no headers.
/// </para>
/// </remarks>
/// <typeparam name="TItem">The type of one row.</typeparam>
public partial class LtTable<TItem>
{
    private readonly string _id = LtIds.Next("lt-table");
    private static readonly object NullRowKey = new();

    private readonly List<LtColumn<TItem>> _columns = [];
    private readonly Dictionary<object, ElementReference> _rowButtons = [];
    private TItem? _detailItem;
    private bool _hasDetail;
    private bool _detailOpen;
    private ElementReference? _returnFocus;
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

    /// <summary>The height of one row in CSS pixels, used by virtualisation. Defaults to 44, the comfortable row height.</summary>
    [Parameter]
    public float RowHeight { get; set; } = 44f;

    /// <summary>Identifies a row across renders, so sorting moves rows rather than rewriting them.</summary>
    [Parameter]
    public Func<TItem, object>? RowKey { get; set; }

    /// <summary>Whether a row is the current one ("you are here"), such as the object the page is about.</summary>
    [Parameter]
    public Func<TItem, bool>? IsCurrent { get; set; }

    /// <summary>
    /// How the table lays out below the small breakpoint. Defaults to
    /// <see cref="LtTableCompact.List"/>; use <see cref="LtTableCompact.ScrollFrame"/>
    /// only for matrix-shaped data.
    /// </summary>
    [Parameter]
    public LtTableCompact Compact { get; set; } = LtTableCompact.List;

    /// <summary>
    /// A row's two lines below the small breakpoint, usually an
    /// <see cref="LtCompactRow"/>. When absent, the row-header column (or the
    /// first) is line one and the next three columns are line two.
    /// </summary>
    [Parameter]
    public RenderFragment<TItem>? CompactRow { get; set; }

    /// <summary>The height of one compact row in CSS pixels, used by virtualisation. Defaults to 64.</summary>
    [Parameter]
    public float CompactRowHeight { get; set; } = 64f;

    /// <summary>
    /// The body of a row's detail sheet below the small breakpoint. When absent,
    /// every column is shown as a definition list.
    /// </summary>
    [Parameter]
    public RenderFragment<TItem>? Detail { get; set; }

    /// <summary>A row's actions, placed at the foot of its detail sheet.</summary>
    [Parameter]
    public RenderFragment<TItem>? DetailActions { get; set; }

    /// <summary>
    /// The title of a row's detail sheet. When absent, the text of the row-header
    /// column (or the first) is used, and failing that the caption.
    /// </summary>
    [Parameter]
    public Func<TItem, string>? DetailTitle { get; set; }

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private string CaptionId => _id + "-caption";

    private bool IsCompactList => Breakpoint == LtBreakpoint.Compact && Compact == LtTableCompact.List;

    private LtColumn<TItem>? PrimaryColumn => _columns.FirstOrDefault(column => column.RowHeader) ?? _columns.FirstOrDefault();

    private IEnumerable<LtColumn<TItem>> SecondaryColumns
    {
        get
        {
            var primary = PrimaryColumn;
            return _columns.Where(column => !ReferenceEquals(column, primary)).Take(3);
        }
    }

    private string DetailTitleText =>
        !_hasDetail
            ? Caption
            : DetailTitle?.Invoke(_detailItem!)
                ?? PrimaryColumn?.Value?.Invoke(_detailItem!)?.ToString()
                ?? Caption;

    private IReadOnlyList<LtSelectOption> SortOptions
    {
        get
        {
            var options = new List<LtSelectOption> { new(string.Empty, "Natural order") };
            for (var i = 0; i < _columns.Count; i++)
            {
                if (_columns[i].IsSortable)
                {
                    options.Add(new LtSelectOption(SortValueOf(i, LtSortDirection.Ascending), _columns[i].Title + ", ascending"));
                    options.Add(new LtSelectOption(SortValueOf(i, LtSortDirection.Descending), _columns[i].Title + ", descending"));
                }
            }

            return options;
        }
    }

    private string SortValue =>
        _sortColumn is null ? string.Empty : SortValueOf(_columns.IndexOf(_sortColumn), _sortDirection);

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
            _rowButtons.Clear();
            ApplySort();
        }

        if (!IsCompactList)
        {
            _detailOpen = false;
        }
    }

    private static string SortValueOf(int index, LtSortDirection direction) =>
        index.ToString(System.Globalization.CultureInfo.InvariantCulture)
        + (direction == LtSortDirection.Ascending ? ":ascending" : ":descending");

    private void OpenDetail(TItem item, object key)
    {
        _detailItem = item;
        _hasDetail = true;
        _detailOpen = true;
        _returnFocus = _rowButtons.TryGetValue(key, out var button) ? button : null;
    }

    private void OnDetailOpenChanged(bool open) => _detailOpen = open;

    private Task SortFromValueAsync(string value)
    {
        var separator = value.IndexOf(':', StringComparison.Ordinal);
        if (separator < 0
            || !int.TryParse(value.AsSpan(0, separator), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var index)
            || index < 0
            || index >= _columns.Count
            || !_columns[index].IsSortable)
        {
            _sortColumn = null;
        }
        else
        {
            _sortColumn = _columns[index];
            _sortDirection = value.EndsWith(":descending", StringComparison.Ordinal) ? LtSortDirection.Descending : LtSortDirection.Ascending;
        }

        ApplySort();
        return Task.CompletedTask;
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

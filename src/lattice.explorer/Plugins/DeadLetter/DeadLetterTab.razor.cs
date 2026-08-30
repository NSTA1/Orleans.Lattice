using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Plugins.DeadLetter;

/// <summary>
/// The dead-letter surface's state and paging path. The markup lives beside this
/// file in <c>DeadLetterTab.razor</c>; the two are one partial class.
/// </summary>
public partial class DeadLetterTab
{
    /// <summary>
    /// The list page size. Fixed rather than operator-controlled, to keep this
    /// read-only inspection surface simple; the surface pages with a "Load more"
    /// cursor.
    /// </summary>
    private const int PageSize = 100;

    // A reference key per row so the adaptive table's @key never boxes, and the
    // column set is composed once per view rather than once per render: the
    // selection cell captures `this`, so it cannot be static, but it is still
    // built exactly once in OnInitialized.
    private static readonly Func<DeadLetterEntry, object> RowKeySelector =
        static entry => entry.Key;

    private readonly Func<DeadLetterEntry, object> _rowKey = RowKeySelector;
    private readonly List<DeadLetterEntry> _entries = [];

    private LatticeTableColumn<DeadLetterEntry>[] _columns = [];

    private DeadLetterEntry? _selected;
    private RenderedValue? _rendered;
    private string? _continuationToken;
    private bool _hasMore;
    private bool _loading;
    private bool _loaded;
    private int _count;
    private string? _error;

    /// <summary>
    /// The ambient shell context, so the two-pane body stacks at compact width
    /// by name rather than by measuring a viewport.
    /// </summary>
    [CascadingParameter]
    public LatticeAdaptiveContext? AdaptiveContext { get; set; }

    private bool IsCompact =>
        (AdaptiveContext?.Breakpoint ?? LatticeBreakpoints.Default) == LatticeBreakpoint.Compact;

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _columns = BuildColumns();
        await ReloadAsync();
    }

    private LatticeTableColumn<DeadLetterEntry>[] BuildColumns() =>
    [
        new()
        {
            Header = "Key",
            IsPrimary = true,
            IsNumericOrCode = true,
            Cell = entry => builder => BuildKeyCell(builder, entry),
        },
        new()
        {
            Header = "Timestamp (UTC)",
            IsNumericOrCode = true,
            Cell = static entry => builder =>
                builder.AddContent(0, entry.TimestampUtc.UtcDateTime.ToString("u")),
        },
        new()
        {
            Header = "Source",
            Cell = static entry => builder => builder.AddContent(0, SourceLabel(entry.Source)),
        },
        new()
        {
            Header = "Reason",
            Cell = static entry => builder => builder.AddContent(0, entry.Reason),
        },
        new()
        {
            Header = "Size",
            IsNumericOrCode = true,
            ShowOnCompact = false,
            Cell = static entry => builder => builder.AddContent(0, $"{entry.ValueByteLength} B"),
        },
    ];

    // A button rather than a row click handler, so the same cell selects in both
    // the table and the card presentation and is reachable from the keyboard.
    private void BuildKeyCell(RenderTreeBuilder builder, DeadLetterEntry entry)
    {
        builder.OpenElement(0, "button");
        builder.AddAttribute(1, "type", "button");
        builder.AddAttribute(
            2,
            "class",
            ReferenceEquals(entry, _selected)
                ? "lx-deadletter-key is-selected"
                : "lx-deadletter-key");
        builder.AddAttribute(3, "title", entry.Key);
        builder.AddAttribute(4, "onclick", EventCallback.Factory.Create(this, () => Select(entry)));
        builder.AddContent(5, entry.Key);
        builder.CloseElement();
    }

    private async Task ReloadAsync()
    {
        _entries.Clear();
        _selected = null;
        _rendered = null;
        _continuationToken = null;
        _hasMore = false;
        _loaded = false;
        await LoadPageAsync(reloadCount: true);
    }

    private Task LoadMoreAsync() => LoadPageAsync(reloadCount: false);

    private async Task LoadPageAsync(bool reloadCount)
    {
        if (_loading)
        {
            return;
        }

        _loading = true;
        _error = null;
        StateHasChanged();

        try
        {
            if (reloadCount)
            {
                _count = await Surface.CountAsync(Selection.Id, TabToken);
            }

            var page = await Surface.ListAsync(Selection.Id, PageSize, _continuationToken, TabToken);
            _entries.AddRange(page.Entries);
            _continuationToken = page.ContinuationToken;
            _hasMore = page.HasMore;
            _loaded = true;
        }
        catch (OperationCanceledException)
        {
            // The view was disposed (selection change or tab switch); drop quietly.
        }
        catch (Exception ex)
        {
            _error = $"Failed to load the dead-letter queue: {ex.Message}";
        }
        finally
        {
            _loading = false;
            StateHasChanged();
        }
    }

    private void Select(DeadLetterEntry entry)
    {
        _selected = entry;

        // A pure projection over the bytes already in hand: it reaches nothing,
        // so it needs no place on the domain contract.
        _rendered = ValueRenderer.Render(entry.Value, entry.Truncated);
    }

    private static string SourceLabel(DeadLetterSource source) => source switch
    {
        DeadLetterSource.Replication => "Replication",
        DeadLetterSource.Restore => "Restore",
        DeadLetterSource.LocalRejected => "Local (rejected)",
        _ => "Unknown",
    };
}

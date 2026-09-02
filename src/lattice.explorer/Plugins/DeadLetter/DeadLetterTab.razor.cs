using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Vocabulary;
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

    /// <summary>
    /// The prompt shown in the detail pane while nothing is selected. Built once
    /// and shared: it is the same sentence on every render of every instance.
    /// </summary>
    private static readonly ExplorerStateMessage NoEntrySelected =
        ExplorerStateCopy.Empty(ExplorerSubjects.DeadLetters) with
        {
            Headline = "Nothing selected",
            Explanation = "Choose a dead-lettered item from the list to see the key it was "
                + "written under, why it was rejected, and the value bytes that were held.",
            Remedy = null,
        };

    private readonly Func<DeadLetterEntry, object> _rowKey = RowKeySelector;
    private readonly List<DeadLetterEntry> _entries = [];

    // One slot: the queue-depth badge is the only one this surface renders, and
    // it is refilled on the load path rather than derived per render.
    private readonly ExplorerBadge[] _countBadges = new ExplorerBadge[1];

    private LatticeTableColumn<DeadLetterEntry>[] _columns = [];

    private DeadLetterEntry? _selected;
    private RenderedValue? _rendered;
    private string? _continuationToken;
    private bool _hasMore;
    private bool _loading;
    private bool _loaded;
    private int _count;
    private int _countBadgeCount;
    private string? _error;
    private ExplorerStateMessage? _failure;

    private EventCallback _reload;

    /// <summary>
    /// The ambient shell context, so the two-pane body stacks at compact width
    /// by name rather than by measuring a viewport.
    /// </summary>
    [CascadingParameter]
    public LatticeAdaptiveContext? AdaptiveContext { get; set; }

    private bool IsCompact =>
        (AdaptiveContext?.Breakpoint ?? LatticeBreakpoints.Default) == LatticeBreakpoint.Compact;

    /// <summary>
    /// The state the list is in, or <see langword="null"/> when it has rows to
    /// show. A failed read is reported as a failure with the cluster's own words
    /// and a retry, never as an empty queue - the two mean opposite things to an
    /// operator looking for rejected writes.
    /// </summary>
    /// <remarks>
    /// Read on the render path, and twice per pass, so it only ever selects an
    /// already-built message. The failure copy quotes the cluster's own words
    /// and therefore has to be composed; it is composed once, where the failure
    /// is caught.
    /// </remarks>
    private ExplorerStateMessage? ListState
    {
        get
        {
            if (_failure is not null)
            {
                return _failure;
            }

            return _loading && _entries.Count == 0
                ? ExplorerStateCopy.Loading(ExplorerSubjects.DeadLetters)
                : null;
        }
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _columns = BuildColumns();
        _reload = EventCallback.Factory.Create(this, ReloadAsync);
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
    // The key is the button's own text, so it needs no title attribute repeating
    // it - and the whole key is shown in the detail pane once a row is chosen.
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
        builder.AddAttribute(3, "onclick", EventCallback.Factory.Create(this, () => Select(entry)));
        builder.AddContent(4, entry.Key);
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
        _failure = null;
        StateHasChanged();

        try
        {
            if (reloadCount)
            {
                _count = await Surface.CountAsync(Selection.Id, TabToken);
                RefreshCountBadge();
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

            // Composed here, not in the ListState property: the failure copy
            // quotes the cluster's words and so has to be built, and ListState
            // is read on every render pass.
            _failure = ExplorerStateCopy.Failed(ExplorerSubjects.DeadLetters, _error);
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

    /// <summary>
    /// Refills the queue-depth badge from the count just read. Called on the
    /// load path, never from a render, so the render path only ever reads the
    /// struct already in the buffer.
    /// </summary>
    /// <remarks>
    /// A count of zero contributes no badge; the toolbar says "Queue empty" in
    /// words instead, which is the honest reading of an empty queue and needs no
    /// number.
    /// </remarks>
    private void RefreshCountBadge()
    {
        if (_count <= 0)
        {
            _countBadgeCount = 0;
            return;
        }

        _countBadges[0] = ExplorerBadges.DeadLetterCount(_count);
        _countBadgeCount = 1;
    }

    private static string SourceLabel(DeadLetterSource source) => source switch
    {
        DeadLetterSource.Replication => "Replication",
        DeadLetterSource.Restore => "Restore",
        DeadLetterSource.LocalRejected => "Local (rejected)",
        _ => "Unknown",
    };
}

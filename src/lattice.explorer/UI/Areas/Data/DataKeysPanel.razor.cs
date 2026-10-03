using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Keys tab: a paged, prefix- or tag-filtered browse of a tree's keys through
/// the Core data reader, with live updates from the state API's change feed when
/// the cluster offers it.
/// </summary>
public partial class DataKeysPanel : IDisposable
{
    /// <summary>How many characters of a key a table cell shows before clipping.</summary>
    internal const int KeyClip = 96;

    private static readonly IReadOnlyList<LtSelectOption> ModeOptions =
    [
        new(nameof(EntryScanMode.Live), "Live"),
        new(nameof(EntryScanMode.Snapshot), "Snapshot"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> PageSizeOptions =
        [.. DataPaging.PageSizes.Select(size => new LtSelectOption(size.ToString(CultureInfo.InvariantCulture), size.ToString(CultureInfo.InvariantCulture)))];

    private readonly ComponentLifetime _lifetime = new();
    private IDataReader? _reader;
    private ILatticeStateClient? _client;
    private DataPager? _pager;
    private (string StateId, string? Prefix, string? Index, string? Tag)? _query;
    private string? _prefixInput;
    private string? _index;
    private string? _tag;
    private IReadOnlyList<TagIndexRef> _indexes = [];
    private IReadOnlyList<string> _tags = [];
    private EntryScanMode _mode = EntryScanMode.Live;
    private int _pageSize = DataPaging.DefaultPageSize;
    private bool _loading;
    private bool _refreshing;
    private bool _pendingRefresh;
    private string? _error;
    private bool _cursorExpired;
    private bool _stale;
    private bool _followAvailable;
    private bool _live = true;
    private string? _liveNote;
    private bool _liveRestartable;
    private readonly ComponentLifetime _follows = new();
    private int _entryVersion;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    /// <summary>
    /// Whether the open entry is shown beside the table rather than below it: at
    /// the expanded width (or outside the layout, which reads as expanded) with a
    /// key open. Narrower, the two stack so neither is squeezed.
    /// </summary>
    internal bool IsSplit => Workspace?.Key is not null && (Breakpoint ?? LtBreakpoint.Expanded) == LtBreakpoint.Expanded;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private TagFilter? ActiveTagFilter => _index is not null && _tag is not null ? new TagFilter(_index, _tag) : null;

    private IReadOnlyList<LtSelectOption> IndexOptions { get; set; } = [];

    private IReadOnlyList<LtSelectOption> TagOptions { get; set; } = [];

    private string TableCaption => Workspace?.Prefix is { Length: > 0 } prefix
        ? $"Keys of {Workspace.Tree.DisplayName} starting with {prefix}"
        : $"Keys of {Workspace?.Tree.DisplayName}";

    private string EmptyText => ActiveTagFilter is not null
        ? "No key carries this tag."
        : Workspace?.Prefix is { Length: > 0 } ? "No key starts with this prefix." : "This tree holds no keys yet.";

    private string RangeText
    {
        get
        {
            if (_pager is null || _loading)
            {
                return string.Empty;
            }

            var count = _pager.Current.Entries.Count;
            if (count == 0)
            {
                return "No keys";
            }

            var first = (_pager.PageIndex * _pageSize) + 1;
            var range = $"{DataFormat.Count(first)}-{DataFormat.Count(first + count - 1)}";
            return ActiveTagFilter is not null
                ? $"{range} tagged {_tag}"
                : Workspace?.Prefix is { Length: > 0 } ? $"{range} under prefix" : range;
        }
    }

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _reader = DataServices.Find<IDataReader>(Services);
        _client = ResolveClient();
        _followAvailable = _client is not null;
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var index = workspace.Address.GetQuery(DataTabs.IndexQuery);
        var tag = workspace.Address.GetQuery(DataTabs.TagQuery);
        var query = (workspace.Tree.StateId, workspace.Prefix, index, tag);
        if (_query == query)
        {
            return;
        }

        var treeChanged = _query?.StateId != workspace.Tree.StateId;
        var indexChanged = _query?.Index != index || treeChanged;
        _query = query;
        _prefixInput = workspace.Prefix;
        _index = index;
        _tag = tag;

        if (treeChanged)
        {
            await LoadIndexesAsync(workspace.Tree.StateId);
        }

        if (indexChanged)
        {
            await LoadTagsAsync(workspace.Tree.StateId);
        }

        if (_lifetime.IsLeft)
        {
            // Left while the filters were read: nothing is paged or followed for a page that is gone.
            return;
        }

        await ResetAsync();
        RestartFollow();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _follows.Leave();
        if (_pager is { } pager)
        {
            _ = CloseQuietlyAsync(pager);
        }

        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    internal static string Clip(string key) =>
        key.Length <= KeyClip ? key : string.Concat(LtTextCut.Prefix(key, KeyClip - 3), "...");

    internal static string TypeText(DataEntry entry) =>
        entry.IsTombstone ? "Deleted" : entry.CrdtShape ?? "LWW";

    private static string Inline(DataEntry entry) =>
        entry.IsTombstone ? "(deleted)" : DataValueRendering.Inline(entry.Value, entry.Truncated);

    private static async Task CloseQuietlyAsync(DataPager pager)
    {
        try
        {
            await pager.CloseAsync();
        }
        catch (Exception)
        {
            // Releasing a cursor is best effort; the server reaps an idle one.
        }
    }

    private ILatticeStateClient? ResolveClient()
    {
        try
        {
            return DataServices.Find<ILatticeStateClient>(Services);
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    private async Task LoadIndexesAsync(string stateId)
    {
        _indexes = [];
        if (_reader is null)
        {
            return;
        }

        try
        {
            _indexes = await _reader.ListTagIndexesForTreeAsync(stateId, _lifetime.Token);
            IndexOptions = [new(string.Empty, "No tag filter"), .. _indexes.Select(index => new LtSelectOption(index.IndexName, index.IndexName))];
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // The tag filter is optional; without the index list it is simply absent.
        }
    }

    private async Task LoadTagsAsync(string stateId)
    {
        _tags = [];
        TagOptions = [];
        if (_reader is null || _index is null)
        {
            return;
        }

        try
        {
            _tags = await _reader.ListTagValuesForIndexAsync(stateId, _index, _lifetime.Token);
            TagOptions = [new(string.Empty, "Choose a tag"), .. _tags.Select(tag => new LtSelectOption(tag, tag))];
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _tags = [];
        }
    }

    private async Task ResetAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        if (_reader is null)
        {
            _error = "This Explorer has no state API to read keys through.";
            return;
        }

        _pager ??= new DataPager(_reader);
        _loading = true;
        _error = null;
        _cursorExpired = false;
        _stale = false;
        try
        {
            await _pager.ResetAsync(workspace.Tree.StateId, _pageSize, ActiveTagFilter, ActiveTagFilter is null ? workspace.Prefix : null, _mode, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            _error = DataErrors.Describe(exception, "read this tree's keys");
        }
        finally
        {
            _loading = false;
        }
    }

    private async Task NextPageAsync()
    {
        if (_pager is null)
        {
            return;
        }

        _loading = true;
        _stale = false;
        try
        {
            await _pager.NextAsync(_lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception) when (DataErrors.IsCursorExpired(exception, resuming: true))
        {
            _cursorExpired = true;
        }
        catch (Exception exception)
        {
            _error = DataErrors.Describe(exception, "read the next page of keys");
        }
        finally
        {
            _loading = false;
        }
    }

    private void PreviousPage()
    {
        _stale = false;
        _pager?.Previous();
    }

    private void ApplyPrefix(string prefix)
    {
        if (Workspace is { } workspace)
        {
            workspace.NavigateTo(workspace.With(ExplorerAddress.PrefixQuery, prefix).WithQuery(ExplorerAddress.KeyQuery, null));
        }
    }

    private void SetIndex(string value)
    {
        if (Workspace is { } workspace)
        {
            workspace.NavigateTo(workspace.With(DataTabs.IndexQuery, value).WithQuery(DataTabs.TagQuery, null).WithQuery(ExplorerAddress.KeyQuery, null));
        }
    }

    private void SetTag(string value)
    {
        if (Workspace is { } workspace)
        {
            workspace.NavigateTo(workspace.With(DataTabs.TagQuery, value).WithQuery(ExplorerAddress.KeyQuery, null));
        }
    }

    private async Task SetModeAsync(string value)
    {
        if (Enum.TryParse<EntryScanMode>(value, out var mode) && mode != _mode)
        {
            _mode = mode;
            await ResetAsync();
            RestartFollow();
        }
    }

    private async Task SetPageSizeAsync(string value)
    {
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var size))
        {
            _pageSize = DataPaging.Normalize(size);
            await ResetAsync();
        }
    }

    private void SetLive(bool live)
    {
        _live = live;
        _liveNote = null;
        _liveRestartable = false;
        RestartFollow();
    }

    private void RestartLive()
    {
        _live = true;
        _liveNote = null;
        _liveRestartable = false;
        RestartFollow();
    }

    private void StopFollow()
    {
        _follows.Renew();
    }

    private void RestartFollow()
    {
        StopFollow();
        if (_lifetime.IsLeft || !_followAvailable || !_live || _mode != EntryScanMode.Live || _client is null || Workspace is not { } workspace)
        {
            return;
        }

        var prefix = ActiveTagFilter is null ? workspace.Prefix : null;
        var request = new StateObserveRequest
        {
            TreeId = workspace.Tree.StateId,
            StartInclusive = string.IsNullOrEmpty(prefix) ? null : prefix,
            EndExclusive = string.IsNullOrEmpty(prefix) ? null : DataTreeNames.PrefixUpperBound(prefix),
            IncludeMaintenance = false,
        };
        _ = FollowAsync(_client, request, _follows.Token);
    }

    private async Task FollowAsync(ILatticeStateClient client, StateObserveRequest request, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var change in client.ObserveChangesAsync(request, cancellationToken))
            {
                await InvokeAsync(() => OnChangeAsync(change));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await InvokeAsync(() =>
            {
                if (DataErrors.IsNotOffered(exception))
                {
                    _followAvailable = false;
                    _live = false;
                    _liveNote = "Live updates are not available on this cluster.";
                }
                else if (DataErrors.IsCursorExpired(exception, resuming: false))
                {
                    _liveNote = "Live updates stopped because the change feed moved past this position.";
                    _liveRestartable = true;
                }
                else
                {
                    _liveNote = DataErrors.Describe(exception, "follow changes to this tree");
                    _liveRestartable = true;
                }

                StateHasChanged();
            });
        }
    }

    private async Task OnChangeAsync(StateChangeNotification change)
    {
        if (Workspace?.Key is { } key && HistoryLiveTailCovers(change, key))
        {
            _entryVersion++;
        }

        if (_pager is null || _pager.PageIndex > 0)
        {
            _stale = true;
            StateHasChanged();
            return;
        }

        if (_refreshing)
        {
            _pendingRefresh = true;
            return;
        }

        _refreshing = true;
        try
        {
            do
            {
                _pendingRefresh = false;
                await ResetAsync();
                StateHasChanged();
            }
            while (_pendingRefresh && !_lifetime.IsLeft);
        }
        finally
        {
            _refreshing = false;
        }
    }

    private static bool HistoryLiveTailCovers(StateChangeNotification change, string key) =>
        HistoryLiveTail.Covers(change, key);
}

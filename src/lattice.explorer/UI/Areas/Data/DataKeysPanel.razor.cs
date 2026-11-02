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
    private readonly ComponentLifetime _scans = new();
    private IDataReader? _reader;
    private ILatticeStateClient? _client;
    private DataPager? _pager;
    private (string StateId, string? Prefix, string? Index, string? Tag)? _query;
    private string? _prefixInput;
    private string? _index;
    private string? _tag;
    private IReadOnlyList<TagIndexRef> _indexes = [];
    private IReadOnlyList<string> _tags = [];
    private string? _indexesFor;
    private (string StateId, string? Index)? _tagsFor;
    private EntryScanMode _mode = EntryScanMode.Live;
    private int _pageSize = DataPaging.DefaultPageSize;
    private bool _loading;
    private bool _refreshing;
    private bool _pendingRefresh;
    private int _changeRefreshFailures;
    private DateTimeOffset _nextChangeRefreshUtc;
    private string? _error;
    private bool _cursorExpired;
    private bool _stale;
    private bool _followAvailable;
    private bool _live = true;
    private string? _liveNote;
    private bool _liveRestartable;
    private readonly ComponentLifetime _follows = new();
    private readonly ComponentLifetime _rightsLifetime = new();
    private int _entryVersion;
    private DataWriteRights _treeRights;
    private DataTreeEntry? _rightsFor;
    private bool _creating;
    private bool _listening;

    [Inject]
    internal DataWriteGate Gate { get; set; } = default!;

    [Inject]
    internal DataEntryCommands EntryCommands { get; set; } = default!;

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

        if (!_listening)
        {
            _listening = true;
            EntryCommands.Requested += OnCommand;
        }

        if (!ReferenceEquals(_rightsFor, workspace.Tree))
        {
            _ = LoadTreeRightsAsync(workspace.Tree);
        }

        var index = workspace.Address.GetQuery(DataTabs.IndexQuery);
        var tag = workspace.Address.GetQuery(DataTabs.TagQuery);
        var query = (workspace.Tree.StateId, workspace.Prefix, index, tag);
        if (_query == query)
        {
            return;
        }

        var token = _lifetime.Renew();
        _scans.Renew();
        StopFollow();
        _query = query;
        _prefixInput = workspace.Prefix;
        _index = index;
        _tag = tag;

        if (_indexesFor != workspace.Tree.StateId)
        {
            await LoadIndexesAsync(workspace.Tree.StateId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }
        }

        if (_tagsFor != (workspace.Tree.StateId, index))
        {
            await LoadTagsAsync(workspace.Tree.StateId, token);
        }

        if (token.IsCancellationRequested)
        {
            // Left while the filters were read: nothing is paged or followed for a page that is gone.
            return;
        }

        await ResetAsync();
        if (!token.IsCancellationRequested)
        {
            RestartFollow();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        EntryCommands.Requested -= OnCommand;
        if (_rightsFor is { } tree)
        {
            EntryCommands.HideTree(tree);
        }

        _rightsLifetime.Leave();
        _follows.Leave();
        _scans.Leave();
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

    private async Task LoadIndexesAsync(string stateId, CancellationToken token)
    {
        _indexesFor = null;
        _indexes = [];
        if (_reader is null)
        {
            return;
        }

        try
        {
            var indexes = await _reader.ListTagIndexesForTreeAsync(stateId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _indexes = indexes;
            _indexesFor = stateId;
            IndexOptions = [new(string.Empty, "No tag filter"), .. _indexes.Select(index => new LtSelectOption(index.IndexName, index.IndexName))];
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // The tag filter is optional; without the index list it is simply absent.
        }
    }

    private async Task LoadTagsAsync(string stateId, CancellationToken token)
    {
        _tagsFor = null;
        _tags = [];
        TagOptions = [];
        if (_reader is null || _index is null)
        {
            _tagsFor = (stateId, _index);
            return;
        }

        try
        {
            var tags = await _reader.ListTagValuesForIndexAsync(stateId, _index, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _tags = tags;
            _tagsFor = (stateId, _index);
            TagOptions = [new(string.Empty, "Choose a tag"), .. _tags.Select(tag => new LtSelectOption(tag, tag))];
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            if (!token.IsCancellationRequested)
            {
                _tags = [];
            }
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
            RecordLoadFailure();
            return;
        }

        var token = _scans.Renew();
        var pager = new DataPager(_reader);
        _loading = true;
        _error = null;
        _cursorExpired = false;
        _stale = false;
        try
        {
            await pager.ResetAsync(workspace.Tree.StateId, _pageSize, ActiveTagFilter, ActiveTagFilter is null ? workspace.Prefix : null, _mode, token);
            if (token.IsCancellationRequested)
            {
                await CloseQuietlyAsync(pager);
                return;
            }

            var previous = _pager;
            _pager = pager;
            RecordLoadSuccess();
            if (previous is not null)
            {
                await CloseQuietlyAsync(previous);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _error = DataErrors.Describe(exception, "read this tree's keys");
                RecordLoadFailure();
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _loading = false;
            }
        }
    }

    private async Task NextPageAsync()
    {
        if (_pager is not { } pager)
        {
            return;
        }

        var token = _scans.Token;
        _loading = true;
        _stale = false;
        _error = null;
        try
        {
            await pager.NextAsync(token);
            if (token.IsCancellationRequested)
            {
                await CloseQuietlyAsync(pager);
                return;
            }

            RecordLoadSuccess();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception) when (DataErrors.IsCursorExpired(exception, resuming: true))
        {
            if (!token.IsCancellationRequested)
            {
                _cursorExpired = true;
            }
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _error = DataErrors.Describe(exception, "read the next page of keys");
                RecordLoadFailure();
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _loading = false;
            }
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
                await InvokeAsync(() => cancellationToken.IsCancellationRequested ? Task.CompletedTask : OnChangeAsync(change));
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

        if (_error is not null && DateTimeOffset.UtcNow < _nextChangeRefreshUtc)
        {
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
            while (_pendingRefresh
                && !_lifetime.IsLeft
                && (_error is null || DateTimeOffset.UtcNow >= _nextChangeRefreshUtc));
        }
        finally
        {
            _refreshing = false;
            _pendingRefresh = false;
        }
    }

    private void RecordLoadSuccess()
    {
        _changeRefreshFailures = 0;
        _nextChangeRefreshUtc = DateTimeOffset.MinValue;
    }

    private void RecordLoadFailure()
    {
        _changeRefreshFailures = Math.Min(_changeRefreshFailures + 1, 6);
        var delaySeconds = Math.Min(30, 1 << (_changeRefreshFailures - 1));
        _nextChangeRefreshUtc = DateTimeOffset.UtcNow.AddSeconds(delaySeconds);
    }

    private static bool HistoryLiveTailCovers(StateChangeNotification change, string key) =>
        HistoryLiveTail.Covers(change, key);

    /// <summary>
    /// Asks the data facade whether the caller may write anywhere in the tree, and
    /// publishes it for the palette. Until it answers, and whenever it cannot,
    /// "New entry..." is not drawn.
    /// </summary>
    private async Task LoadTreeRightsAsync(DataTreeEntry tree)
    {
        var token = _rightsLifetime.Renew();
        if (_rightsFor is { } previous)
        {
            EntryCommands.HideTree(previous);
        }

        _rightsFor = tree;
        _treeRights = DataWriteRights.None;
        EntryCommands.ShowTree(tree, DataWriteRights.None);
        var rights = await Gate.ForTreeAsync(tree, token);
        if (token.IsCancellationRequested || _rightsLifetime.IsLeft)
        {
            return;
        }

        _treeRights = rights;
        EntryCommands.ShowTree(tree, rights);
        if (EntryCommands.TryTake(DataEntryCommands.NewEntryId))
        {
            OpenCreate();
        }

        await InvokeAsync(StateHasChanged);
    }

    private void OnCommand(string commandId)
    {
        if (string.Equals(commandId, DataEntryCommands.NewEntryId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(() =>
            {
                if (!_lifetime.IsLeft)
                {
                    OpenCreate();
                    StateHasChanged();
                }
            });
        }
    }

    private void OpenCreate()
    {
        if (_treeRights.CanWrite)
        {
            _creating = true;
        }
    }

    private void OnCreated(string key)
    {
        if (Workspace is { } workspace)
        {
            workspace.NavigateTo(workspace.With(ExplorerAddress.KeyQuery, key));
        }
    }
}

using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>
/// The History tab: a key's revisions from the state API, newest first, with
/// diffs, a live tail, and a point in time; or, with only a prefix, a live tail
/// of the changes under it.
/// </summary>
public partial class DataHistoryPanel : IDisposable
{
    /// <summary>How many revisions one page asks for.</summary>
    internal const int PageSize = 50;

    /// <summary>The most prefix changes kept on screen.</summary>
    internal const int PrefixChangeLimit = 200;

    private readonly CancellationTokenSource _lifetime = new();
    private readonly List<HistoryRevisionRow> _durable = [];
    private readonly List<HistoryRevisionRow> _liveRows = [];
    private readonly List<StateChangeNotification> _prefixChanges = [];
    private ILatticeStateClient? _client;
    private (string StateId, string? Key, string? Prefix, string? At)? _query;
    private HistoryTimeline? _timeline;
    private HistoryLiveTail? _tail;
    private StateQueryStatus _status;
    private EntryHistoryBound _bound;
    private HybridLogicalClock _earliest;
    private string? _continuation;
    private DateTimeOffset? _at;
    private string? _atInput;
    private string? _atError;
    private string? _keyInput;
    private bool _newestFirst = true;
    private bool _live = true;
    private bool _following;
    private bool _followOffered = true;
    private string? _liveNote;
    private bool _liveRestartable;
    private bool _loading;
    private string? _error;
    private CancellationTokenSource? _follow;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private string CountText => _timeline is null
        ? string.Empty
        : DataArea.Plural(_timeline.Rows.Count, "revision") + (_continuation is null ? string.Empty : " so far");

    private string? BoundNote => _bound switch
    {
        EntryHistoryBound.Truncated => $"Older revisions were trimmed; history is available from {DataFormat.Time(_earliest)}.",
        EntryHistoryBound.WalWindowFallback => "This tree keeps no durable history, so only changes still in the write-ahead log are shown.",
        _ => null,
    };

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        try
        {
            _client = DataServices.Find<ILatticeStateClient>(Services);
        }
        catch (InvalidOperationException)
        {
            _client = null;
        }
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var atText = workspace.Address.GetQuery(ExplorerAddress.AtQuery);
        var query = (workspace.Tree.StateId, workspace.Key, workspace.Prefix, atText);
        if (_query == query)
        {
            return;
        }

        _query = query;
        StopFollow();
        _keyInput = null;
        _atInput = atText;
        _atError = null;
        _at = null;
        if (atText is not null)
        {
            if (DataFormat.TryParseInstant(atText, out var at))
            {
                _at = at;
            }
            else
            {
                _atError = "Write a time such as 2026-09-28T14:00:00Z.";
            }
        }

        _prefixChanges.Clear();
        if (workspace.Key is not null)
        {
            await ReloadAsync();
        }

        RestartFollow();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        StopFollow();
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    internal static string ChangeText(StateChangeKind kind) => kind switch
    {
        StateChangeKind.Delete => "Deleted",
        StateChangeKind.DeleteRange => "Range deleted",
        _ => "Set",
    };

    private static string RowKindText(HistoryRevisionRow row) => row.RenderMode switch
    {
        HistoryRowRenderMode.Delete => "Deleted",
        HistoryRowRenderMode.RangeTombstone => "Range deleted",
        HistoryRowRenderMode.CrdtMembers when row.IsSnapshot => "Full state",
        HistoryRowRenderMode.CrdtMembers => "CRDT change",
        HistoryRowRenderMode.LiveTail => "Live " + (row.Kind == HistoryRowKind.Delete ? "delete" : "set"),
        HistoryRowRenderMode.MetadataOnly => "Set (metadata only)",
        _ => "Set",
    };

    private static string DiffClass(HistoryDiffLineKind kind) => kind switch
    {
        HistoryDiffLineKind.Added => "lt-data-diff__line lt-data-diff__line--added",
        HistoryDiffLineKind.Removed => "lt-data-diff__line lt-data-diff__line--removed",
        _ => "lt-data-diff__line",
    };

    private static string DiffPrefix(HistoryDiffLineKind kind) => kind switch
    {
        HistoryDiffLineKind.Added => "+ ",
        HistoryDiffLineKind.Removed => "- ",
        _ => "  ",
    };

    private bool IsInEffect(HistoryRevisionRow row)
    {
        if (_at is null || _timeline is null || row.IsLiveTail)
        {
            return false;
        }

        HistoryRevisionRow? newest = null;
        foreach (var candidate in _timeline.Rows)
        {
            if (!candidate.IsLiveTail && (newest is null || candidate.Hlc > newest.Hlc))
            {
                newest = candidate;
            }
        }

        return ReferenceEquals(row, newest);
    }

    private async Task ReloadAsync()
    {
        _durable.Clear();
        _liveRows.Clear();
        _continuation = null;
        _timeline = null;
        await LoadPageAsync();
    }

    private Task LoadOlderAsync() => LoadPageAsync();

    private async Task LoadPageAsync()
    {
        if (Workspace is not { Key: { } key } workspace)
        {
            return;
        }

        if (_client is null)
        {
            _error = "This Explorer has no state API to read history through.";
            return;
        }

        _loading = true;
        _error = null;
        try
        {
            var response = await _client.GetEntryHistoryAsync(
                new EntryHistoryRequest
                {
                    TreeId = workspace.Tree.StateId,
                    Key = key,
                    ToHlc = _at is { } at ? new HybridLogicalClock { WallClockTicks = at.UtcTicks, Counter = int.MaxValue } : null,
                    Limit = PageSize,
                    ContinuationToken = _continuation,
                    ValuePreviewBudget = HistoryReader.HistoryPreviewBudget,
                    Reverse = true,
                },
                _lifetime.Token);

            foreach (var record in response.Revisions)
            {
                _durable.Add(HistoryRevisionRow.From(record));
            }

            _status = response.Status;
            _bound = response.Bound;
            _earliest = response.EarliestAvailable;
            _continuation = string.IsNullOrEmpty(response.ContinuationToken) ? null : response.ContinuationToken;
            _tail = new HistoryLiveTail(key, _durable);
            Build();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _error = DataErrors.Describe(exception, "read this key's history");
        }
        finally
        {
            _loading = false;
        }
    }

    private void Build()
    {
        if (Workspace is not { Key: { } key } workspace)
        {
            return;
        }

        // Pages arrive newest first; the timeline is built oldest first so each
        // retained value diffs against the one before it, with the live tail last.
        var chronological = new List<HistoryRevisionRow>(_durable.Count + _liveRows.Count);
        for (var i = _durable.Count - 1; i >= 0; i--)
        {
            chronological.Add(_durable[i]);
        }

        chronological.AddRange(_liveRows);
        _timeline = HistoryTimeline.Build(
            workspace.Tree.StateId,
            key,
            _status,
            chronological,
            _bound,
            _earliest,
            _continuation,
            _newestFirst);
    }

    private void SetNewestFirst(bool newestFirst)
    {
        _newestFirst = newestFirst;
        Build();
    }

    private void ApplyAt()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(_atInput))
        {
            ClearAt();
            return;
        }

        if (!DataFormat.TryParseInstant(_atInput, out var at))
        {
            _atError = "Write a time such as 2026-09-28T14:00:00Z.";
            return;
        }

        workspace.NavigateTo(workspace.With(ExplorerAddress.AtQuery, DataFormat.Instant(at)));
    }

    private void ClearAt() => Workspace?.NavigateTo(Workspace.With(ExplorerAddress.AtQuery, null));

    private void ShowKey()
    {
        if (Workspace is { } workspace && !string.IsNullOrEmpty(_keyInput))
        {
            workspace.NavigateTo(workspace.With(ExplorerAddress.KeyQuery, _keyInput));
        }
    }

    private void SetLive(bool live)
    {
        _live = live;
        _liveNote = null;
        _liveRestartable = false;
        RestartFollow();
    }

    private void RestartLive() => SetLive(true);

    private void StopFollow()
    {
        _following = false;
        if (_follow is { } follow)
        {
            _follow = null;
            follow.Cancel();
            follow.Dispose();
        }
    }

    private void RestartFollow()
    {
        StopFollow();
        if (_client is null || !_followOffered || !_live || _at is not null || Workspace is not { } workspace)
        {
            return;
        }

        StateObserveRequest request;
        if (workspace.Key is { } key)
        {
            request = new StateObserveRequest { TreeId = workspace.Tree.StateId, StartInclusive = key, EndExclusive = key + '\0' };
        }
        else if (workspace.Prefix is { Length: > 0 } prefix)
        {
            request = new StateObserveRequest
            {
                TreeId = workspace.Tree.StateId,
                StartInclusive = prefix,
                EndExclusive = DataTreeNames.PrefixUpperBound(prefix),
            };
        }
        else
        {
            return;
        }

        var follow = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        _follow = follow;
        _following = true;
        _ = FollowAsync(_client, request, workspace.Key, follow.Token);
    }

    private async Task FollowAsync(ILatticeStateClient client, StateObserveRequest request, string? key, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var change in client.ObserveChangesAsync(request, cancellationToken))
            {
                await InvokeAsync(() =>
                {
                    if (key is null)
                    {
                        _prefixChanges.Insert(0, change);
                        if (_prefixChanges.Count > PrefixChangeLimit)
                        {
                            _prefixChanges.RemoveAt(_prefixChanges.Count - 1);
                        }
                    }
                    else if (_tail is not null && _tail.TryAccept(change, out var row) && row is not null)
                    {
                        _liveRows.Add(row);
                        Build();
                    }

                    StateHasChanged();
                });
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
                _following = false;
                if (DataErrors.IsNotOffered(exception))
                {
                    _followOffered = false;
                    _liveNote = "Live updates are not available on this cluster.";
                }
                else
                {
                    _liveNote = DataErrors.IsCursorExpired(exception, resuming: false)
                        ? "Live updates stopped because the change feed moved past this position."
                        : DataErrors.Describe(exception, "follow changes to this key");
                    _liveRestartable = true;
                }

                StateHasChanged();
            });
        }
    }
}

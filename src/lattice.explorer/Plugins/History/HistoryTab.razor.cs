using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Plugins.History;

/// <summary>
/// The revision-timeline surface's retrospective state: the accumulated
/// revisions, the paged read, and the display rebuild. The live tail and its
/// backfill live in <c>HistoryTab.Live.cs</c> and the markup in
/// <c>HistoryTab.razor</c>; all three are one partial class.
/// </summary>
public partial class HistoryTab
{
    /// <summary>The page size requested for each history read.</summary>
    private const int PageLimit = 50;

    private readonly List<HistoryRevisionRow> _accumulated = [];

    private string? _key;
    private HistoryTimeline? _timeline;
    private StateQueryStatus _status;
    private EntryHistoryBound _bound;
    private HybridLogicalClock _earliest;
    private string? _continuation;

    private LatticeConnectionState _connectionState;
    private IDisposable? _connectionSubscription;

    private bool _loading;
    private string? _error;
    private bool _newestFirst = true;

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        // The live indicator mirrors the shell's connection state, so a status
        // change must redraw the toolbar dot and label. The subscription is owned
        // by this view and released in Dispose, so it never outlives it.
        _connectionState = Surface.ConnectionState;
        _connectionSubscription = Surface.ObserveConnection(OnConnectionStateChanged);

        // Open on the key the operator drilled into on the value drill-down
        // surface: the two share one inspected key per tree, so activating this
        // surface shows the timeline they meant without a payload changing hands.
        _key = Surface.InspectedKey(Selection.Id);
        if (_key is not null)
        {
            await LoadFirstAsync();
        }
    }

    private void OnConnectionStateChanged(LatticeConnectionState state) =>
        InvokeAsync(() =>
        {
            _connectionState = state;
            StateHasChanged();
        });

    private async Task LoadFirstAsync()
    {
        StopFollowing();
        _accumulated.Clear();
        _liveError = null;
        _continuation = null;
        _timeline = null;
        await LoadPageAsync(continuation: null);

        // Begin (or restart) the live tail only once the retrospective first page
        // has loaded, so the seen-set is seeded against the loaded revisions and
        // the live tail does not double-count an overlapping revision.
        StartFollowing();
    }

    private Task LoadMoreAsync() => LoadPageAsync(_continuation);

    private async Task LoadPageAsync(string? continuation)
    {
        if (_key is null)
        {
            return;
        }

        _loading = true;
        _error = null;
        StateHasChanged();

        try
        {
            var page = await Surface.LoadAsync(Selection.Id, _key, PageLimit, continuation, TabToken);

            _accumulated.AddRange(page.Revisions);
            _status = page.Status;
            _bound = page.Bound;
            _earliest = page.EarliestAvailable;
            _continuation = page.ContinuationToken;

            Rebuild();
        }
        catch (OperationCanceledException) when (TabToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _error = ex.Message;
        }
        finally
        {
            _loading = false;
            StateHasChanged();
        }
    }

    private void ToggleOrder(ChangeEventArgs e)
    {
        _newestFirst = e.Value is bool b ? b : !_newestFirst;
        Rebuild();
    }

    /// <summary>
    /// Re-derives the display timeline (diffs, dividers, ordering, active mode)
    /// from the accumulated chronological revisions without a server round trip.
    /// </summary>
    private void Rebuild()
    {
        if (_key is null)
        {
            return;
        }

        _timeline = HistoryTimeline.Build(
            Selection.Id, _key, _status, _accumulated, _bound, _earliest, _continuation, _newestFirst);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connectionSubscription?.Dispose();
            _connectionSubscription = null;
            StopFollowing();
        }

        base.Dispose(disposing);
    }
}

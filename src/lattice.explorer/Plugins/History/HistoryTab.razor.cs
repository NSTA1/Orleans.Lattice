using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Core.Vocabulary;

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

    /// <summary>
    /// What the forward-only live feed is and is not, said once, through the
    /// help disclosure rather than a tooltip.
    /// </summary>
    private const string LiveFeedExplanation =
        "The live feed only carries changes emitted while this surface is open: it extends the "
        + "timeline forward from now and is not a substitute for durable retention of past "
        + "changes. New rows show kind, time and origin first, then upgrade to the value diff or "
        + "CRDT members once the durable backfill arrives.";

    /// <summary>What the retention badge beside the key means.</summary>
    private const string RetentionExplanation =
        "The retention mode in force over this key, which decides how much of each revision is "
        + "kept: the whole value, metadata only, or a hybrid that keeps values for some revisions "
        + "and metadata for the rest.";

    /// <summary>
    /// The prompt shown when no key has been drilled into yet. Built once; it
    /// names the surface to go to rather than leaving the reader to guess.
    /// </summary>
    private static readonly ExplorerStateMessage NoKeyInspected =
        ExplorerStateCopy.Empty(ExplorerSubjects.Changes) with
        {
            Headline = "No key chosen",
            Explanation = "This surface shows the revisions of one key, and no key has been "
                + "opened for this table yet.",
            Remedy = "Open the Data surface, choose a key, and return here.",
        };

    /// <summary>
    /// What the surface says when the table itself has gone. Distinct from an
    /// empty history: the reader's link is stale, not their key.
    /// </summary>
    private static readonly ExplorerStateMessage TreeNotFound =
        ExplorerStateCopy.Empty(ExplorerSubjects.Changes) with
        {
            Headline = "Table not found",
            Explanation = "The table this key belongs to is no longer in the cluster. It may have "
                + "been deleted since the catalog was listed.",
            Remedy = "Choose another table from the catalog.",
        };

    /// <summary>
    /// What the surface says when the key has no recorded revisions. Explicit
    /// that this is not a refusal and not a retention gap.
    /// </summary>
    private static readonly ExplorerStateMessage NoRevisions =
        ExplorerStateCopy.Empty(ExplorerSubjects.Changes) with
        {
            Headline = "No revisions for this key",
            Explanation = "No revisions of this key are recorded. Nothing is being hidden from "
                + "you: the key may never have been written, or every revision of it may have "
                + "aged out of the retention window.",
            Remedy = "Choose another key, or check the retention configured for this table.",
        };

    private readonly List<HistoryRevisionRow> _accumulated = [];

    private EventCallback _reload;

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
    private ExplorerStateMessage? _failure;
    private bool _newestFirst = true;

    /// <summary>
    /// The state the timeline is in, or <see langword="null"/> when it has
    /// revisions to draw. Five distinct situations, each named rather than
    /// collapsed into one "nothing here": no key chosen, a read in flight, a
    /// read that failed, a table that has gone, and a key with no revisions.
    /// </summary>
    /// <remarks>
    /// Read on the render path, and twice per pass - and this surface re-renders
    /// on every live row and every connection-status change - so it only ever
    /// selects an already-built message. The failure copy quotes the cluster's
    /// own words and therefore has to be composed; it is composed once, where
    /// the failure is caught.
    /// </remarks>
    private ExplorerStateMessage? State
    {
        get
        {
            if (_key is null)
            {
                return NoKeyInspected;
            }

            if (_failure is not null)
            {
                return _failure;
            }

            if (_timeline is null)
            {
                return _loading
                    ? ExplorerStateCopy.Loading(ExplorerSubjects.Changes)
                    : NoRevisions;
            }

            if (_timeline.Status == StateQueryStatus.TreeNotFound)
            {
                return TreeNotFound;
            }

            return _timeline.Status == StateQueryStatus.KeyNotFound || !_timeline.HasRows
                ? NoRevisions
                : null;
        }
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        // Bound once rather than per render, so the state block's retry does not
        // allocate a delegate on every pass.
        _reload = EventCallback.Factory.Create(this, LoadFirstAsync);

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
        _failure = null;
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

            // Composed here, not in the State property: the failure copy quotes
            // the cluster's words and so has to be built, and State is read on
            // every render pass - of which this surface has many, because a live
            // row and a connection-status change each trigger one.
            _failure = ExplorerStateCopy.Failed(ExplorerSubjects.Changes, _error);
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

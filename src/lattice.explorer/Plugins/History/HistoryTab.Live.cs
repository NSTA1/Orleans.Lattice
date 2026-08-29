using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Plugins.History;

/// <summary>
/// The revision-timeline surface's live follow mode: the forward-only change
/// tail and the debounced backfill that upgrades its metadata-only markers to
/// durable rows.
/// <para>
/// Everything here is driven by the change feed rather than by a clock: the
/// follow loop appends whatever the feed yields, and the backfill is a debounce
/// on top of that. Both are linked to the view's own lifetime token, so a
/// superseded selection or a surface switch tears the subscription down and
/// abandons its in-flight refetch.
/// </para>
/// </summary>
public partial class HistoryTab
{
    /// <summary>
    /// How long a burst of live rows is allowed to coalesce before the durable
    /// backfill refetches. Long enough that a write storm costs one refetch, and
    /// short enough that a single change upgrades promptly.
    /// </summary>
    private static readonly TimeSpan BackfillDebounce = TimeSpan.FromMilliseconds(750);

    private CancellationTokenSource? _liveCts;
    private CancellationTokenSource? _backfillCts;
    private string? _liveError;

    /// <summary>
    /// Opens the live subscription for the current key, seeding the
    /// de-duplication tail with the already-loaded revisions. The loop is linked
    /// to the view token so a tear-down cancels it; <see cref="StopFollowing"/>
    /// cancels it on a key change.
    /// </summary>
    private void StartFollowing()
    {
        if (_key is null)
        {
            return;
        }

        var tail = new HistoryLiveTail(_key, _accumulated);
        var cts = CancellationTokenSource.CreateLinkedTokenSource(TabToken);
        _liveCts = cts;
        _ = FollowLoopAsync(tail, cts);
    }

    private async Task FollowLoopAsync(HistoryLiveTail tail, CancellationTokenSource cts)
    {
        var treeId = Selection.Id;
        try
        {
            await foreach (var row in Surface.FollowAsync(treeId, tail, cts.Token).ConfigureAwait(false))
            {
                await InvokeAsync(() =>
                {
                    OnLiveRow(row);
                    StateHasChanged();
                });
            }
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
            // Expected teardown on key change or view dispose. The cancelled
            // stream may surface either as OperationCanceledException or as a
            // transport "Cancelled" status; both are silent teardown, not a fault.
        }
        catch (Exception ex)
        {
            await InvokeAsync(() =>
            {
                _liveError = ex.Message;
                StateHasChanged();
            });
        }
        finally
        {
            cts.Dispose();
        }
    }

    /// <summary>Appends an accepted live row; rows are de-duplicated by the tail.</summary>
    private void OnLiveRow(HistoryRevisionRow row)
    {
        _accumulated.Add(row);
        Rebuild();

        // A live row is a metadata-only marker. Once the durable history view
        // records the revision, a silent first-page refetch returns the full
        // value/diff for the same clock; debounce so a burst coalesces.
        ScheduleBackfill();
    }

    /// <summary>
    /// Debounced silent refetch that upgrades live-tail markers to durable rows
    /// without disrupting the live subscription or showing a spinner.
    /// </summary>
    private void ScheduleBackfill()
    {
        // Only auto-backfill while a single page is loaded; once the operator has
        // paged older history, a first-page refetch would drop those pages, so
        // leave the markers until a manual refresh.
        var durableLoaded = 0;
        for (var i = 0; i < _accumulated.Count; i++)
        {
            if (!_accumulated[i].IsLiveTail)
            {
                durableLoaded++;
            }
        }

        if (durableLoaded > PageLimit)
        {
            return;
        }

        _backfillCts?.Cancel();
        _backfillCts?.Dispose();
        var cts = CancellationTokenSource.CreateLinkedTokenSource(TabToken);
        _backfillCts = cts;
        _ = BackfillAsync(cts);
    }

    private async Task BackfillAsync(CancellationTokenSource cts)
    {
        try
        {
            await Task.Delay(BackfillDebounce, cts.Token).ConfigureAwait(false);
            var page = await Surface
                .LoadAsync(Selection.Id, _key!, PageLimit, continuationToken: null, cts.Token)
                .ConfigureAwait(false);
            await InvokeAsync(() => MergeBackfill(page));
        }
        catch (Exception) when (cts.IsCancellationRequested)
        {
            // Superseded by a newer change or torn down; leave markers in place.
        }
        catch
        {
            // Backfill is best-effort; the live markers stay until the next try.
        }
        finally
        {
            if (ReferenceEquals(_backfillCts, cts))
            {
                _backfillCts = null;
            }

            cts.Dispose();
        }
    }

    /// <summary>
    /// Replaces the loaded durable revisions with the refetched page, keeping any
    /// live markers whose revision the source has not yet recorded so nothing is
    /// lost while a marker awaits its durable backfill.
    /// </summary>
    private void MergeBackfill(HistoryPage page)
    {
        if (_key is null)
        {
            return;
        }

        var durable = new HashSet<HybridLogicalClock>(page.Revisions.Count);
        for (var i = 0; i < page.Revisions.Count; i++)
        {
            durable.Add(page.Revisions[i].Hlc);
        }

        var stillPending = new List<HistoryRevisionRow>();
        for (var i = 0; i < _accumulated.Count; i++)
        {
            var row = _accumulated[i];
            if (row.IsLiveTail && !durable.Contains(row.Hlc))
            {
                stillPending.Add(row);
            }
        }

        _accumulated.Clear();
        _accumulated.AddRange(page.Revisions);
        _accumulated.AddRange(stillPending);
        _status = page.Status;
        _bound = page.Bound;
        _earliest = page.EarliestAvailable;
        _continuation = page.ContinuationToken;

        Rebuild();
        StateHasChanged();
    }

    private void StopFollowing()
    {
        _liveCts?.Cancel();
        _liveCts = null;
        _backfillCts?.Cancel();
        _backfillCts = null;
    }
}

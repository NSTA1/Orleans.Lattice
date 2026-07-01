using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;

namespace MultiSiteManufacturing.Host.Dashboard;

/// <summary>
/// Per-fact fan-out path for <see cref="DashboardBroadcaster"/>.
/// Receives a fact from <see cref="OnBroadcastReceived"/>, derives
/// the corresponding <see cref="PartSummaryUpdate"/> /
/// <see cref="DivergenceEvent"/> / <see cref="SiteActivityIndexEntry"/>
/// values, and writes them to every active per-circuit channel.
/// Errors are logged but never thrown - the cluster-wide stream
/// agent must keep moving even if a single fact's fan-out fails.
/// </summary>
public sealed partial class DashboardBroadcaster
{
    private void OnChaosConfigChanged(object? sender, EventArgs e) => _ = PublishChaosAsync();

    /// <summary>
    /// Marks a part's summary as needing a rebuild. The actual rebuild is
    /// performed by <see cref="RunPartRebuildLoopAsync"/> at most once per
    /// the configured rebuild interval, coalescing a burst of facts for the
    /// same serial into a single fact-tree scan.
    /// <para>
    /// Marks unconditionally - the per-part rebuild maintains the
    /// materialised <see cref="PartSummaryView"/> (one row per part) that the
    /// dashboard snapshot reads, so the view must stay current from the fact
    /// stream even when no circuit is attached; otherwise a later dashboard
    /// open would read stale rows. The per-circuit channel fan-out is still
    /// gated on having subscribers inside <see cref="FanOutPartUpdate"/>, so
    /// no channel work is done for an idle dashboard. At true idle (no facts
    /// arriving) nothing is marked, so the rebuild loop simply sleeps.
    /// </para>
    /// </summary>
    private void MarkPartDirty(PartSerialNumber serial)
    {
        _dirtyParts[serial] = 0;
    }

    /// <summary>
    /// Test seam: number of parts currently queued for a coalesced rebuild.
    /// Lets a test assert the coalescing-set behaviour deterministically
    /// without depending on stream-delivery timing.
    /// </summary>
    internal int PendingRebuildCount => _dirtyParts.Count;

    /// <summary>
    /// Test seam: invokes the same dirty-marking path the stream handlers use,
    /// so a test can exercise the coalescing-set behaviour directly.
    /// </summary>
    internal void MarkPartDirtyForTest(PartSerialNumber serial) => MarkPartDirty(serial);

    /// <summary>
    /// Test seam: drains the dirty set once on the caller's task, rebuilding
    /// each queued part's summary (and materialising it into
    /// <see cref="PartSummaryView"/>) synchronously. Lets a test deterministically
    /// materialise the view without waiting on the background rebuild interval.
    /// </summary>
    internal Task<int> DrainDirtyForTestAsync() => DrainDirtyAsync(CancellationToken.None);

    /// <summary>
    /// Test seam: current consecutive-failed-drain streak (readable) and a way
    /// to feed a synthetic drain outcome, so a test can drive the back-off
    /// state machine deterministically without a genuinely failing storage tier
    /// (which cannot be reproduced in the in-memory test cluster).
    /// </summary>
    internal int ConsecutiveFailedDrainsForTest => _consecutiveFailedDrains;

    /// <summary>Test seam: applies one drain outcome to the back-off counter.</summary>
    internal void RecordDrainOutcomeForTest(int failures) => RecordDrainOutcome(failures);

    /// <summary>Test seam: the delay the loop would wait before its next cycle.</summary>
    internal TimeSpan ComputeRebuildDelayForTest() => ComputeRebuildDelay();

    /// <summary>Test seam: the configured ceiling on the rebuild back-off delay.</summary>
    internal static TimeSpan MaxRebuildBackoffForTest => MaxRebuildBackoff;

    /// <summary>
    /// Background loop that drains <see cref="_dirtyParts"/> once per
    /// the configured rebuild interval, rebuilding each dirty part's summary
    /// exactly once per window. Replaces the previous per-fact, per-stream
    /// synchronous rebuild-and-publish call that turned every fact
    /// (and every replication re-delivery of it) into an immediate fact-tree
    /// scan on every silo - the scan-storm fixed here.
    /// </summary>
    private async Task RunPartRebuildLoopAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(ComputeRebuildDelay(), token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            // Reconcile the materialised view against tree truth on its own
            // (slower) cadence, but only while a dashboard is attached. This is
            // what surfaces parts written by a path that bypasses
            // FederationRouter (a direct SetMany seed raises no FactRouted, so
            // the stream never marks those serials dirty): the pass diffs the
            // tree against the view and queues the missing serials, which the
            // drain below then folds and fans out live (issue #1048).
            //
            // Skipped while backing off from upsert failures: re-queuing parts
            // we demonstrably cannot persist yet would only pour more load onto
            // the failing storage partition and defeat the back-pressure. The
            // still-dirty parts keep retrying (at the backed-off cadence); once
            // the storage tier recovers and a drain succeeds the counter resets
            // and reconciliation resumes.
            if (_consecutiveFailedDrains == 0)
            {
                await MaybeReconcileViewWithTreeAsync(token);
            }

            if (_dirtyParts.IsEmpty)
            {
                // Nothing pending: clear any accumulated back-off so the next
                // cycle reconciles and re-probes at the normal cadence.
                _consecutiveFailedDrains = 0;
                continue;
            }

            var failures = await DrainDirtyAsync(token);
            RecordDrainOutcome(failures);
        }
    }

    /// <summary>
    /// Updates the consecutive-failed-drain counter from one drain's failure
    /// count: any failure grows the streak (driving further back-off), a clean
    /// drain resets it so the loop returns to its normal cadence.
    /// </summary>
    private void RecordDrainOutcome(int failures) =>
        _consecutiveFailedDrains = failures > 0 ? _consecutiveFailedDrains + 1 : 0;

    /// <summary>
    /// Computes the delay before the next rebuild cycle. Returns the base
    /// <see cref="_partRebuildInterval"/> in steady state; while summary
    /// upserts are failing it grows the delay exponentially (doubling per
    /// consecutive failed cycle) up to <see cref="MaxRebuildBackoff"/>, so a
    /// saturated storage tier is not hammered by a full-rate retry storm.
    /// </summary>
    private TimeSpan ComputeRebuildDelay()
    {
        if (_consecutiveFailedDrains == 0)
        {
            return _partRebuildInterval;
        }

        // Cap the exponent so the shift cannot overflow, then clamp the scaled
        // delay at the configured ceiling.
        var exponent = Math.Min(_consecutiveFailedDrains, 16);
        var scaledMs = _partRebuildInterval.TotalMilliseconds * Math.Pow(2, exponent);
        var cappedMs = Math.Min(scaledMs, MaxRebuildBackoff.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(cappedMs);
    }

    /// <summary>
    /// True when at least one circuit is watching a feed that the per-part
    /// summary view backs (the part-summary grid or the divergence stream).
    /// Reconciliation is gated on this so an idle silo - or one serving only
    /// health-check prerenders - does no periodic tree scans at all.
    /// </summary>
    private bool HasPartWatchers => !_partSubs.IsEmpty || !_divSubs.IsEmpty;

    /// <summary>
    /// Runs a reconciliation pass at most once per <see cref="_reconcileInterval"/>,
    /// and only while a dashboard subscriber is attached. Failures are logged and
    /// swallowed so a transient tree-scan error never tears down the rebuild loop.
    /// </summary>
    private async Task MaybeReconcileViewWithTreeAsync(CancellationToken token)
    {
        if (!HasPartWatchers || DateTime.UtcNow < _nextReconcileUtc)
        {
            return;
        }

        try
        {
            await ReconcileViewWithTreeAsync(token);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Dashboard view reconciliation pass failed");
        }
        finally
        {
            _nextReconcileUtc = DateTime.UtcNow + _reconcileInterval;
        }
    }

    /// <summary>
    /// Diffs the fact tree (the source of truth) against the materialised
    /// <see cref="PartSummaryView"/> and marks any part present in the tree but
    /// absent from the view dirty, up to <see cref="_reconcileBudget"/> per pass.
    /// The bounded budget spreads a large backfill (e.g. a fresh bulk seed) over
    /// several cadences rather than folding every discovered part in one burst.
    /// Returns the number of parts newly queued.
    /// </summary>
    private async Task<int> ReconcileViewWithTreeAsync(CancellationToken token)
    {
        var treeParts = await ResolvePartSerialsAsync(token);
        if (treeParts.Count == 0)
        {
            return 0;
        }

        var viewRows = await _summaryView.ReadAllAsync(token);
        var known = new HashSet<PartSerialNumber>(viewRows.Count);
        foreach (var row in viewRows)
        {
            known.Add(row.Serial);
        }

        var marked = 0;
        foreach (var serial in treeParts)
        {
            if (token.IsCancellationRequested || marked >= _reconcileBudget)
            {
                break;
            }

            // Skip parts already materialised in the view or already queued by
            // the fact stream - reconciliation only fills the gap left by
            // non-routed writers.
            if (known.Contains(serial) || _dirtyParts.ContainsKey(serial))
            {
                continue;
            }

            MarkPartDirty(serial);
            marked++;
        }

        return marked;
    }

    /// <summary>
    /// Test seam: runs one reconciliation pass (bypassing the subscriber gate
    /// and cadence) and then drains, so a test can deterministically converge
    /// the view to tree truth for parts written directly to the tree (bypassing
    /// <see cref="FederationRouter"/>). Returns the number of parts the pass
    /// newly discovered.
    /// </summary>
    internal async Task<int> ReconcileViewWithTreeForTestAsync()
    {
        var marked = await ReconcileViewWithTreeAsync(CancellationToken.None);
        await DrainDirtyAsync(CancellationToken.None);
        return marked;
    }

    /// <summary>
    /// Maximum number of summary rows folded and flushed to the materialised
    /// view in a single <see cref="PartSummaryView.UpsertManyAsync"/> batch.
    /// Coalescing the drain's upserts into batched writes (instead of one
    /// <c>SetAsync</c> per part) collapses N per-part WAL appends into a few
    /// larger appends the WAL layer packs into fewer Azure Table transactions -
    /// the dominant durable-write cost when a bulk seed marks thousands of
    /// parts dirty at once. Bounded so a very large dirty set flushes
    /// incrementally rather than materialising every payload in memory at once.
    /// </summary>
    private const int SummaryUpsertBatchSize = 256;

    /// <summary>
    /// Drains the dirty set once: snapshots the queued serials, clears each as
    /// it is taken, folds its summary, and flushes the folded rows to the
    /// materialised view in <see cref="SummaryUpsertBatchSize"/>-sized batched
    /// writes (see <see cref="FlushSummaryBatchAsync"/>). Marks that arrive
    /// during the drain are picked up on the next tick. Returns the number of
    /// parts whose fold or durable write failed, so the rebuild loop can apply
    /// back-pressure.
    /// </summary>
    private async Task<int> DrainDirtyAsync(CancellationToken token)
    {
        var serials = _dirtyParts.Keys.ToArray();
        if (serials.Length == 0)
        {
            return 0;
        }

        var failures = 0;
        var batch = new List<PartSummaryUpdate>(Math.Min(serials.Length, SummaryUpsertBatchSize));
        foreach (var serial in serials)
        {
            if (token.IsCancellationRequested)
            {
                return failures;
            }

            _dirtyParts.TryRemove(serial, out _);

            PartSummaryUpdate update;
            try
            {
                update = await BuildSummaryAsync(serial, token);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to build dashboard update for serial {Serial}", serial.Value);
                failures++;
                continue;
            }

            batch.Add(update);
            if (batch.Count >= SummaryUpsertBatchSize)
            {
                failures += await FlushSummaryBatchAsync(batch, token);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            failures += await FlushSummaryBatchAsync(batch, token);
        }

        return failures;
    }

    /// <summary>
    /// Durably upserts one batch of folded summaries into the materialised
    /// <see cref="PartSummaryView"/> in a single batched write, then fans each
    /// row out to the in-memory part / divergence channels. A failed durable
    /// write re-queues the batch's serials (so a later cycle retries - the
    /// same self-healing the reconciliation pass provides) and reports the
    /// batch size as failures so the rebuild loop's back-off engages; the
    /// in-memory fan-out is skipped for a batch that did not persist.
    /// </summary>
    private async Task<int> FlushSummaryBatchAsync(List<PartSummaryUpdate> batch, CancellationToken token)
    {
        try
        {
            await _summaryView.UpsertManyAsync(batch, token);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to upsert {Count} dashboard summary rows", batch.Count);
            foreach (var update in batch)
            {
                MarkPartDirty(update.Serial);
            }
            return batch.Count;
        }

        foreach (var update in batch)
        {
            FanOutPartUpdate(update);
        }

        return 0;
    }

    /// <summary>
    /// Subscribed to <see cref="PartCrdtStore.PartChanged"/> in
    /// <see cref="StartAsync"/>. Forwards the carried serial onto the
    /// cluster-wide part-change stream so every silo's broadcaster -
    /// including this one - re-runs the per-circuit fan-out
    /// (<see cref="FanOutPartUpdate"/>) for whichever Blazor sessions
    /// it hosts. Without this stream hop a CRDT mutation handled on
    /// silo A would be invisible to a circuit pinned to silo B,
    /// because <see cref="PartCrdtStore.PartChanged"/> fires only on
    /// the silo that wrote the CRDT delta (or the silo that received
    /// the cross-cluster OR-Set apply). Fire-and-forget; any publish
    /// error is logged inside <see cref="PublishPartChangeToBroadcastStreamAsync"/>.
    /// </summary>
    private void OnPartCrdtChanged(PartSerialNumber serial) => _ = PublishPartChangeToBroadcastStreamAsync(serial);

    /// <summary>
    /// Builds a <see cref="SiteActivityIndexEntry"/> from the in-memory
    /// fact and fans it out to every site-activity subscriber. Exposed
    /// as a standalone helper so <see cref="OnBroadcastReceived"/> can
    /// share the same logic as tests that invoke it directly.
    /// </summary>
    private void FanOutSiteActivity(Fact fact)
    {
        try
        {
            var entry = new SiteActivityIndexEntry(
                fact.Site,
                fact.Serial,
                fact.Hlc,
                SiteActivityIndex.DescribeActivity(fact));
            foreach (var sub in _activitySubs.Values)
            {
                sub.Writer.TryWrite(entry);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fan out site-activity entry for fact {FactId}", fact.FactId);
        }
    }

    /// <summary>
    /// Fans one already-folded, already-persisted <see cref="PartSummaryUpdate"/>
    /// out to the in-memory per-circuit channels: the part-summary subscribers
    /// and any derived divergence transition. Pure in-memory work (no I/O) -
    /// the durable materialised-view write is done once per batch by
    /// <see cref="FlushSummaryBatchAsync"/> before this is called, so a bulk
    /// drain collapses N per-part durable writes into a few batched appends.
    /// Never throws: a channel write failure is logged and swallowed so one
    /// bad subscriber cannot stall the drain.
    /// </summary>
    private void FanOutPartUpdate(PartSummaryUpdate update)
    {
        try
        {
            // Per-circuit channel fan-out is gated on having watchers: no
            // summary subscriber means the TryWrite loop below would no-op
            // anyway, so skip it.
            foreach (var sub in _partSubs.Values)
            {
                sub.Writer.TryWrite(update);
            }

            // Derive a divergence transition, if any, and fan that out on
            // the divergence channel. We publish on:
            //   - entry into divergence (previous absent or agreed; now disagrees)
            //   - state change while still divergent (both backends' states
            //     have shifted but they still disagree)
            //   - resolution (previously disagreed; now agrees)
            var newStates = (update.BaselineState, update.LatticeState);
            _lastStates.TryGetValue(update.Serial, out var oldStates);
            _lastStates[update.Serial] = newStates;

            var nowDiverges = update.Diverges;
            var wasDiverging = oldStates != default && oldStates.Baseline != oldStates.Lattice;

            if (!nowDiverges && !wasDiverging)
            {
                return;
            }

            DivergenceEvent? evt = null;
            if (nowDiverges && (!wasDiverging || oldStates != newStates))
            {
                evt = new DivergenceEvent
                {
                    Serial = update.Serial,
                    BaselineState = update.BaselineState,
                    LatticeState = update.LatticeState,
                    Resolved = false,
                };
            }
            else if (!nowDiverges && wasDiverging)
            {
                evt = new DivergenceEvent
                {
                    Serial = update.Serial,
                    BaselineState = update.BaselineState,
                    LatticeState = update.LatticeState,
                    Resolved = true,
                };
            }

            if (evt is not null)
            {
                foreach (var sub in _divSubs.Values)
                {
                    sub.Writer.TryWrite(evt);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fan out dashboard update for serial {Serial}", update.Serial.Value);
        }
    }

    private async Task PublishChaosAsync()
    {
        try
        {
            var overview = await GetChaosOverviewAsync();
            foreach (var sub in _chaosSubs.Values)
            {
                sub.Writer.TryWrite(overview);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to build chaos overview update");
        }
    }
}

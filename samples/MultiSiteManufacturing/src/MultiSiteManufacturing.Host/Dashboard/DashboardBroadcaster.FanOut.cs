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
    /// Gated on <see cref="HasPartWatchers"/>: the sample no longer owns a
    /// durable summary tree to keep warm (the library-maintained folded view
    /// does that off the WAL), so there is nothing to maintain when no circuit
    /// is attached. Marking - and therefore the per-part fold + fan-out - only
    /// happens while a dashboard is actually watching. At true idle nothing is
    /// marked, so the rebuild loop simply sleeps.
    /// </para>
    /// </summary>
    private void MarkPartDirty(PartSerialNumber serial)
    {
        if (!HasPartWatchers)
        {
            return;
        }

        _dirtyParts[serial] = 0;
    }

    /// <summary>
    /// Test seam: number of parts currently queued for a coalesced rebuild.
    /// Lets a test assert the coalescing-set behaviour deterministically
    /// without depending on stream-delivery timing.
    /// </summary>
    internal int PendingRebuildCount => _dirtyParts.Count;

    /// <summary>
    /// Test seam: invokes the same dirty-marking path the stream handlers use
    /// (including the <see cref="HasPartWatchers"/> gate), so a test can
    /// exercise the coalescing-set behaviour directly.
    /// </summary>
    internal void MarkPartDirtyForTest(PartSerialNumber serial) => MarkPartDirty(serial);

    /// <summary>
    /// Test seam: drains the dirty set once on the caller's task, folding each
    /// queued part's summary and fanning it out synchronously. Lets a test
    /// deterministically fan out without waiting on the background rebuild
    /// interval.
    /// </summary>
    internal Task DrainDirtyForTestAsync() => DrainDirtyAsync(CancellationToken.None);

    /// <summary>
    /// Test seam: the set of serials the broadcaster has already fanned out
    /// live (tracked in <see cref="_lastStates"/>). Reconciliation only queues
    /// fact-tree parts absent from this set, so a test can assert convergence.
    /// </summary>
    internal IReadOnlyCollection<PartSerialNumber> FannedOutPartsForTest => _lastStates.Keys.ToArray();

    /// <summary>
    /// Background loop that drains <see cref="_dirtyParts"/> once per
    /// the configured rebuild interval, rebuilding each dirty part's summary
    /// exactly once per window. Replaces the previous per-fact, per-stream
    /// synchronous rebuild-and-publish call that turned every fact
    /// (and every replication re-delivery of it) into an immediate fact-tree
    /// scan on every silo - the scan-storm fixed here (issue #1038).
    /// </summary>
    private async Task RunPartRebuildLoopAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_partRebuildInterval, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            // Reconcile the fact tree against the set of parts already fanned
            // out, but only while a dashboard is attached and on its own
            // (slower) cadence. This is what surfaces parts written by a path
            // that bypasses FederationRouter (a direct SetMany seed raises no
            // FactRouted, so the stream never marks those serials dirty): the
            // pass queues the missing serials, which the drain below then folds
            // and fans out live (issue #1048).
            await MaybeReconcileViewWithTreeAsync(token);

            if (_dirtyParts.IsEmpty)
            {
                continue;
            }

            await DrainDirtyAsync(token);
        }
    }

    /// <summary>
    /// True when at least one circuit is watching a feed the per-part summary
    /// fan-out backs (the part-summary grid or the divergence stream). Marking,
    /// draining, and reconciliation are all gated on this so an idle silo - or
    /// one serving only health-check prerenders - does no periodic tree scans or
    /// folds at all.
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
            _logger.LogWarning(ex, "Dashboard fact-tree reconciliation pass failed");
        }
        finally
        {
            _nextReconcileUtc = DateTime.UtcNow + _reconcileInterval;
        }
    }

    /// <summary>
    /// Scans the fact tree (the source of truth) and marks any part present in
    /// the tree but not yet fanned out live (absent from <see cref="_lastStates"/>)
    /// dirty, up to <see cref="_reconcileBudget"/> per pass. The bounded budget
    /// spreads a large backfill (e.g. a fresh bulk seed) over several cadences
    /// rather than folding every discovered part in one burst. Marking is gated
    /// on <see cref="HasPartWatchers"/> inside <see cref="MarkPartDirty"/>, so a
    /// pass with no watcher queues nothing. Returns the number of parts newly
    /// queued.
    /// </summary>
    private async Task<int> ReconcileViewWithTreeAsync(CancellationToken token)
    {
        // Reconciliation only queues live fan-out for an attached dashboard -
        // the mark is watcher-gated, so without a watcher there is nothing to
        // discover (there is no sample tree to keep warm).
        if (!HasPartWatchers)
        {
            return 0;
        }

        var treeParts = await ResolvePartSerialsAsync(token);
        if (treeParts.Count == 0)
        {
            return 0;
        }

        var marked = 0;
        foreach (var serial in treeParts)
        {
            if (token.IsCancellationRequested || marked >= _reconcileBudget)
            {
                break;
            }

            // Skip parts already fanned out live or already queued by the fact
            // stream - reconciliation only fills the gap left by non-routed
            // writers.
            if (_lastStates.ContainsKey(serial) || _dirtyParts.ContainsKey(serial))
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
    /// and cadence at the loop level) and then drains, so a test can
    /// deterministically fan out parts written directly to the tree (bypassing
    /// <see cref="FederationRouter"/>). Note the <see cref="HasPartWatchers"/>
    /// gate inside <see cref="MarkPartDirty"/> still applies, so a caller must
    /// attach a part subscriber first. Returns the number of parts the pass
    /// newly queued.
    /// </summary>
    internal async Task<int> ReconcileViewWithTreeForTestAsync()
    {
        var marked = await ReconcileViewWithTreeAsync(CancellationToken.None);
        await DrainDirtyAsync(CancellationToken.None);
        return marked;
    }

    /// <summary>
    /// Drains the dirty set once: snapshots the queued serials, clears each as
    /// it is taken, folds its summary (a fresh lattice fold of that one part's
    /// facts joined with its baseline state), and fans it out to the in-memory
    /// per-circuit channels. Marks that arrive during the drain are picked up on
    /// the next tick. The library-maintained folded view owns durable
    /// materialisation, so the drain is pure in-memory fan-out with no durable
    /// write of its own.
    /// </summary>
    private async Task DrainDirtyAsync(CancellationToken token)
    {
        var serials = _dirtyParts.Keys.ToArray();
        if (serials.Length == 0)
        {
            return;
        }

        foreach (var serial in serials)
        {
            if (token.IsCancellationRequested)
            {
                return;
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
                continue;
            }

            FanOutPartUpdate(update);
        }
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
    /// Fans one folded <see cref="PartSummaryUpdate"/> out to the in-memory
    /// per-circuit channels: the part-summary subscribers and any derived
    /// divergence transition. Pure in-memory work (no I/O) - the
    /// library-maintained folded view owns durable materialisation off the WAL,
    /// so the drain no longer writes a summary row of its own. Never throws: a
    /// channel write failure is logged and swallowed so one bad subscriber
    /// cannot stall the drain.
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

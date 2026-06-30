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
    /// gated on having subscribers inside <see cref="PublishPartAsync"/>, so
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
    internal Task DrainDirtyForTestAsync() => DrainDirtyAsync(CancellationToken.None);

    /// <summary>
    /// Background loop that drains <see cref="_dirtyParts"/> once per
    /// the configured rebuild interval, rebuilding each dirty part's summary
    /// exactly once per window. Replaces the previous per-fact, per-stream
    /// synchronous <see cref="PublishPartAsync"/> call that turned every fact
    /// (and every replication re-delivery of it) into an immediate fact-tree
    /// scan on every silo - the scan-storm fixed here.
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

            if (_dirtyParts.IsEmpty)
            {
                continue;
            }

            await DrainDirtyAsync(token);
        }
    }

    /// <summary>
    /// Drains the dirty set once: snapshots the queued serials, clears each as
    /// it is taken, and rebuilds its summary. Marks that arrive during the
    /// drain are picked up on the next tick.
    /// </summary>
    private async Task DrainDirtyAsync(CancellationToken token)
    {
        foreach (var serial in _dirtyParts.Keys.ToArray())
        {
            if (token.IsCancellationRequested)
            {
                return;
            }
            _dirtyParts.TryRemove(serial, out _);
            await PublishPartAsync(serial);
        }
    }

    /// <summary>
    /// Subscribed to <see cref="PartCrdtStore.PartChanged"/> in
    /// <see cref="StartAsync"/>. Forwards the carried serial onto the
    /// cluster-wide part-change stream so every silo's broadcaster -
    /// including this one - re-runs the per-circuit fan-out
    /// (<see cref="PublishPartAsync"/>) for whichever Blazor sessions
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

    private async Task PublishPartAsync(PartSerialNumber serial)
    {
        try
        {
            var update = await BuildSummaryAsync(serial, CancellationToken.None);

            // Maintain the materialised per-part summary view regardless of
            // subscribers, so the dashboard snapshot (which reads this view in
            // a single scan) is always current. This is the read-model that
            // replaces the old per-render full re-fold of the fact tree.
            await _summaryView.UpsertAsync(update, CancellationToken.None);

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
            _logger.LogWarning(ex, "Failed to build dashboard update for serial {Serial}", serial.Value);
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

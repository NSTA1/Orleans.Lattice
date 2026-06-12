using Orleans.Lattice;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The scoped bootstrap-snapshot fallback engine. When a targeted leaf re-replay
/// reports the local write-ahead-log has been garbage-collected past the
/// divergence point, this engine re-derives the committed projection of just the
/// divergent leaf ranges from the live tree (via the range-scoped
/// <see cref="ISnapshotProvider"/> export, which is immune to WAL trimming) and
/// re-ships those committed entries to the diverged peer through the supplied
/// sink so the repair travels the ordinary causal-stable apply pipeline.
/// <para>
/// Re-shipped entries carry their committed-projection clock verbatim and are
/// deduplicated at the receiver on <c>(originClusterId, hlc)</c>, so re-sending
/// is idempotent. The pass re-ships committed projection rows only: prepared
/// (not-yet-decided) saga rows and tombstoned keys are skipped, since the
/// committed projection already reflects every decided value.
/// </para>
/// </summary>
internal static class BootstrapFallbackPlanner
{
    /// <summary>
    /// Exports the scoped snapshot of <paramref name="ranges"/> from the local
    /// <paramref name="snapshotProvider"/>, converts the committed-projection
    /// rows to <see cref="WalRecord"/> sets, and re-ships them to
    /// <paramref name="peer"/> bounded by the configured caps. Emits the
    /// <see cref="LatticeReplicationMetrics.BootstrapFallbackTriggered"/>,
    /// <see cref="LatticeReplicationMetrics.BootstrapFallbackEntries"/>, and
    /// <see cref="LatticeReplicationMetrics.BootstrapFallbackSkipped"/> counters.
    /// </summary>
    /// <param name="treeName">The logical replicated-tree name.</param>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="originClusterId">The local (sending) cluster id stamped on the re-shipped entries.</param>
    /// <param name="ranges">The localised divergent leaf covering ranges to scope the snapshot to.</param>
    /// <param name="snapshotProvider">The local snapshot provider the scoped export reads from.</param>
    /// <param name="sink">The re-ship sink.</param>
    /// <param name="maxEntries">Soft cap on committed entries re-shipped per pass; always ships at least one.</param>
    /// <param name="maxBytes">Soft cap on the estimated re-shipped payload bytes per pass; always ships at least one.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The outcome of the pass.</returns>
    public static async Task<BootstrapFallbackOutcome> PlanAsync(
        string treeName,
        string peer,
        string originClusterId,
        IReadOnlyList<LeafReReplayRange> ranges,
        ISnapshotProvider snapshotProvider,
        ILeafReReplaySink sink,
        int maxEntries,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(originClusterId);
        ArgumentNullException.ThrowIfNull(ranges);
        ArgumentNullException.ThrowIfNull(snapshotProvider);
        ArgumentNullException.ThrowIfNull(sink);

        if (ranges.Count == 0)
        {
            RecordSkipped(treeName, peer, BootstrapFallbackSkipReason.RangeEmpty);
            return new BootstrapFallbackOutcome { SkipReason = BootstrapFallbackSkipReason.RangeEmpty };
        }

        RecordTriggered(treeName, peer);

        var stream = await snapshotProvider
            .ExportAsync(treeName, ranges, HybridLogicalClock.Zero, cancellationToken)
            .ConfigureAwait(false);

        var chosen = new List<WalRecord>();
        long bytes = 0;
        await foreach (var entry in stream.Entries.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            // Committed projection rows only: skip prepared (not-yet-decided)
            // saga rows, tombstones, and rows with no value to ship.
            if (entry.IsPrepared || entry.IsTombstone || entry.Value is null)
            {
                continue;
            }

            var record = new WalRecord
            {
                TreeId = treeName,
                Op = MutationKind.Set,
                Key = entry.Key,
                Value = entry.Value,
                Timestamp = entry.Timestamp,
                OriginClusterId = originClusterId,
                ExpiresAtTicks = entry.ExpiresAtTicks,
            };

            var est = EstimateBytes(record);

            // Always ship the first entry (list empty); thereafter stop once the
            // next entry would breach either soft cap. The remaining divergence
            // is repaired on the next cadence.
            if (chosen.Count > 0 && (chosen.Count + 1 > maxEntries || bytes + est > maxBytes))
            {
                break;
            }

            chosen.Add(record);
            bytes += est;
        }

        if (chosen.Count == 0)
        {
            RecordSkipped(treeName, peer, BootstrapFallbackSkipReason.Empty);
            return new BootstrapFallbackOutcome
            {
                RangesProcessed = ranges.Count,
                SkipReason = BootstrapFallbackSkipReason.Empty,
            };
        }

        var shipped = await sink.ReplayAsync(peer, treeName, chosen, cancellationToken).ConfigureAwait(false);
        if (shipped > 0)
        {
            RecordEntries(treeName, peer, shipped);
        }

        return new BootstrapFallbackOutcome
        {
            Attempted = true,
            RangesProcessed = ranges.Count,
            EntriesShipped = shipped,
            SkipReason = BootstrapFallbackSkipReason.None,
        };
    }

    // The byte cap is applied to a cheap estimate of each entry's payload size
    // (value + key plus a fixed framing allowance), not the exact encoded wire
    // bytes, because selection runs on materialised records before any encode.
    // The estimate is a deliberate over-approximation so the cap never
    // under-counts and over-ships.
    private static long EstimateBytes(in WalRecord r)
        => (r.Value?.Length ?? 0) + (r.Key?.Length ?? 0) + 64;

    private static void RecordTriggered(string tree, string peer) =>
        LatticeReplicationMetrics.BootstrapFallbackTriggered.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer));

    private static void RecordEntries(string tree, string peer, int count) =>
        LatticeReplicationMetrics.BootstrapFallbackEntries.Add(
            count,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer));

    private static void RecordSkipped(string tree, string peer, BootstrapFallbackSkipReason reason) =>
        LatticeReplicationMetrics.BootstrapFallbackSkipped.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer),
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagReason,
                LatticeReplicationMetrics.BootstrapFallbackSkipReasonTag(reason)));
}

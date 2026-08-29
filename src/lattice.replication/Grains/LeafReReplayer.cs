using Orleans.Lattice;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The targeted leaf re-replay repair engine. Given the cluster-stable covering
/// ranges of the leaves a read-only Merkle walk localised as diverging, plus
/// the diverged peer's high-water-mark cursor, it selects the retained
/// write-ahead-log entries that fall inside those ranges and sit strictly above
/// the cursor, then re-ships them through the supplied sink so the repair
/// travels the ordinary causal-stable apply pipeline. Atomic-batch boundaries
/// are respected: if any member of an atomic batch is selected, every retained
/// sibling of that batch ships with it, and the entry/byte caps are applied as
/// whole units so a batch is never split.
/// <para>
/// Re-shipped entries carry their source clock verbatim and are deduplicated at
/// the receiver on <c>(originClusterId, hlc)</c>, so re-sending is idempotent.
/// </para>
/// </summary>
internal static class LeafReReplayer
{
    /// <summary>
    /// Selects and re-ships the retained WAL entries covering the localised leaf
    /// ranges to <paramref name="peer"/>, bounded by the peer's cursor and the
    /// configured caps. Emits the
    /// <see cref="LatticeReplicationMetrics.LeafReReplayEntries"/> /
    /// <see cref="LatticeReplicationMetrics.LeafReReplaySkipped"/> counters.
    /// </summary>
    /// <param name="treeName">The logical replicated-tree name.</param>
    /// <param name="peer">The diverged peer cluster id.</param>
    /// <param name="originClusterId">The local (sending) cluster id whose entries are eligible.</param>
    /// <param name="ranges">The localised leaf covering ranges.</param>
    /// <param name="peerCursor">The peer's high-water-mark for this origin; only entries strictly above it are re-shipped.</param>
    /// <param name="walSource">The read-only WAL source.</param>
    /// <param name="sink">The re-ship sink.</param>
    /// <param name="maxEntries">Soft cap on entries re-shipped per pass; never splits an atomic batch.</param>
    /// <param name="maxBytes">Soft cap on the estimated re-shipped payload bytes per pass; never splits an atomic batch.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The outcome of the pass.</returns>
    public static async Task<LeafReReplayOutcome> ReplayAsync(
        string treeName,
        string peer,
        string originClusterId,
        IReadOnlyList<LeafReReplayRange> ranges,
        HybridLogicalClock peerCursor,
        IWalReReplaySource walSource,
        ILeafReReplaySink sink,
        int maxEntries,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(peer);
        ArgumentNullException.ThrowIfNull(originClusterId);
        ArgumentNullException.ThrowIfNull(ranges);
        ArgumentNullException.ThrowIfNull(walSource);
        ArgumentNullException.ThrowIfNull(sink);

        if (ranges.Count == 0)
        {
            RecordSkipped(treeName, peer, LeafReReplaySkipReason.RangeEmpty);
            return new LeafReReplayOutcome { SkipReason = LeafReReplaySkipReason.RangeEmpty };
        }

        var read = await walSource.ReadAsync(cancellationToken).ConfigureAwait(false);

        if (read.WasTrimmed && read.OldestRetainedHlc > peerCursor)
        {
            // The local WAL has been garbage-collected past the divergence
            // point: the missing entries are gone, so the repair cannot proceed.
            // Emit the operator-only alert and fall back without attempting
            // repair; a bootstrap-snapshot remediation (issue #517) is the
            // follow-up.
            RecordSkipped(treeName, peer, LeafReReplaySkipReason.WalTrimmed);
            return new LeafReReplayOutcome { SkipReason = LeafReReplaySkipReason.WalTrimmed };
        }

        var entries = read.Entries;
        if (entries is null || entries.Count == 0)
        {
            RecordSkipped(treeName, peer, LeafReReplaySkipReason.RangeEmpty);
            return new LeafReReplayOutcome
            {
                RangesProcessed = ranges.Count,
                SkipReason = LeafReReplaySkipReason.RangeEmpty,
            };
        }

        // 1. Base candidates: same origin, strictly above the peer's cursor, and
        //    the key falls inside at least one localised leaf range.
        var selected = new bool[entries.Count];
        HashSet<Guid>? batchTxIds = null;
        var anyBase = false;
        for (var i = 0; i < entries.Count; i++)
        {
            var e = entries[i];
            if (!string.Equals(e.OriginClusterId, originClusterId, StringComparison.Ordinal))
            {
                continue;
            }
            if (!(e.Timestamp > peerCursor))
            {
                continue;
            }
            if (!InAnyRange(e.Key, ranges))
            {
                continue;
            }
            selected[i] = true;
            anyBase = true;
            if (e.AtomicBatchSize > 0 && e.TransactionId != Guid.Empty)
            {
                batchTxIds ??= [];
                batchTxIds.Add(e.TransactionId);
            }
        }

        if (!anyBase)
        {
            RecordSkipped(treeName, peer, LeafReReplaySkipReason.RangeEmpty);
            return new LeafReReplayOutcome
            {
                RangesProcessed = ranges.Count,
                SkipReason = LeafReReplaySkipReason.RangeEmpty,
            };
        }

        // 2. Pull in every retained sibling row of a partially-selected atomic
        //    batch (same origin, same transaction id) so the batch ships whole
        //    and is never split across the cap boundary.
        if (batchTxIds is { Count: > 0 })
        {
            for (var i = 0; i < entries.Count; i++)
            {
                if (selected[i])
                {
                    continue;
                }
                var e = entries[i];
                if (e.TransactionId != Guid.Empty
                    && batchTxIds.Contains(e.TransactionId)
                    && string.Equals(e.OriginClusterId, originClusterId, StringComparison.Ordinal))
                {
                    selected[i] = true;
                }
            }
        }

        // 3. Collect the selected entries in (Timestamp, AtomicBatchIndex) order.
        var chosen = new List<WalRecord>();
        for (var i = 0; i < entries.Count; i++)
        {
            if (selected[i])
            {
                chosen.Add(entries[i]);
            }
        }
        chosen.Sort(static (a, b) =>
        {
            var c = a.Timestamp.CompareTo(b.Timestamp);
            return c != 0 ? c : a.AtomicBatchIndex.CompareTo(b.AtomicBatchIndex);
        });

        // 4. Apply the soft caps as whole units (single entries or complete
        //    atomic batches). Always ship at least one unit; never split a batch.
        var capped = ApplyCaps(chosen, maxEntries, maxBytes);
        if (capped.Count == 0)
        {
            RecordSkipped(treeName, peer, LeafReReplaySkipReason.RangeEmpty);
            return new LeafReReplayOutcome
            {
                RangesProcessed = ranges.Count,
                SkipReason = LeafReReplaySkipReason.RangeEmpty,
            };
        }

        var shipped = await sink.ReplayAsync(peer, treeName, capped, cancellationToken).ConfigureAwait(false);
        if (shipped > 0)
        {
            RecordEntries(treeName, peer, shipped);
        }

        return new LeafReReplayOutcome
        {
            Attempted = true,
            RangesProcessed = ranges.Count,
            EntriesReReplayed = shipped,
            SkipReason = LeafReReplaySkipReason.None,
        };
    }

    private static List<WalRecord> ApplyCaps(List<WalRecord> chosen, int maxEntries, long maxBytes)
    {
        // Group atomic-batch members by transaction id so a batch is emitted as
        // a single unit at the position of its earliest entry.
        Dictionary<Guid, List<WalRecord>>? batches = null;
        foreach (var e in chosen)
        {
            if (e.AtomicBatchSize > 0 && e.TransactionId != Guid.Empty)
            {
                batches ??= [];
                if (!batches.TryGetValue(e.TransactionId, out var list))
                {
                    list = [];
                    batches[e.TransactionId] = list;
                }
                list.Add(e);
            }
        }

        var result = new List<WalRecord>(chosen.Count);
        HashSet<Guid>? emittedTx = batches is null ? null : [];
        long bytes = 0;
        foreach (var e in chosen)
        {
            var isAtomic = e.AtomicBatchSize > 0 && e.TransactionId != Guid.Empty;
            int unitCount;
            long unitBytes;
            List<WalRecord>? batch = null;
            if (isAtomic)
            {
                if (!emittedTx!.Add(e.TransactionId))
                {
                    continue; // This batch was already emitted at its first member.
                }
                batch = batches![e.TransactionId];
                unitCount = batch.Count;
                unitBytes = 0;
                foreach (var u in batch)
                {
                    unitBytes += EstimateBytes(u);
                }
            }
            else
            {
                unitCount = 1;
                unitBytes = EstimateBytes(e);
            }

            // Always ship the first unit (result empty); thereafter stop adding
            // once a unit would breach either soft cap. Ships a timestamp-ordered
            // prefix - the remaining divergence is repaired on the next cadence
            // as the peer's cursor advances.
            if (result.Count > 0 && (result.Count + unitCount > maxEntries || bytes + unitBytes > maxBytes))
            {
                break;
            }

            if (batch is not null)
            {
                result.AddRange(batch);
            }
            else
            {
                result.Add(e);
            }
            bytes += unitBytes;
        }

        return result;
    }

    private static bool InAnyRange(string? key, IReadOnlyList<LeafReReplayRange> ranges)
    {
        for (var i = 0; i < ranges.Count; i++)
        {
            if (ranges[i].Contains(key))
            {
                return true;
            }
        }
        return false;
    }

    // The byte cap is applied to a cheap estimate of each entry's payload size
    // (value + delta + key plus a fixed framing allowance), not the exact
    // encoded wire bytes, because selection runs on materialised records before
    // any encode. The estimate is a deliberate over-approximation so the cap
    // never under-counts and over-ships.
    private static long EstimateBytes(in WalRecord r)
        => (r.Value?.Length ?? 0) + (r.Delta?.Length ?? 0) + (r.Key?.Length ?? 0) + 64;

    private static void RecordEntries(string tree, string peer, int count) =>
        LatticeReplicationMetrics.LeafReReplayEntries.Add(
            count,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer),
            LatticeTenantLabel.ForTree(tree));

    private static void RecordSkipped(string tree, string peer, LeafReReplaySkipReason reason) =>
        LatticeReplicationMetrics.LeafReReplaySkipped.Add(
            1,
            new System.Diagnostics.TagList
            {
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, tree),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peer),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, LatticeReplicationMetrics.LeafReReplaySkipReasonTag(reason)),
                LatticeTenantLabel.ForTree(tree),
            });
}

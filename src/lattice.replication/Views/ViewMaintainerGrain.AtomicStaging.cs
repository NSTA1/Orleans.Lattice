using System.Linq;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Atomic-write staging path of the view maintainer (Phase 4). Restores the
/// single-tree all-or-nothing, not-visible-until-committed guarantee for source
/// <c>SetManyAtomicAsync</c> sagas, which Phase 1 simply skipped.
/// <para>
/// <b>Staging buffer.</b> A saga writes each key <c>IsPrepared=true</c> under a
/// shared <see cref="LatticeMutation.TransactionId"/> and appends it to the WAL
/// <b>before</b> the per-shard <see cref="MutationKind.TxCommit"/> /
/// <see cref="MutationKind.TxAbort"/> terminals. The maintainer (one cluster-wide
/// activation tailing every WAL partition) buffers every prepared entry by
/// transaction id rather than applying it. A <see cref="MutationKind.TxCommit"/>
/// terminal tallies the distinct committed shard (its
/// <see cref="LatticeMutation.ShardIndex"/>) and raises the expected shard count
/// to <c>max(seen, AtomicShardCount)</c> (late-discovered shards make the count
/// non-decreasing). When every expected shard terminal has arrived <b>and</b> the
/// staged prepares satisfy <see cref="LatticeMutation.AtomicBatchSize"/>, the
/// whole batch is flushed to the view tree atomically through
/// <see cref="ILattice.SetManyAtomicAsync(System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, byte[]}}, string, System.Threading.CancellationToken)"/>
/// with a deterministic operation id derived from the source transaction id (so a
/// replay re-attaches to the completed view saga and applies nothing). A
/// <see cref="MutationKind.TxAbort"/> terminal discards the buffer entry - its
/// writes are never surfaced.
/// </para>
/// <para>
/// <b>Checkpoint invariant.</b> The persisted per-partition resume offset is held
/// back to <c>min(contiguous-applied offset, lowest-still-staged offset - 1)</c>
/// so a restart re-reads and re-stages an incomplete batch and can never skip an
/// un-applied prepared (or unresolved terminal) entry. Staging itself is not
/// persisted; it is rebuilt idempotently from the held-back replay.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    private const int ResolvedCapacity = 8192;

    // Single cluster-wide activation: one buffer suffices for a saga whose
    // prepares and per-shard terminals are spread across partition cursors.
    private readonly Dictionary<Guid, StagedTransaction> _staging = new();

    // Bounded set of transaction ids already committed-and-applied or aborted
    // in this activation, so a late prepare/terminal that the held-back
    // checkpoint re-reads (or a sibling read later in the same pass than its
    // abort terminal) is dropped rather than re-staged into a phantom batch.
    private readonly HashSet<Guid> _resolved = new();
    private readonly Queue<Guid> _resolvedOrder = new();

    /// <summary>How a source mutation is dispositioned by the drain loop.</summary>
    private enum StagingDisposition
    {
        /// <summary>Project and apply immediately (ordinary visible write).</summary>
        Apply,

        /// <summary>Route into the per-transaction staging buffer (prepare or terminal).</summary>
        Stage,

        /// <summary>Ignore (maintenance, or a transactional entry with no usable id).</summary>
        Skip,
    }

    /// <summary>
    /// Classifies a source mutation. Replaces the Phase 1 blanket skip of
    /// prepared entries and transaction terminals with a routing decision: the
    /// terminals and prepared halves now flow into the staging buffer.
    /// </summary>
    private static StagingDisposition Classify(in LatticeMutation mutation, out bool terminalCommit, out bool terminalAbort)
    {
        terminalCommit = false;
        terminalAbort = false;

        if (mutation.Category == MutationCategory.Maintenance)
        {
            return StagingDisposition.Skip;
        }

        if (mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort)
        {
            if (mutation.TransactionId == Guid.Empty)
            {
                return StagingDisposition.Skip;
            }

            terminalCommit = mutation.Kind == MutationKind.TxCommit;
            terminalAbort = !terminalCommit;
            return StagingDisposition.Stage;
        }

        if (mutation.IsPrepared)
        {
            // A prepared entry with no transaction id cannot be reassembled into
            // a batch; never expose it (it has no terminal to make it visible).
            return mutation.TransactionId == Guid.Empty ? StagingDisposition.Skip : StagingDisposition.Stage;
        }

        return StagingDisposition.Apply;
    }

    /// <summary>
    /// Records a prepared entry or a transaction terminal into the staging
    /// buffer and appends the transaction id to <paramref name="completed"/> when
    /// the batch becomes ready to flush. Late entries for an already-resolved
    /// transaction are dropped.
    /// </summary>
    private void HandleStagingEntry(
        in LatticeMutation mutation,
        int partition,
        long offset,
        bool terminalCommit,
        bool terminalAbort,
        List<Guid> completed)
    {
        var txId = mutation.TransactionId;
        if (_resolved.Contains(txId))
        {
            return;
        }

        if (terminalAbort)
        {
            // The batch is never exposed: discard any staged prepares and
            // remember the id so a sibling prepare read later in this pass (or
            // re-read after a restart) is not re-staged into a phantom batch.
            _staging.Remove(txId);
            MarkResolved(txId);
            return;
        }

        if (!_staging.TryGetValue(txId, out var tx))
        {
            tx = new StagedTransaction();
            _staging[txId] = tx;
        }

        tx.NoteOffset(partition, offset);

        if (terminalCommit)
        {
            tx.TerminalSeen = true;
            if (mutation.AtomicShardCount > tx.ExpectedShardCount)
            {
                tx.ExpectedShardCount = mutation.AtomicShardCount;
            }

            tx.CommittedShards.Add(mutation.ShardIndex);

            // Cross-tree coupling: a cross-tree atomic write stamps its
            // coordinator key and canonical participant source-tree set on the
            // sub-saga terminals. Capturing them here flags the batch for the
            // joint-flip path instead of an immediate single-tree flip.
            if (mutation.CrossTreeOperationId is not null)
            {
                tx.CrossTreeOperationId = mutation.CrossTreeOperationId;
                tx.CrossTreeParticipants = mutation.CrossTreeParticipants;
            }
        }
        else
        {
            tx.StagePrepare(mutation);
        }

        if (IsComplete(tx) && !completed.Contains(txId))
        {
            completed.Add(txId);
        }
    }

    /// <summary>
    /// A staged batch is ready to flush when every expected shard terminal has
    /// arrived and the staged prepares cover the whole batch. When the terminal
    /// carries no shard count (legacy / unit path, <c>AtomicShardCount == 0</c>)
    /// the gate falls back to "complete on the first terminal", which is still
    /// safe because prepare completeness independently proves every key arrived.
    /// </summary>
    private static bool IsComplete(StagedTransaction tx)
    {
        if (!tx.TerminalSeen)
        {
            return false;
        }

        var terminalsComplete = tx.ExpectedShardCount == 0
            || tx.CommittedShards.Count >= tx.ExpectedShardCount;
        if (!terminalsComplete)
        {
            return false;
        }

        return tx.AtomicBatchSize > 0 && tx.PreparesByIndex.Count >= tx.AtomicBatchSize;
    }

    private void MarkResolved(Guid txId)
    {
        if (!_resolved.Add(txId))
        {
            return;
        }

        _resolvedOrder.Enqueue(txId);
        while (_resolvedOrder.Count > ResolvedCapacity)
        {
            _resolved.Remove(_resolvedOrder.Dequeue());
        }
    }

    /// <summary>
    /// Lowest offset, across every still-staged transaction, of any entry that
    /// landed on <paramref name="partition"/>; <see cref="long.MaxValue"/> when
    /// nothing is staged on the partition. The persisted resume offset is held
    /// back to one below this so a restart re-reads the unresolved batch.
    /// </summary>
    private long HeldFloorForPartition(int partition)
    {
        var floor = long.MaxValue;
        foreach (var tx in _staging.Values)
        {
            if (tx.MinOffsetByPartition.TryGetValue(partition, out var off) && off < floor)
            {
                floor = off;
            }
        }

        return floor;
    }

    /// <summary>
    /// Applies the per-partition checkpoint hold-back to
    /// <paramref name="advancedOffsets"/>: each partition's persisted offset is
    /// clamped to one below the lowest still-staged offset on that partition.
    /// </summary>
    private void ApplyCheckpointHoldBack(Dictionary<int, long> advancedOffsets)
    {
        if (_staging.Count == 0)
        {
            return;
        }

        foreach (var partition in advancedOffsets.Keys.ToList())
        {
            var floor = HeldFloorForPartition(partition);
            if (floor != long.MaxValue && floor - 1 < advancedOffsets[partition])
            {
                advancedOffsets[partition] = floor - 1;
            }
        }
    }

    /// <summary>
    /// The HLC of the oldest still-staged prepared entry across every in-flight
    /// transaction, reported as the WAL-GC blocked-floor pin so the source log is
    /// not trimmed under the staged prepares; <see langword="null"/> when nothing
    /// is staged (which clears any prior pin).
    /// </summary>
    private HybridLogicalClock? ComputeBlockedAtHlc()
    {
        HybridLogicalClock? floor = null;
        foreach (var tx in _staging.Values)
        {
            if (tx.HasOldestPreparedHlc && (floor is null || tx.OldestPreparedHlc < floor.Value))
            {
                floor = tx.OldestPreparedHlc;
            }
        }

        return floor;
    }

    /// <summary>
    /// Returns <see langword="true"/> (and clears the buffer) when the staging
    /// buffer has exceeded its configured bound, or an un-terminated batch's
    /// blocked-floor pin would sink below the source WAL retention ceiling and so
    /// cannot complete before the log trims under it. The caller responds by
    /// forcing a <see cref="RebuildAsync"/>, which reconverges the view from
    /// current committed source state (excluding the still-uncommitted prepares).
    /// </summary>
    private bool StagingBackstopTripped(LatticeViewOptions options, TimeSpan? walRetention)
    {
        if (_staging.Count == 0)
        {
            return false;
        }

        var maxTx = options.MaxStagedTransactions > 0
            ? options.MaxStagedTransactions
            : LatticeViewOptions.DefaultMaxStagedTransactions;
        var maxBytes = options.MaxStagedBytes > 0
            ? options.MaxStagedBytes
            : LatticeViewOptions.DefaultMaxStagedBytes;

        if (_staging.Count > maxTx)
        {
            logger.LogWarning(
                "View '{ViewName}' atomic staging buffer exceeded the transaction cap ({Count} > {Cap}); falling back to a rebuild.",
                ViewName, _staging.Count, maxTx);
            return TripBackstop();
        }

        long stagedBytes = 0;
        foreach (var tx in _staging.Values)
        {
            stagedBytes += tx.StagedBytes;
        }

        if (stagedBytes > maxBytes)
        {
            logger.LogWarning(
                "View '{ViewName}' atomic staging buffer exceeded the byte cap ({Bytes} > {Cap}); falling back to a rebuild.",
                ViewName, stagedBytes, maxBytes);
            return TripBackstop();
        }

        if (walRetention is { } retention && retention > TimeSpan.Zero)
        {
            var blocked = ComputeBlockedAtHlc();
            if (blocked is { } pin)
            {
                var age = DateTime.UtcNow.Ticks - pin.WallClockTicks;
                if (age > retention.Ticks)
                {
                    logger.LogWarning(
                        "View '{ViewName}' has an un-terminated atomic batch whose blocked-floor pin is older than the source WAL retention ceiling ({Retention}); falling back to a rebuild.",
                        ViewName, retention);
                    return TripBackstop();
                }
            }
        }

        return false;
    }

    private bool TripBackstop()
    {
        ViewAtomicStagingBackstop.Add(1, ViewTag);
        _staging.Clear();
        return true;
    }

    /// <summary>
    /// The source tree's configured WAL retention ceiling, used by the staging
    /// backstop to decide whether an un-terminated batch can still be held.
    /// </summary>
    private Task<TimeSpan?> GetSourceWalRetentionAsync(string sourceTreeId)
        => Task.FromResult(latticeOptions.Get(sourceTreeId).WalRetention);

    /// <summary>
    /// Flushes every atomic batch in <paramref name="completed"/> that is still
    /// staged: projects its prepared entries through the filter / re-key
    /// projection, coalesces by view key (last-writer-wins on the source HLC),
    /// and applies the survivors to the view tree atomically. Upserts flip
    /// through <see cref="ILattice.SetManyAtomicAsync(System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, byte[]}}, string, System.Threading.CancellationToken)"/>
    /// keyed by a deterministic operation id derived from the source transaction
    /// id, so a replay re-attaches to the completed view saga and never
    /// double-applies. Retractions (filter-exit / value-drop deletes) follow.
    /// </summary>
    private async Task<int> FlushCompletedFilterBatchesAsync(
        ILattice viewTree,
        ViewRegistration registration,
        List<Guid> completed,
        CancellationToken cancellationToken)
    {
        var applied = 0;
        foreach (var txId in completed)
        {
            if (!_staging.TryGetValue(txId, out var tx))
            {
                continue;
            }

            var writes = new List<ViewWrite>();
            foreach (var prepared in tx.PreparesByIndex.Values)
            {
                foreach (var write in registration.Projection!.Project(prepared))
                {
                    writes.Add(write);
                }
            }

            var upserts = new List<KeyValuePair<string, byte[]>>();
            var deletes = new List<string>();
            foreach (var write in ViewWriteCoalescer.Coalesce(writes))
            {
                switch (write.Kind)
                {
                    case ViewWriteKind.Upsert:
                        upserts.Add(new KeyValuePair<string, byte[]>(write.Key, write.Value!));
                        break;
                    case ViewWriteKind.Delete:
                        deletes.Add(write.Key);
                        break;
                    default:
                        // A prepared mutation is always a Set, so its projection
                        // is only ever an Upsert or a retracting Delete.
                        break;
                }
            }

            if (upserts.Count == 0 && deletes.Count == 0 && tx.CrossTreeOperationId is null)
            {
                _staging.Remove(txId);
                MarkResolved(txId);
                continue;
            }

            // Cross-tree atomic batch: do not flip this slice into the view tree
            // immediately - rendezvous with the view-side coordinator so every
            // participant view flips jointly. A participant view with an empty
            // slice still registers (empty upserts) so the coordinator is not left
            // waiting on it. While the joint decision is pending the batch stays
            // staged (holding the checkpoint back) and is retried on a later drain.
            if (tx.CrossTreeOperationId is not null)
            {
                var resolved = await HandleCrossTreeBatchAsync(viewTree, txId, tx, upserts, deletes, cancellationToken);
                if (resolved)
                {
                    applied += upserts.Count + deletes.Count;
                    _staging.Remove(txId);
                    MarkResolved(txId);
                }

                continue;
            }

            if (upserts.Count > 0)
            {
                await viewTree.SetManyAtomicAsync(upserts, ViewSagaOperationId(txId), cancellationToken);
                applied += upserts.Count;
            }

            foreach (var key in deletes)
            {
                await viewTree.DeleteAsync(key, cancellationToken);
                applied++;
            }

            _staging.Remove(txId);
            MarkResolved(txId);
        }

        return applied;
    }

    /// <summary>
    /// Deterministic view-tree saga operation id for a source transaction, so a
    /// replay of the same committed batch re-attaches to the completed view saga
    /// (idempotent) rather than minting a fresh one.
    /// </summary>
    private static string ViewSagaOperationId(Guid txId) => $"mv-tx-{txId:N}";

    /// <summary>
    /// One in-flight atomic-write transaction's staged state. Reassembled from
    /// the WAL on every drain; never persisted (the held-back checkpoint replays
    /// it idempotently after a restart).
    /// </summary>
    private sealed class StagedTransaction
    {
        /// <summary>Prepared entries keyed by <see cref="LatticeMutation.AtomicBatchIndex"/> (re-stage replaces, so replay is idempotent).</summary>
        public Dictionary<int, LatticeMutation> PreparesByIndex { get; } = new();

        /// <summary>Distinct committed shard indices observed via <see cref="MutationKind.TxCommit"/> terminals.</summary>
        public HashSet<int> CommittedShards { get; } = new();

        /// <summary>Lowest WAL offset seen for this transaction per partition, driving the checkpoint hold-back.</summary>
        public Dictionary<int, long> MinOffsetByPartition { get; } = new();

        /// <summary>Total batch key count (<see cref="LatticeMutation.AtomicBatchSize"/>), learned from the prepares.</summary>
        public int AtomicBatchSize { get; private set; }

        /// <summary>Expected distinct shard-terminal count (<c>max</c> over the terminals' <see cref="LatticeMutation.AtomicShardCount"/>).</summary>
        public int ExpectedShardCount { get; set; }

        /// <summary><see langword="true"/> once at least one <see cref="MutationKind.TxCommit"/> terminal has arrived.</summary>
        public bool TerminalSeen { get; set; }

        /// <summary>Buffered payload octets (key + value) across the staged prepares, for the byte-cap backstop.</summary>
        public long StagedBytes { get; private set; }

        /// <summary>HLC of the oldest staged prepared entry, for the blocked-floor pin.</summary>
        public HybridLogicalClock OldestPreparedHlc { get; private set; }

        /// <summary><see langword="true"/> once at least one prepared entry is staged.</summary>
        public bool HasOldestPreparedHlc { get; private set; }

        /// <summary>
        /// The cross-tree atomic-write coordinator key carried by the batch's
        /// terminals, or <see langword="null"/> for an ordinary single-tree batch.
        /// Non-null routes the completed batch through the joint-flip path.
        /// </summary>
        public string? CrossTreeOperationId { get; set; }

        /// <summary>
        /// The canonical participant <i>source</i> tree-id set of the cross-tree
        /// atomic write, used to compute this view's joint-flip wait set.
        /// </summary>
        public IReadOnlyList<string>? CrossTreeParticipants { get; set; }

        /// <summary>
        /// Wall-clock ticks at which this view first waited on the cross-tree
        /// coordinator, bounding the readiness wait before degrading to
        /// per-tree-slice atomicity; <c>0</c> until the first wait.
        /// </summary>
        public long CrossTreeFirstSeenTicks { get; set; }

        public void NoteOffset(int partition, long offset)
        {
            if (!MinOffsetByPartition.TryGetValue(partition, out var cur) || offset < cur)
            {
                MinOffsetByPartition[partition] = offset;
            }
        }

        public void StagePrepare(in LatticeMutation mutation)
        {
            if (mutation.AtomicBatchSize > AtomicBatchSize)
            {
                AtomicBatchSize = mutation.AtomicBatchSize;
            }

            var bytes = (long)(mutation.Key?.Length ?? 0) + (mutation.Value?.Length ?? 0);
            if (PreparesByIndex.TryGetValue(mutation.AtomicBatchIndex, out var prev))
            {
                StagedBytes -= (long)(prev.Key?.Length ?? 0) + (prev.Value?.Length ?? 0);
            }

            PreparesByIndex[mutation.AtomicBatchIndex] = mutation;
            StagedBytes += bytes;

            if (!HasOldestPreparedHlc || mutation.Timestamp < OldestPreparedHlc)
            {
                OldestPreparedHlc = mutation.Timestamp;
                HasOldestPreparedHlc = true;
            }
        }
    }
}

using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

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

    // In-activation read position per partition while anything is staged. The
    // persisted resume offset is held back below the lowest staged entry so a
    // restart re-stages the batch, but within the activation the staged entries
    // are already buffered, so the next pass resumes reading where the last one
    // stopped. Resuming from the held-back offset instead re-reads the same
    // bounded window forever whenever the batch's terminal lies more than one
    // drain batch past its prepare on the same partition.
    private readonly Dictionary<int, long> _stagingReadOffsets = new();
    private string? _stagingReadWalTreeId;

    // Replay fence armed from the persisted checkpoint for a pass that re-reads
    // entries an earlier activation already applied or resolved (see
    // ViewCheckpointState.ReplayFenceOffsets); null when no replay is fenced.
    private Dictionary<int, long>? _replayFenceOffsets;
    private HashSet<Guid>? _replayFenceStaged;

    // Bounded set of transaction ids already committed-and-applied or aborted
    // in this activation, so a late prepare/terminal that the held-back
    // checkpoint re-reads (or a sibling read later in the same pass than its
    // abort terminal) is dropped rather than re-staged into a phantom batch.
    private readonly HashSet<Guid> _resolved = new();
    private readonly Queue<Guid> _resolvedOrder = new();
    private string? _resolvedWalTreeId;

    // Per still-staged SOURCE key: how many staged transactions reference it, and
    // the highest HLC of any ordinary (non-prepared) source write seen for that
    // key while it is staged. At flush time a staged prepare whose source key has
    // a recorded ordinary HLC strictly greater than the prepare's own HLC is
    // dropped, reproducing the source-side last-writer-wins outcome (a committed
    // prepared Set installs under its prepare-time HLC, so a higher-HLC ordinary
    // write dominates it at the source and must dominate it in the view too).
    // A flushed batch's prepares are folded in the same way while another staged
    // batch still references the key, so overlapping batches resolve by prepare
    // HLC rather than by the order in which they complete.
    private readonly Dictionary<string, int> _stagedSourceKeyRefCount = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HybridLogicalClock> _ordinaryHlcOverStagedKey = new(StringComparer.Ordinal);

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
            if (_staging.TryGetValue(txId, out var aborted))
            {
                ReleaseStagedKeys(aborted);
                _staging.Remove(txId);
            }

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

            // Refcount the source key so an ordinary write to it seen while it is
            // staged can be recorded for supersession; the per-tx StagedSourceKeys
            // set keeps re-staging the same key (replay) from double-counting.
            if (tx.StagedSourceKeys.Add(mutation.Key))
            {
                _stagedSourceKeyRefCount[mutation.Key] = _stagedSourceKeyRefCount.GetValueOrDefault(mutation.Key) + 1;
            }
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

    /// <summary>
    /// Seeds a pass's completed list with every batch already complete but still
    /// staged (a cross-tree batch awaiting its joint decision), so it is retried
    /// on this pass even though the pass resumes reading past its terminal.
    /// </summary>
    private void SeedStillStagedCompletedTransactions(List<Guid> completed)
    {
        foreach (var (txId, tx) in _staging)
        {
            if (IsComplete(tx))
            {
                completed.Add(txId);
            }
        }
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
    /// Drops <paramref name="tx"/>'s contribution to the staged source-key
    /// refcount; when a key's last staged transaction is released its recorded
    /// ordinary-supersession HLC is forgotten too. Must be called with the live
    /// <see cref="StagedTransaction"/> reference before it is removed from
    /// <see cref="_staging"/>.
    /// </summary>
    private void ReleaseStagedKeys(StagedTransaction tx)
    {
        foreach (var key in tx.StagedSourceKeys)
        {
            if (!_stagedSourceKeyRefCount.TryGetValue(key, out var count))
            {
                continue;
            }

            if (count <= 1)
            {
                _stagedSourceKeyRefCount.Remove(key);
                _ordinaryHlcOverStagedKey.Remove(key);
            }
            else
            {
                _stagedSourceKeyRefCount[key] = count - 1;
            }
        }
    }

    /// <summary>
    /// Records an ordinary (non-prepared, applicable) source write as a candidate
    /// to supersede a still-staged prepare for the same source key, keeping the
    /// highest such HLC. A source key lives on one WAL partition, so a prepare and
    /// any ordinary write to it are read in offset (HLC) order: an ordinary write
    /// read after the prepare is staged necessarily carries a higher HLC and so
    /// correctly supersedes it; one read before the prepare is not yet refcounted
    /// (and carries a lower HLC), so the prepare correctly wins.
    /// </summary>
    private void RecordOrdinaryOverStagedKey(in LatticeMutation mutation)
    {
        if (!_stagedSourceKeyRefCount.ContainsKey(mutation.Key))
        {
            return;
        }

        // Single-probe max-fold: the prior TryGetValue-then-indexer-set pair
        // hashed the source key twice for every ordinary write landing on a
        // still-staged key.
        ref var prev = ref CollectionsMarshal.GetValueRefOrAddDefault(
            _ordinaryHlcOverStagedKey, mutation.Key, out var existed);
        if (!existed || mutation.Timestamp.CompareTo(prev) > 0)
        {
            prev = mutation.Timestamp;
        }
    }

    /// <summary>
    /// Records every prepare of a batch that has just been flushed (or jointly
    /// resolved) as a supersession candidate for the same source key in any
    /// <i>other</i> still-staged batch. The source tree installs overlapping
    /// committed prepares by last-writer-wins on their prepare-time HLC, whichever
    /// commits first, so a batch that completes later but prepared earlier must
    /// not overwrite the newer value in the view. Must run before
    /// <see cref="ReleaseStagedKeys"/> so a key still referenced elsewhere keeps
    /// its recorded HLC; a key referenced only by this batch is forgotten there.
    /// </summary>
    private void RecordFlushedPreparesOverStagedKeys(StagedTransaction tx)
    {
        foreach (var prepared in tx.PreparesByIndex.Values)
        {
            if (!_stagedSourceKeyRefCount.TryGetValue(prepared.Key, out var count) || count <= 1)
            {
                continue;
            }

            ref var prev = ref CollectionsMarshal.GetValueRefOrAddDefault(
                _ordinaryHlcOverStagedKey, prepared.Key, out var existed);
            if (!existed || prepared.Timestamp.CompareTo(prev) > 0)
            {
                prev = prepared.Timestamp;
            }
        }
    }

    /// <summary>
    /// <see langword="true"/> when a recorded ordinary write to
    /// <paramref name="prepared"/>'s source key carries a strictly higher HLC than
    /// the prepare itself, meaning the ordinary write is the source last-writer and
    /// the prepared entry must be skipped at flush time (a lost-write otherwise).
    /// </summary>
    private bool IsSupersededByOrdinary(in LatticeMutation prepared)
        => _ordinaryHlcOverStagedKey.TryGetValue(prepared.Key, out var oh)
            && oh.CompareTo(prepared.Timestamp) > 0;

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
    /// The per-partition offsets a drain pass resumes reading from: the persisted
    /// (held-back) checkpoint, advanced to the in-activation read position while a
    /// batch is still staged against the same physical WAL.
    /// </summary>
    private IReadOnlyDictionary<int, long> DrainReadCheckpoints(string walTreeId, Dictionary<int, long> persisted)
    {
        if (!string.Equals(_resolvedWalTreeId, walTreeId, StringComparison.Ordinal))
        {
            // Scope deduplication to the log actually tailed, not a candidate
            // binding that a failed rebuild may have left in mutable state.
            _resolved.Clear();
            _resolvedOrder.Clear();
            _resolvedWalTreeId = walTreeId;
        }

        if (_staging.Count == 0 || _stagingReadOffsets.Count == 0
            || !string.Equals(_stagingReadWalTreeId, walTreeId, StringComparison.Ordinal))
        {
            _stagingReadOffsets.Clear();
            _stagingReadWalTreeId = null;
            ArmReplayFence(walTreeId);
            return persisted;
        }

        var merged = new Dictionary<int, long>(persisted);
        foreach (var (partition, offset) in _stagingReadOffsets)
        {
            if (offset > merged.GetValueOrDefault(partition, -1))
            {
                merged[partition] = offset;
            }
        }

        return merged;
    }

    /// <summary>
    /// Records where this pass stopped reading (before the checkpoint hold-back)
    /// so the next pass resumes there while a batch stays staged; clears the
    /// position once nothing is staged, when the persisted checkpoint is exact.
    /// </summary>
    private void RecordDrainReadOffsets(
        string walTreeId,
        IReadOnlyDictionary<int, long> readFrom,
        IReadOnlyDictionary<int, long> advanced)
    {
        _stagingReadOffsets.Clear();
        if (_staging.Count == 0)
        {
            _stagingReadWalTreeId = null;
            return;
        }

        _stagingReadWalTreeId = walTreeId;
        foreach (var (partition, offset) in readFrom)
        {
            _stagingReadOffsets[partition] = offset;
        }

        foreach (var (partition, offset) in advanced)
        {
            if (offset > _stagingReadOffsets.GetValueOrDefault(partition, -1))
            {
                _stagingReadOffsets[partition] = offset;
            }
        }
    }

    /// <summary>
    /// Arms the persisted replay fence before a pass that resumes from the
    /// held-back checkpoint with nothing staged in memory (a fresh activation, or
    /// a rebuild that kept the checkpoint), restoring the supersession HLCs the
    /// skipped flushed batches had recorded. The persisted fence always contains
    /// any fence already armed in this activation, so reloading it is safe. A
    /// fence recorded against another physical log is discarded.
    /// </summary>
    private void ArmReplayFence(string walTreeId)
    {
        if (_staging.Count != 0)
        {
            return;
        }

        if (state.State.ReplayFenceOffsets is not { Count: > 0 } offsets)
        {
            _replayFenceOffsets = null;
            _replayFenceStaged = null;
            return;
        }

        if (!string.Equals(state.State.ReplayFenceWalTreeId, walTreeId, StringComparison.Ordinal))
        {
            ClearReplayFence();
            return;
        }

        _replayFenceOffsets = new Dictionary<int, long>(offsets);
        _replayFenceStaged = new HashSet<Guid>(state.State.ReplayFenceStagedTransactions ?? []);
        if (state.State.ReplayFenceSupersession is { } supersession)
        {
            foreach (var (key, hlc) in supersession)
            {
                _ordinaryHlcOverStagedKey[key] = hlc;
            }
        }
    }

    /// <summary>
    /// <see langword="true"/> when an entry was already read by an earlier pass
    /// that persisted the armed replay fence, so it must not be re-applied.
    /// </summary>
    private bool IsReplayFenced(int partition, long offset)
        => _replayFenceOffsets is { } fence
            && fence.TryGetValue(partition, out var fenced)
            && offset <= fenced;

    /// <summary>
    /// <see langword="true"/> when <paramref name="txId"/> was still staged when the
    /// armed replay fence was recorded, so its fenced entries are re-staged.
    /// </summary>
    private bool IsReplayStaged(Guid txId) => _replayFenceStaged?.Contains(txId) == true;

    /// <summary>
    /// Records the replay fence for the coming checkpoint write: the read position
    /// reached (never below a still-unpassed armed fence), the batches still
    /// staged (plus armed batches not yet re-read), and their supersession HLCs.
    /// Clears it once nothing is staged and any armed fence has been passed.
    /// Returns <see langword="true"/> when the persisted fence changed and so must
    /// be written even if the checkpoint did not move.
    /// </summary>
    private bool UpdateReplayFence(
        string walTreeId,
        IReadOnlyDictionary<int, long> readFrom,
        IReadOnlyDictionary<int, long> advanced)
    {
        var position = new Dictionary<int, long>(readFrom);
        foreach (var (partition, offset) in advanced)
        {
            if (offset > position.GetValueOrDefault(partition, -1))
            {
                position[partition] = offset;
            }
        }

        var staged = new HashSet<Guid>(_staging.Keys);
        var supersession = new Dictionary<string, HybridLogicalClock>(_ordinaryHlcOverStagedKey, StringComparer.Ordinal);
        if (_replayFenceOffsets is { } armed)
        {
            var passed = true;
            foreach (var (partition, fenced) in armed)
            {
                if (position.GetValueOrDefault(partition, -1) < fenced)
                {
                    passed = false;
                    position[partition] = fenced;
                }
            }

            if (passed)
            {
                _replayFenceOffsets = null;
                _replayFenceStaged = null;
            }
            else
            {
                foreach (var txId in _replayFenceStaged!)
                {
                    if (!_resolved.Contains(txId))
                    {
                        staged.Add(txId);
                    }
                }

                if (state.State.ReplayFenceSupersession is { } prior)
                {
                    foreach (var (key, hlc) in prior)
                    {
                        if (!supersession.TryGetValue(key, out var live) || hlc.CompareTo(live) > 0)
                        {
                            supersession[key] = hlc;
                        }
                    }
                }
            }
        }

        if (staged.Count == 0)
        {
            return ClearReplayFence();
        }

        var changed = !string.Equals(state.State.ReplayFenceWalTreeId, walTreeId, StringComparison.Ordinal)
            || !SameEntries(state.State.ReplayFenceOffsets, position)
            || !SameEntries(state.State.ReplayFenceSupersession, supersession)
            || state.State.ReplayFenceStagedTransactions is not { } persisted
            || persisted.Count != staged.Count
            || !persisted.TrueForAll(staged.Contains);
        if (!changed)
        {
            return false;
        }

        state.State.ReplayFenceWalTreeId = walTreeId;
        state.State.ReplayFenceOffsets = position;
        state.State.ReplayFenceStagedTransactions = [.. staged];
        state.State.ReplayFenceSupersession = supersession;
        return true;
    }

    /// <summary>
    /// Drops the persisted and armed replay fence; the caller's next checkpoint
    /// write persists the removal. Returns <see langword="true"/> when a persisted
    /// fence was present.
    /// </summary>
    private bool ClearReplayFence()
    {
        _replayFenceOffsets = null;
        _replayFenceStaged = null;
        if (state.State.ReplayFenceOffsets is null && state.State.ReplayFenceWalTreeId is null
            && state.State.ReplayFenceStagedTransactions is null && state.State.ReplayFenceSupersession is null)
        {
            return false;
        }

        state.State.ReplayFenceOffsets = null;
        state.State.ReplayFenceWalTreeId = null;
        state.State.ReplayFenceStagedTransactions = null;
        state.State.ReplayFenceSupersession = null;
        return true;
    }

    private static bool SameEntries<TKey, TValue>(Dictionary<TKey, TValue>? persisted, Dictionary<TKey, TValue> live)
        where TKey : notnull
    {
        if (persisted is null || persisted.Count != live.Count)
        {
            return false;
        }

        foreach (var (key, value) in live)
        {
            if (!persisted.TryGetValue(key, out var other) || !EqualityComparer<TValue>.Default.Equals(value, other))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Applies the per-partition checkpoint hold-back to
    /// <paramref name="advancedOffsets"/>: each partition's persisted offset is
    /// clamped to one below the lowest still-staged offset on that partition.
    /// </summary>
    private void ApplyCheckpointHoldBack(Dictionary<int, long> advancedOffsets, int partitions)
    {
        if (_staging.Count == 0)
        {
            return;
        }

        for (var partition = 0; partition < partitions; partition++)
        {
            if (!advancedOffsets.TryGetValue(partition, out var advanced))
            {
                continue;
            }

            var floor = HeldFloorForPartition(partition);
            if (floor != long.MaxValue && floor - 1 < advanced)
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
        ViewAtomicStagingBackstop.Add(1, ViewTag, ViewTenantTag);
        _staging.Clear();
        _stagedSourceKeyRefCount.Clear();
        _ordinaryHlcOverStagedKey.Clear();
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
                // Supersession skip: an ordinary write to this source key with a
                // higher HLC arrived while the batch was staged, so it is the
                // source last-writer; dropping the prepared entry reproduces the
                // source LWW outcome (otherwise apply-order alone would resurrect
                // the lower-HLC atomic value as a permanent lost write).
                if (!registration.Accumulative && IsSupersededByOrdinary(prepared))
                {
                    continue;
                }

                foreach (var write in registration.Projection!.Project(prepared))
                {
                    writes.Add(write);
                }
            }

            // Coalesce once by view key (LWW on the source HLC), then partition
            // the survivors into upserts and deletes. The two lists partition the
            // coalesced survivors, so each is bounded by the coalesced count;
            // presize both to remove their grow chains on this per-completed-batch
            // path.
            var coalesced = ViewWriteCoalescer.Coalesce(writes);
            var upserts = new List<KeyValuePair<string, byte[]>>(coalesced.Count);
            var deletes = new List<string>(coalesced.Count);
            foreach (var write in coalesced)
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
                ReleaseStagedKeys(tx);
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
                    await ReassertResolvedSliceAsync(viewTree, upserts, deletes, cancellationToken);
                    applied += upserts.Count + deletes.Count;
                    RecordFlushedPreparesOverStagedKeys(tx);
                    ReleaseStagedKeys(tx);
                    _staging.Remove(txId);
                    MarkResolved(txId);
                }

                continue;
            }

            // Filter-path flush: apply the upserts and the retraction deletes
            // inside a SINGLE mixed atomic op so a re-key projection (a row
            // moving from view key A to view key B) flips the upsert at B and
            // the delete at A as one visibility change - no reader ever sees
            // both keys, or neither, at once.
            if (upserts.Count > 0 || deletes.Count > 0)
            {
                await viewTree.SetManyAtomicAsync(upserts, deletes, ViewSagaOperationId(txId), cancellationToken);
                await ReassertResolvedSliceAsync(viewTree, upserts, deletes, cancellationToken);
                applied += upserts.Count + deletes.Count;
            }

            RecordFlushedPreparesOverStagedKeys(tx);
            ReleaseStagedKeys(tx);
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
    /// Re-asserts a resolved batch's view slice after its atomic flip. The flip
    /// is keyed by the deterministic <see cref="ViewSagaOperationId(Guid)"/>, so
    /// when a pass is replayed (a lost flush response retried on the same
    /// activation, or a reactivation resuming below an already-flushed batch)
    /// the flip re-attaches to the completed view saga and writes nothing. The
    /// same replay has already re-applied every older ordinary write to the
    /// batch's keys with a blind point write, so without this step the view
    /// would keep that older value permanently. The maintainer is the view
    /// tree's sole writer and every ordinary write that supersedes the batch
    /// was dropped from <paramref name="upserts"/> and <paramref name="deletes"/>,
    /// so restoring the slice reproduces the source last-writer-wins outcome.
    /// A fresh flip already matches, so this reads and writes nothing further.
    /// </summary>
    private static async Task ReassertResolvedSliceAsync(
        ILattice viewTree,
        List<KeyValuePair<string, byte[]>> upserts,
        List<string> deletes,
        CancellationToken cancellationToken)
    {
        if (upserts.Count == 0 && deletes.Count == 0)
        {
            return;
        }

        var keys = new List<string>(upserts.Count + deletes.Count);
        foreach (var upsert in upserts)
        {
            keys.Add(upsert.Key);
        }

        keys.AddRange(deletes);
        var current = await viewTree.GetManyAsync(keys, cancellationToken);
        foreach (var upsert in upserts)
        {
            if (!current.TryGetValue(upsert.Key, out var value) || !value.AsSpan().SequenceEqual(upsert.Value))
            {
                await viewTree.SetAsync(upsert.Key, upsert.Value, cancellationToken);
            }
        }

        foreach (var key in deletes)
        {
            if (current.ContainsKey(key))
            {
                await viewTree.DeleteAsync(key, cancellationToken);
            }
        }
    }

    /// <summary>
    /// One in-flight atomic-write transaction's staged state. Reassembled from
    /// the WAL on every drain; never persisted (the held-back checkpoint replays
    /// it idempotently after a restart).
    /// </summary>
    private sealed class StagedTransaction
    {
        /// <summary>Prepared entries keyed by <see cref="LatticeMutation.AtomicBatchIndex"/> (re-stage replaces, so replay is idempotent).</summary>
        public Dictionary<int, LatticeMutation> PreparesByIndex { get; } = new();

        /// <summary>Distinct source keys this transaction has staged a prepare for, so the maintainer's per-key staged refcount is replay-safe (re-staging the same key by the same tx must not double-count).</summary>
        public HashSet<string> StagedSourceKeys { get; } = new(StringComparer.Ordinal);

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
            // Single-probe min-fold: the prior TryGetValue-then-indexer-set
            // pair hashed `partition` twice on every staged entry.
            ref var cur = ref CollectionsMarshal.GetValueRefOrAddDefault(
                MinOffsetByPartition, partition, out var existed);
            if (!existed || offset < cur)
            {
                cur = offset;
            }
        }

        public void StagePrepare(in LatticeMutation mutation)
        {
            if (mutation.AtomicBatchSize > AtomicBatchSize)
            {
                AtomicBatchSize = mutation.AtomicBatchSize;
            }

            var bytes = (long)(mutation.Key?.Length ?? 0) + (mutation.Value?.Length ?? 0);

            // Single-probe replace: the prior read (to discount the superseded
            // entry's bytes) and the write hashed the batch index twice per
            // staged prepare. Nothing mutates PreparesByIndex between the ref
            // being taken and its last use, so the ref stays valid.
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(
                PreparesByIndex, mutation.AtomicBatchIndex, out var existed);
            if (existed)
            {
                StagedBytes -= (long)(slot.Key?.Length ?? 0) + (slot.Value?.Length ?? 0);
            }

            slot = mutation;
            StagedBytes += bytes;

            if (!HasOldestPreparedHlc || mutation.Timestamp < OldestPreparedHlc)
            {
                OldestPreparedHlc = mutation.Timestamp;
                HasOldestPreparedHlc = true;
            }
        }
    }
}

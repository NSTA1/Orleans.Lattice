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

    // Set when a commit terminal arrives for a batch the staging backstop
    // abandoned (ViewCheckpointState.AbandonedTransactions): the drain answers
    // it with a rebuild, which reads the now-committed batch from source.
    private bool _abandonedBatchCommitted;

    // Activation-thread snapshot of ViewCheckpointState.AbandonedTransactions for
    // the drain handler (which must not touch grain state), and the ids it retired
    // that the activation thread folds back into the persisted list.
    private HashSet<Guid> _abandoned = [];
    private readonly List<Guid> _abandonedRetirements = [];

    // Replay fence planned by the resume-floor capture of an in-progress rebuild,
    // persisted by the rebuild's checkpoint write (see PlanRebuildReplayAsync).
    private RebuildReplayFence? _pendingRebuildFence;

    /// <summary>
    /// Most WAL entries per partition the rebuild resume-floor capture examines
    /// below the head for in-flight atomic prepares it must not skip.
    /// </summary>
    private const int RebuildPrepareScanLimit = 1 << 16;

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

    // The supersession HLCs the armed replay fence restored. They outlive the
    // refcount of the live map: a replayed batch released (aborted or flushed)
    // before a still-replayed batch re-stages the same key would otherwise drop
    // the reflected ordinary write's HLC, and the second batch could flush its
    // older prepare before the fenced ordinary write is re-read and re-recorded.
    private Dictionary<string, HybridLogicalClock>? _replayFenceSupersession;
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

        if (IsAbandoned(txId))
        {
            // The backstop discarded this batch before its decision was known and
            // the rebuild that followed could not see it, so it is never re-staged
            // (its earlier entries are not re-read). An abort retires it; a commit
            // means the committed batch now lives only in source state, which the
            // drain re-reads by rebuilding. A commit does not resolve the batch: a
            // multi-shard batch's terminals land one partition at a time, so a later
            // shard's commit must trigger its own rebuild rather than be dropped.
            if (terminalAbort)
            {
                _abandoned.Remove(txId);
                _abandonedRetirements.Add(txId);
                MarkResolved(txId);
            }
            else if (terminalCommit)
            {
                _abandonedBatchCommitted = true;
            }

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
    /// highest such HLC. Only writes read <i>after</i> the prepare is staged are
    /// seen here, and offset order is NOT HLC order: a replicated, shadow-forwarded
    /// or clock-skewed ordinary write can carry a lower HLC than a prepare it
    /// follows (the HLC comparison at flush keeps the prepare then), and one with a
    /// higher HLC can be logged before the prepare (it is not refcounted yet, so is
    /// missed here and caught by source confirmation at flush instead; issue #4882).
    /// A range delete records only the keys it actually tombstoned
    /// (<see cref="LatticeMutation.MatchedKeys"/>), never its start key: the start
    /// key need not have been live, and an unconstrained range is likewise left to
    /// source confirmation.
    /// </summary>
    private void RecordOrdinaryOverStagedKey(in LatticeMutation mutation)
    {
        if (mutation.Kind == MutationKind.DeleteRange)
        {
            if (mutation.MatchedKeys is { } matched)
            {
                foreach (var key in matched)
                {
                    RecordOrdinaryOverStagedKey(key, mutation.Timestamp);
                }
            }

            return;
        }

        RecordOrdinaryOverStagedKey(mutation.Key, mutation.Timestamp);
    }

    private void RecordOrdinaryOverStagedKey(string key, HybridLogicalClock timestamp)
    {
        if (!_stagedSourceKeyRefCount.ContainsKey(key))
        {
            return;
        }

        // Single-probe max-fold: the prior TryGetValue-then-indexer-set pair
        // hashed the source key twice for every ordinary write landing on a
        // still-staged key.
        ref var prev = ref CollectionsMarshal.GetValueRefOrAddDefault(
            _ordinaryHlcOverStagedKey, key, out var existed);
        if (!existed || timestamp.CompareTo(prev) > 0)
        {
            prev = timestamp;
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
        => (_ordinaryHlcOverStagedKey.TryGetValue(prepared.Key, out var oh)
                && oh.CompareTo(prepared.Timestamp) > 0)
            || (_replayFenceSupersession is { } fenced
                && fenced.TryGetValue(prepared.Key, out var fh)
                && fh.CompareTo(prepared.Timestamp) > 0);

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

        SyncAbandoned();
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
            _replayFenceSupersession = null;
            return;
        }

        if (!string.Equals(state.State.ReplayFenceWalTreeId, walTreeId, StringComparison.Ordinal))
        {
            ClearReplayFence();
            return;
        }

        _replayFenceOffsets = new Dictionary<int, long>(offsets);
        _replayFenceStaged = new HashSet<Guid>(state.State.ReplayFenceStagedTransactions ?? []);
        _replayFenceSupersession = null;
        if (state.State.ReplayFenceSupersession is { Count: > 0 } supersession)
        {
            _replayFenceSupersession = new Dictionary<string, HybridLogicalClock>(supersession, StringComparer.Ordinal);
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
    /// Clears it once nothing is staged and any armed fence has been passed; an
    /// unpassed fence is kept (with an empty staged set) even when every batch it
    /// re-staged has resolved, because the entries between the read position and
    /// the fence head were already applied by an earlier pass and must stay fenced.
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
        var armedUnpassed = false;
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
                _replayFenceSupersession = null;
            }
            else
            {
                armedUnpassed = true;
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

        if (staged.Count == 0 && !armedUnpassed)
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
        _replayFenceSupersession = null;
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
    /// Raises <paramref name="advancedOffsets"/> to the position this pass resumed
    /// reading from on every partition. The drain reports only partitions that
    /// read something, so without this a pass that resolves a staged batch while
    /// reading nothing new (a joint cross-tree flip observed on re-registration,
    /// or an abandonment) would leave the persisted checkpoint held back below
    /// entries earlier passes already applied, after the emptied staging buffer
    /// has cleared the replay fence; the next pass would then re-read from the
    /// stale hold-back, skip the resolved batch, and re-apply an older ordinary
    /// write over its value. Every entry below the resume position was applied,
    /// is resolved, or is still staged; <see cref="ApplyCheckpointHoldBack"/>
    /// then clamps any partition a still-staged batch occupies.
    /// </summary>
    private static void CoverReadPosition(Dictionary<int, long> advancedOffsets, IReadOnlyDictionary<int, long> readFrom)
    {
        foreach (var (partition, offset) in readFrom)
        {
            if (offset > advancedOffsets.GetValueOrDefault(partition, -1))
            {
                advancedOffsets[partition] = offset;
            }
        }
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

        // A batch whose commit has not yet been read is undecided as far as the
        // view knows, so the rebuild below omits it and its entries are not
        // re-read. Remember it so a later commit terminal is answered with a
        // rebuild rather than staged without its prepares (and held forever).
        // A complete cross-tree batch may already have registered its slice with
        // the joint coordinator. The rebuild reads current source state, so a later
        // joint flip of that registered slice would land older values over it:
        // remember the operation so the caller degrades it before rebuilding.
        foreach (var (txId, tx) in _staging)
        {
            if (!IsComplete(tx))
            {
                RecordAbandoned(txId);
            }
            else if (tx.CrossTreeOperationId is { } operationId
                && ComputeViewWaitSet(tx.CrossTreeParticipants ?? []).Count > 1)
            {
                var pending = state.State.BackstopDiscardedCrossTreeOperations ??= [];
                if (!pending.Contains(operationId))
                {
                    pending.Add(operationId);
                }
            }
        }

        _staging.Clear();
        _stagedSourceKeyRefCount.Clear();
        _ordinaryHlcOverStagedKey.Clear();
        _droppedSuperseders.Clear();
        return true;
    }

    /// <summary>
    /// Terminally degrades, at the joint coordinator, every cross-tree operation
    /// whose registered slice the staging backstop just discarded, so no later
    /// joint flip lands that stale slice over the rebuild that follows. When the
    /// joint flip has already been applied it landed before the rebuild, which
    /// then supersedes it, so nothing further is needed. An operation is retired
    /// only once the coordinator has answered. The pending ids are persisted before
    /// the first coordinator call, so a failed call - or a reactivation before it -
    /// leaves them durable: the next drain is admitted to the backstop path by
    /// <see cref="BackstopDegradePending"/>, and every rebuild (backstop, operator,
    /// fall-off-log or reconcile) degrades them before it clears or publishes.
    /// </summary>
    private async Task DegradeBackstopDiscardedCrossTreeAsync()
    {
        var pending = state.State.BackstopDiscardedCrossTreeOperations;
        if (pending is not { Count: > 0 })
        {
            return;
        }

        await state.WriteStateAsync();
        while (pending.Count > 0)
        {
            var operationId = pending[^1];
            var decision = await grainFactory
                .GetGrain<IViewCrossTreeCoordinatorGrain>(operationId)
                .RegisterDegradedAsync(ViewName);
            if (!decision.Applied)
            {
                ViewCrossTreeJointViolation.Add(1, ViewTag, ViewTenantTag);
                logger.LogWarning(
                    "View '{ViewName}' degraded cross-tree operation '{OperationId}' to per-tree-slice atomicity because the staging backstop discarded its registered slice.",
                    ViewName, operationId);
            }

            pending.RemoveAt(pending.Count - 1);
        }

        // The emptied list is persisted by the rebuild's checkpoint write; until
        // then a stale entry only causes a redundant, idempotent degrade.
        state.State.BackstopDiscardedCrossTreeOperations = null;
    }

    /// <summary>
    /// <c>true</c> while a cross-tree operation discarded by the staging backstop
    /// still awaits terminal degradation at its joint coordinator.
    /// </summary>
    private bool BackstopDegradePending()
        => state.State.BackstopDiscardedCrossTreeOperations is { Count: > 0 };

    private bool IsAbandoned(Guid txId) => _abandoned.Contains(txId);

    /// <summary>
    /// Folds the drain handler's retirements into the persisted abandoned list and
    /// refreshes the handler's snapshot of it. Activation thread only.
    /// </summary>
    private void SyncAbandoned()
    {
        foreach (var txId in _abandonedRetirements)
        {
            RetireAbandoned(txId);
        }

        _abandonedRetirements.Clear();
        _abandoned = state.State.AbandonedTransactions is { Count: > 0 } persisted ? [.. persisted] : [];
    }

    private void RecordAbandoned(Guid txId)
    {
        var abandoned = state.State.AbandonedTransactions ??= [];
        if (!abandoned.Contains(txId))
        {
            abandoned.Add(txId);
            var capacity = Options.AbandonedTransactionCapacity > 0
                ? Options.AbandonedTransactionCapacity
                : ResolvedCapacity;
            if (abandoned.Count > capacity)
            {
                // A forgotten batch's later commit stages without its unread
                // prepares; OrphanedCommittedBatchAsync detects that and rebuilds.
                logger.LogWarning(
                    "View '{ViewName}' abandoned-batch memory exceeded its capacity ({Capacity}); forgetting the {Count} oldest.",
                    ViewName, capacity, abandoned.Count - capacity);
                abandoned.RemoveRange(0, abandoned.Count - capacity);
            }
        }

        _abandoned = [.. abandoned];
    }

    private void RetireAbandoned(Guid txId)
    {
        _abandoned.Remove(txId);
        if (state.State.AbandonedTransactions is { } abandoned && abandoned.Remove(txId) && abandoned.Count == 0)
        {
            state.State.AbandonedTransactions = null;
        }
    }

    /// <summary>
    /// <see langword="true"/> (once) when this pass read a commit terminal for a
    /// batch the staging backstop abandoned; the caller rebuilds so the committed
    /// batch is read from source state.
    /// </summary>
    private bool AbandonedBatchCommitted()
    {
        SyncAbandoned();
        if (!_abandonedBatchCommitted)
        {
            return false;
        }

        _abandonedBatchCommitted = false;
        logger.LogWarning(
            "View '{ViewName}' read the commit of an atomic batch the staging backstop had abandoned; rebuilding to read it from source.",
            ViewName);
        return true;
    }

    /// <summary>
    /// Plans how the tail resumes after a rebuild whose source scan starts at the
    /// captured heads. The scan reflects every ordinary write and every batch
    /// decided before it, but not a batch still in flight: such a batch's later
    /// commit terminal lands above the head, while its prepares may sit at or
    /// below it - unread by an earlier pass, or read and staged. Skipping them
    /// would leave the terminal staged without its prepares, holding the
    /// checkpoint forever and never exposing the committed batch.
    /// <para>
    /// The capture therefore examines each partition below its head, from the
    /// persisted checkpoint (bounded by <see cref="RebuildPrepareScanLimit"/>), for
    /// prepares of a batch whose terminal is not in that window, and lowers the
    /// partition's resume floor below the earliest. It also records a replay
    /// fence at the heads naming those batches and the still-staged ones, so the
    /// resumed tail re-stages only them and does not re-apply any other entry
    /// the scan already reflects. Batches the backstop abandoned are excluded.
    /// </para>
    /// </summary>
    private async Task PlanRebuildReplayAsync(
        string walTreeId,
        IReadOnlyList<long> heads,
        Dictionary<int, long> capturedOffsets,
        CancellationToken cancellationToken)
    {
        _abandonedBatchCommitted = false;
        _pendingRebuildFence = null;
        SyncAbandoned();

        var prepareFloors = new Dictionary<Guid, Dictionary<int, long>>();
        var tallies = new Dictionary<Guid, TerminalTally>();
        var crossTreeOperations = new Dictionary<Guid, string>();
        var preparedKeys = new Dictionary<Guid, Dictionary<string, HybridLogicalClock>>();
        var ordinaryKeys = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        var boundToThisLog = string.Equals(state.State.BoundPhysicalTreeId, walTreeId, StringComparison.Ordinal);
        var scanLimit = Options.RebuildPrepareScanLimit > 0
            ? Options.RebuildPrepareScanLimit
            : RebuildPrepareScanLimit;
        for (var partition = 0; partition < heads.Count; partition++)
        {
            var last = heads[partition] - 1;
            if (last < 0)
            {
                continue;
            }

            var start = Math.Max(-1, last - scanLimit);
            var clamped = start > -1;
            if (boundToThisLog
                && state.State.AppliedOffsets.TryGetValue(partition, out var applied)
                && applied > start)
            {
                start = applied;
                clamped = false;
            }

            // The clamp bounds only the search for unknown in-flight batches. A still
            // staged batch is replayed whatever the window, so the scan reaches its
            // held floor: the batches and ordinary writes between it and the window
            // that the rebuild reflects must supersede its keys (ReflectedSupersession),
            // or its older values overwrite them when it later completes.
            var stagedFloor = HeldFloorForPartition(partition);
            if (clamped && stagedFloor != long.MaxValue && stagedFloor - 1 < start)
            {
                start = stagedFloor - 1;
            }

            if (start >= last)
            {
                continue;
            }

            if (clamped)
            {
                // Prepares below the window are not found here; a batch they belong to
                // that commits later is caught by OrphanedCommittedBatchAsync instead.
                logger.LogWarning(
                    "View '{ViewName}' rebuild prepare scan of partition {Partition} was clamped to its last {Limit} entries.",
                    ViewName, partition, scanLimit);
            }

            await foreach (var (offset, mutation) in commitLogReader.ReadAsync(walTreeId, partition, start, cancellationToken))
            {
                if (offset > last)
                {
                    break;
                }

                var disposition = Classify(mutation, out var commit, out var abort);
                if (disposition == StagingDisposition.Apply)
                {
                    // A range delete supersedes only the keys it tombstoned; see
                    // RecordOrdinaryOverStagedKey.
                    if (mutation.Kind != MutationKind.DeleteRange)
                    {
                        NoteHighest(ordinaryKeys, mutation.Key, mutation.Timestamp);
                    }
                    else if (mutation.MatchedKeys is { } matched)
                    {
                        foreach (var key in matched)
                        {
                            NoteHighest(ordinaryKeys, key, mutation.Timestamp);
                        }
                    }

                    continue;
                }

                if (disposition != StagingDisposition.Stage)
                {
                    continue;
                }

                var txId = mutation.TransactionId;
                if (commit || abort)
                {
                    if (!tallies.TryGetValue(txId, out var tally))
                    {
                        tally = new TerminalTally();
                        tallies[txId] = tally;
                    }

                    tally.Note(commit, abort, mutation.ShardIndex, mutation.AtomicShardCount);
                    if (commit
                        && mutation.CrossTreeOperationId is { } operationId
                        && ComputeViewWaitSet(mutation.CrossTreeParticipants ?? []).Count > 1)
                    {
                        crossTreeOperations[txId] = operationId;
                    }
                }

                else if (mutation.IsPrepared)
                {
                    if (!preparedKeys.TryGetValue(txId, out var keys))
                    {
                        keys = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
                        preparedKeys[txId] = keys;
                    }

                    ref var hlc = ref CollectionsMarshal.GetValueRefOrAddDefault(keys, mutation.Key, out var seen);
                    if (!seen || mutation.Timestamp.CompareTo(hlc) > 0)
                    {
                        hlc = mutation.Timestamp;
                    }
                }

                // A terminal's offset is a floor too: a batch still committing shard by
                // shard re-reads the terminals already in the log alongside its prepares.
                if (!prepareFloors.TryGetValue(txId, out var floors))
                {
                    floors = [];
                    prepareFloors[txId] = floors;
                }

                floors.TryAdd(partition, offset);
            }
        }

        // A batch is decided before the scan only once it aborted or every shard's
        // commit lies below the heads: the multi-shard saga appends one terminal per
        // shard on that shard's partition, so one commit alone does not make the
        // source reflect the batch. A decided batch the backstop abandoned is
        // reflected by the scan and retired.
        var terminated = new HashSet<Guid>();
        foreach (var (txId, tally) in tallies)
        {
            if (tally.Decided)
            {
                terminated.Add(txId);
                RetireAbandoned(txId);
                RecordRetiredCrossTreeOperation(txId, crossTreeOperations);
            }
        }

        var replayed = boundToThisLog ? new HashSet<Guid>(_staging.Keys) : new HashSet<Guid>();
        foreach (var (txId, floors) in prepareFloors)
        {
            if (terminated.Contains(txId) || _resolved.Contains(txId) || IsAbandoned(txId))
            {
                continue;
            }

            replayed.Add(txId);
            foreach (var (partition, offset) in floors)
            {
                if (offset - 1 < capturedOffsets[partition])
                {
                    capturedOffsets[partition] = offset - 1;
                }
            }
        }

        var fence = new Dictionary<int, long>(heads.Count);
        var lowered = false;
        for (var partition = 0; partition < heads.Count; partition++)
        {
            fence[partition] = heads[partition] - 1;
            lowered |= capturedOffsets[partition] < heads[partition] - 1;
        }

        if (lowered)
        {
            _pendingRebuildFence = new RebuildReplayFence(
                walTreeId, fence, [.. replayed], ReflectedSupersession(tallies, preparedKeys, ordinaryKeys, replayed));
        }
    }

    /// <summary>
    /// The supersession HLCs a replay fence must carry for the batches a rebuild
    /// reflects from source rather than replays. The resumed tail re-reads those
    /// batches' fenced prepares as already resolved, so they never pass through
    /// <see cref="RecordFlushedPreparesOverStagedKeys"/>; yet the source installed
    /// each committed one by last-writer-wins on its prepare HLC, so a replayed
    /// batch that prepared the same key earlier and commits later must not
    /// overwrite it. Only committed batches count (an aborted prepare never
    /// reached the source), and only keys a replayed batch also prepared.
    /// <para>
    /// The ordinary writes in the scanned window count the same way. A replayed
    /// batch already decided below the heads (still staged at the rebuild) can
    /// complete on a resumed pass that ends before re-reading a later ordinary
    /// write to one of its keys; every later pass then reads that write fenced,
    /// as reflected, so without its HLC here the batch's older value would
    /// overwrite it permanently.
    /// </para>
    /// </summary>
    private static Dictionary<string, HybridLogicalClock>? ReflectedSupersession(
        Dictionary<Guid, TerminalTally> tallies,
        Dictionary<Guid, Dictionary<string, HybridLogicalClock>> preparedKeys,
        Dictionary<string, HybridLogicalClock> ordinaryKeys,
        HashSet<Guid> replayed)
    {
        var replayedKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var txId in replayed)
        {
            if (preparedKeys.TryGetValue(txId, out var keys))
            {
                replayedKeys.UnionWith(keys.Keys);
            }
        }

        Dictionary<string, HybridLogicalClock>? supersession = null;
        foreach (var (txId, keys) in preparedKeys)
        {
            if (replayed.Contains(txId) || !tallies.TryGetValue(txId, out var tally) || !tally.Committed)
            {
                continue;
            }

            foreach (var (key, hlc) in keys)
            {
                if (!replayedKeys.Contains(key))
                {
                    continue;
                }

                supersession ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
                NoteHighest(supersession, key, hlc);
            }
        }

        foreach (var (key, hlc) in ordinaryKeys)
        {
            if (replayedKeys.Contains(key))
            {
                supersession ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
                NoteHighest(supersession, key, hlc);
            }
        }

        return supersession;
    }

    private static void NoteHighest(Dictionary<string, HybridLogicalClock> highest, string key, HybridLogicalClock hlc)
    {
        ref var prev = ref CollectionsMarshal.GetValueRefOrAddDefault(highest, key, out var seen);
        if (!seen || hlc.CompareTo(prev) > 0)
        {
            prev = hlc;
        }
    }

    /// <summary>
    /// Persists (on the rebuild's checkpoint write) the replay fence planned by
    /// <see cref="PlanRebuildReplayAsync"/>, or clears the fence when the resumed
    /// tail starts at the heads and so re-reads nothing.
    /// </summary>
    private void ApplyRebuildReplayFence()
    {
        if (_pendingRebuildFence is not { } planned)
        {
            ClearReplayFence();
            return;
        }

        _pendingRebuildFence = null;
        _replayFenceOffsets = null;
        _replayFenceStaged = null;
        _replayFenceSupersession = null;
        state.State.ReplayFenceWalTreeId = planned.WalTreeId;
        state.State.ReplayFenceOffsets = planned.Offsets;
        state.State.ReplayFenceStagedTransactions = planned.Staged;
        state.State.ReplayFenceSupersession = planned.Supersession;
    }

    /// <summary>
    /// Queues for terminal degradation a decided cross-tree batch the rebuild
    /// reflects from source and does not replay. Its slice may have been
    /// registered at the joint coordinator by this view - in this activation or a
    /// previous one whose staging was lost - and the view never re-registers a
    /// batch it does not replay, so a peer completing the wait set later would
    /// flip that stale slice over the rebuilt (and since overwritten) values. A
    /// batch still staged is resolved by its own drain, and one this activation
    /// already resolved needs nothing; degrading an operation the coordinator has
    /// already flipped is answered as applied and is a no-op.
    /// </summary>
    private void RecordRetiredCrossTreeOperation(Guid txId, Dictionary<Guid, string> crossTreeOperations)
    {
        if (!crossTreeOperations.TryGetValue(txId, out var operationId)
            || _staging.ContainsKey(txId)
            || _resolved.Contains(txId))
        {
            return;
        }

        var pending = state.State.BackstopDiscardedCrossTreeOperations ??= [];
        if (!pending.Contains(operationId))
        {
            pending.Add(operationId);
        }
    }

    /// <summary>The terminals of one batch a rebuild's resume-floor capture read.</summary>
    private sealed class TerminalTally
    {
        private readonly HashSet<int> _committedShards = [];
        private bool _aborted;
        private int _expectedShards;

        /// <summary><see langword="true"/> when every expected shard committed and none aborted.</summary>
        public bool Committed => !_aborted && Decided;

        /// <summary><see langword="true"/> when the batch aborted or every expected shard committed.</summary>
        public bool Decided => _aborted
            || (_committedShards.Count > 0 && (_expectedShards == 0 || _committedShards.Count >= _expectedShards));

        public void Note(bool commit, bool abort, int shardIndex, int atomicShardCount)
        {
            _aborted |= abort;
            if (commit)
            {
                _committedShards.Add(shardIndex);
                _expectedShards = Math.Max(_expectedShards, atomicShardCount);
            }
        }
    }

    /// <summary>
    /// Detects a batch with known prepare cardinality staged without prepares it can no longer read
    /// (they lie below the checkpoint because the backstop forgot or a clamped
    /// rebuild scan missed the batch) and responds by rebuilding. Every prepare is
    /// appended before the complete set of the batch's terminals, so once the drain has read the
    /// whole log below the heads captured after a terminal was staged, a prepare
    /// still missing will never be read. The batch is discarded and remembered as
    /// abandoned, so the rebuild reads it from source and its later entries are not
    /// re-staged. A terminal with no observed prepare remains staged unless a
    /// bounded search finds its cardinality in the same physical log below the
    /// checkpoint; a sampled head alone is never such a witness.
    /// </summary>
    private async Task<bool> OrphanedCommittedBatchAsync(
        string walTreeId,
        int partitions,
        IReadOnlyDictionary<int, long> readFrom,
        Dictionary<int, long> advancedOffsets,
        CancellationToken cancellationToken)
    {
        List<Guid>? orphaned = null;
        var orphanSearchBudget = Options.RebuildPrepareScanLimit > 0
            ? Options.RebuildPrepareScanLimit
            : RebuildPrepareScanLimit;
        foreach (var (txId, tx) in _staging)
        {
            if (!tx.TerminalSeen)
            {
                continue;
            }

            if ((tx.AtomicBatchSize == 0
                    || (tx.ExpectedShardCount > 0 && tx.CommittedShards.Count < tx.ExpectedShardCount))
                && orphanSearchBudget > 0)
            {
                orphanSearchBudget -= await RecoverOrphanPrepareCardinalityAsync(
                    walTreeId, partitions, txId, tx, orphanSearchBudget, cancellationToken);
            }

            if (tx.AtomicBatchSize == 0 || tx.PreparesByIndex.Count >= tx.AtomicBatchSize
                || (tx.ExpectedShardCount > 0 && tx.CommittedShards.Count < tx.ExpectedShardCount))
            {
                continue;
            }

            if (tx.PrepareHorizon is null)
            {
                var heads = await Orleans.Lattice.BPlusTree.BoundedFanOut.RunAsync(
                    partitions,
                    Orleans.Lattice.BPlusTree.BoundedFanOut.DefaultWidth,
                    partition => commitLogReader.GetHeadOffsetAsync(walTreeId, partition, cancellationToken),
                    cancellationToken);
                tx.PrepareHorizon = heads;
            }

            var readPastHorizon = true;
            for (var partition = 0; partition < tx.PrepareHorizon.Length; partition++)
            {
                var read = Math.Max(
                    readFrom.TryGetValue(partition, out var from) ? from : -1,
                    advancedOffsets.TryGetValue(partition, out var advanced) ? advanced : -1);
                if (read < tx.PrepareHorizon[partition] - 1)
                {
                    readPastHorizon = false;
                    break;
                }
            }

            if (readPastHorizon)
            {
                (orphaned ??= []).Add(txId);
            }
        }

        if (orphaned is null)
        {
            return false;
        }

        foreach (var txId in orphaned)
        {
            ReleaseStagedKeys(_staging[txId]);
            _staging.Remove(txId);
            RecordAbandoned(txId);
        }

        logger.LogWarning(
            "View '{ViewName}' staged {Count} committed atomic batch(es) whose prepares lie below its checkpoint; rebuilding to read them from source.",
            ViewName, orphaned.Count);
        return true;
    }

    /// <summary>
    /// Recovers an omitted prepare's cardinality and consumed shard terminals
    /// from retained entries below this physical checkpoint. Search cursors are
    /// volatile staging state: a reactivation retries them, and a rebind drops
    /// them with the batch. All batches share one examined-entry budget per drain.
    /// </summary>
    private async Task<int> RecoverOrphanPrepareCardinalityAsync(
        string walTreeId,
        int partitions,
        Guid txId,
        StagedTransaction tx,
        int budget,
        CancellationToken cancellationToken)
    {
        var examined = 0;
        tx.OrphanPrepareSearchOffsets ??= [];
        for (var partition = 0; partition < partitions; partition++)
        {
            if (!state.State.AppliedOffsets.TryGetValue(partition, out var checkpoint))
            {
                continue;
            }

            var from = tx.OrphanPrepareSearchOffsets.GetValueOrDefault(partition, -1);
            if (from >= checkpoint)
            {
                continue;
            }

            await foreach (var (offset, mutation) in commitLogReader.ReadFilteredAsync(
                walTreeId, partition, from, checkpoint, budget - examined, default, cancellationToken))
            {
                examined++;
                tx.OrphanPrepareSearchOffsets[partition] = offset;
                if (mutation.IsPrepared && mutation.TransactionId == txId && mutation.AtomicBatchSize > 0)
                {
                    tx.NotePrepareCardinality(mutation.AtomicBatchSize);
                }
                else if (mutation.Kind == MutationKind.TxCommit && mutation.TransactionId == txId)
                {
                    tx.CommittedShards.Add(mutation.ShardIndex);
                    tx.ExpectedShardCount = Math.Max(tx.ExpectedShardCount, mutation.AtomicShardCount);
                }

                if (tx.AtomicBatchSize > 0
                    && (tx.ExpectedShardCount == 0 || tx.CommittedShards.Count >= tx.ExpectedShardCount))
                {
                    return examined;
                }

                if (examined >= budget)
                {
                    return examined;
                }
            }

            tx.OrphanPrepareSearchOffsets[partition] = checkpoint;
        }

        return examined;
    }

    /// <summary>A replay fence planned by a rebuild's resume-floor capture.</summary>
    private sealed record RebuildReplayFence(
        string WalTreeId,
        Dictionary<int, long> Offsets,
        List<Guid> Staged,
        Dictionary<string, HybridLogicalClock>? Supersession);

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
        CancellationToken cancellationToken,
        bool crossTreeOnly)
    {
        var applied = 0;
        foreach (var txId in completed)
        {
            if (!_staging.TryGetValue(txId, out var tx)
                || (tx.CrossTreeOperationId is not null) != crossTreeOnly)
            {
                continue;
            }

            var closure = registration.Accumulative ? null : LocalPublicationClosure(txId, completed);
            if (closure is { Count: > 1 })
            {
                applied += await PublishLocalFilterClosureAsync(viewTree, registration, closure, cancellationToken);
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
                if (!registration.Accumulative && (IsSupersededByOrdinary(prepared) || IsSourceSuperseded(tx, prepared)))
                {
                    continue;
                }

                foreach (var write in registration.Projection!.Project(ProjectionInput(registration, prepared)))
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
    /// The highest source HLC at which an atomic batch completing this pass writes
    /// each view key (an Upsert or a retracting Delete), or <see langword="null"/>
    /// when no completing batch writes any. Drives the in-pass replay guard
    /// (<see cref="DropSurvivorsOverwrittenByCompletedBatches"/> and the spared
    /// keys of a confirmed range delete): a non-atomic write older than the batch
    /// at the same view key can never be the view's final value, because the
    /// batch's flip lands after it, so it is only ever a transient. It is not a
    /// harmless one on a replay (a lost flush response retried on the same
    /// activation, or a reactivation resuming below an already flipped batch): the
    /// flip re-attaches to its completed <see cref="ViewSagaOperationId(Guid)"/>
    /// saga, or the cross-tree coordinator answers with its memoized decision, and
    /// writes nothing, so the older write would otherwise rewrite part of the
    /// batch's slice non-atomically and expose a slice that never existed at the
    /// source (issue #4869). The source last-writer check (#4885) already drops
    /// such a write while the source still reports the batch's version; this guard
    /// does not depend on that read, so it also holds when the read fails open (a
    /// purged copy) or the batch's tombstone was compacted.
    /// <para>
    /// Cross-tree batches are included. A committed cross-tree batch is always
    /// resolved by writing its slice - the joint flip, the degraded local flip, or
    /// the degenerate single-view flip - so a dropped older write is never lost:
    /// while the joint decision is pending the view keeps the key's pre-write
    /// value, which is also the pre-batch source state of the slice. Prepares an
    /// ordinary write superseded are excluded exactly as the flush excludes them,
    /// so a write that is the source last-writer is never dropped or spared
    /// against.
    /// </para>
    /// </summary>
    private Dictionary<string, HybridLogicalClock>? CompletedBatchHlcByViewKey(
        ViewRegistration registration,
        List<Guid> completed)
    {
        Dictionary<string, HybridLogicalClock>? batchHlcByViewKey = null;
        foreach (var txId in completed)
        {
            if (!_staging.TryGetValue(txId, out var tx))
            {
                continue;
            }

            foreach (var prepared in tx.PreparesByIndex.Values)
            {
                if (IsSupersededByOrdinary(prepared) || IsSourceSuperseded(tx, prepared))
                {
                    continue;
                }

                foreach (var write in registration.Projection!.Project(ProjectionInput(registration, prepared)))
                {
                    if (write.Kind is not (ViewWriteKind.Upsert or ViewWriteKind.Delete))
                    {
                        continue;
                    }

                    batchHlcByViewKey ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
                    if (!batchHlcByViewKey.TryGetValue(write.Key, out var existing) || write.Timestamp.CompareTo(existing) > 0)
                    {
                        batchHlcByViewKey[write.Key] = write.Timestamp;
                    }
                }
            }
        }

        return batchHlcByViewKey;
    }

    /// <summary>
    /// Replay guard for non-atomic point writes, on the point path and the range
    /// path alike: drops every Upsert or Delete whose view key a batch completing
    /// this pass writes at a higher source HLC (see
    /// <see cref="CompletedBatchHlcByViewKey"/>). Range writes are never dropped:
    /// their key is only the range start. Returns <paramref name="writes"/>
    /// unchanged when nothing is dropped.
    /// </summary>
    private static IReadOnlyList<ViewWrite> DropSurvivorsOverwrittenByCompletedBatches(
        IReadOnlyList<ViewWrite> writes,
        Dictionary<string, HybridLogicalClock>? batchHlcByViewKey)
    {
        if (writes.Count == 0 || batchHlcByViewKey is null)
        {
            return writes;
        }

        List<ViewWrite>? kept = null;
        for (var i = 0; i < writes.Count; i++)
        {
            var write = writes[i];
            var overwritten = write.Kind is ViewWriteKind.Upsert or ViewWriteKind.Delete
                && batchHlcByViewKey.TryGetValue(write.Key, out var batchHlc)
                && write.Timestamp.CompareTo(batchHlc) < 0;
            if (overwritten && kept is null)
            {
                kept = new List<ViewWrite>(writes.Count - 1);
                for (var j = 0; j < i; j++)
                {
                    kept.Add(writes[j]);
                }
            }
            else if (!overwritten)
            {
                kept?.Add(write);
            }
        }

        return kept ?? writes;
    }

    /// <summary>
    /// Re-asserts a resolved batch's view slice after its flip. The in-pass guard
    /// (<see cref="CompletedBatchHlcByViewKey"/>) keeps a replayed pass from
    /// rewriting the slice, so on this maintainer's own replays the slice already
    /// matches and this is one <c>GetManyAsync</c> read. It remains a backstop for
    /// a slice something else rewrote between the flip and this pass's read (a
    /// writer outside this drain, or a joint flip carrying a slice registered
    /// before a supersession, which <see cref="ScheduleReconcileIfJointFlipMayBeSuperseded"/>
    /// also covers). When any key differs from the resolved slice it restores the
    /// WHOLE slice through one atomic op, so a reader never observes a partly
    /// repaired slice. The repair op id is fresh on every attempt and never reuses
    /// the completed batch's saga id (that id would re-attach as a no-op), so a
    /// lost repair response is retried with a new id; that is safe because the
    /// repair writes the slice's source last-writer-wins outcome - every ordinary
    /// write that supersedes the batch was dropped from <paramref name="upserts"/>
    /// and <paramref name="deletes"/> - so re-applying it converges to the same
    /// state. This path is local-only; cross-tree slices use mandatory joint repair.
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
        var matches = true;
        foreach (var upsert in upserts)
        {
            if (!current.TryGetValue(upsert.Key, out var value) || !value.AsSpan().SequenceEqual(upsert.Value))
            {
                matches = false;
                break;
            }
        }

        if (matches)
        {
            foreach (var key in deletes)
            {
                if (current.ContainsKey(key))
                {
                    matches = false;
                    break;
                }
            }
        }

        if (!matches)
        {
            await viewTree.SetManyAtomicAsync(upserts, deletes, ViewReassertOperationId(), cancellationToken);
        }
    }

    /// <summary>
    /// A fresh view-tree saga operation id for one slice repair attempt. Never
    /// derived from the batch's transaction id: that id names the completed flip
    /// saga, and re-submitting it re-attaches as a no-op.
    /// </summary>
    private static string ViewReassertOperationId() => $"mv-reassert-{Guid.NewGuid():N}";

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
        /// Source keys whose prepared entry the source no longer holds once the batch
        /// committed (a newer write or tombstone the view has already consumed won
        /// last-writer-wins), established by <see cref="ConfirmStagedBatchAgainstSourceAsync"/>.
        /// The flush skips these exactly as it skips <see cref="IsSupersededByOrdinary"/>.
        /// </summary>
        public HashSet<string>? SourceSupersededKeys { get; set; }

        /// <summary>
        /// Keys observed not to hold their prepared value at the source on an earlier
        /// pass, awaiting the drain's consumption of <see cref="PendingSourceHeads"/>.
        /// </summary>
        public HashSet<string>? PendingSourceKeys { get; set; }

        /// <summary>
        /// Per-partition WAL head offsets sampled after the source read that produced
        /// <see cref="PendingSourceKeys"/>; every write that read observed is below them.
        /// </summary>
        public long[]? PendingSourceHeads { get; set; }
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

        /// <summary>
        /// Source WAL heads captured once a commit terminal is staged while prepares
        /// are missing; every prepare lies below them. <see langword="null"/> until then.
        /// </summary>
        public long[]? PrepareHorizon { get; set; }

        /// <summary>Resumable positions of the bounded historical cardinality search on this staged physical log.</summary>
        public Dictionary<int, long>? OrphanPrepareSearchOffsets { get; set; }

        /// <summary>Records the cardinality carried by an actual prepared entry, without re-staging its payload.</summary>
        public void NotePrepareCardinality(int cardinality) =>
            AtomicBatchSize = Math.Max(AtomicBatchSize, cardinality);

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

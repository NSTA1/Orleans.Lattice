using System.Diagnostics;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// <see cref="ILeafProjection"/> implementation for <see cref="BPlusLeafGrain"/>.
/// Replays a single durably-committed mutation against the leaf's
/// in-memory state using LWW semantics; persists the projection
/// checkpoint offset alongside the leaf's existing storage row.
/// <para>
/// Ships dormant: today's foreground commit path is unchanged and no
/// caller drives <see cref="ILeafProjection.Apply"/>. The seam is
/// exercised exclusively by unit tests until the WAL-as-sole-commit-point
/// promotion lands.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Pending in-memory checkpoint offset that has been requested via
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> but not
    /// yet durably persisted. <c>null</c> when no advance is pending
    /// (the persisted offset on <see cref="LeafNodeState"/> is the
    /// source of truth).
    /// <para>
    /// Under <see cref="LatticeOptions.WalPartitions"/> greater than 1
    /// this map is keyed by partition; partition <c>0</c>'s entry is
    /// also mirrored into the scalar <c>ProjectionCheckpointOffset</c>
    /// on flush so a downgrade to a legacy silo still reads a valid
    /// single-partition shape.
    /// </para>
    /// </summary>
    private Dictionary<int, long>? _pendingCheckpointOffsetsByPartition;

    /// <summary>
    /// <see cref="Stopwatch.GetTimestamp"/> reading at the last durable
    /// checkpoint persist. Compared against
    /// <c>MaterialiserCheckpointInterval</c> on each advance to decide
    /// whether the time-driven flush should fire.
    /// </summary>
    private long _lastCheckpointPersistTimestamp = Stopwatch.GetTimestamp();

    void ILeafProjection.Apply(in LatticeMutation mutation)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                if (mutation.IsPrepared)
                    ApplyPreparedSet(mutation);
                else
                    ApplySet(mutation);
                break;
            case MutationKind.Delete:
                if (mutation.IsPrepared)
                    ApplyPreparedDelete(mutation);
                else
                    ApplyDelete(mutation);
                break;
            case MutationKind.DeleteRange:
                ApplyDeleteRange(mutation);
                break;
            case MutationKind.TxCommit:
                ApplyTxCommit(mutation.TransactionId);
                AdvanceProjectionClock(mutation.Timestamp);
                break;
            case MutationKind.TxAbort:
                ApplyTxAbort(mutation.TransactionId);
                AdvanceProjectionClock(mutation.Timestamp);
                break;
            case MutationKind.Tombstone:
                ApplyTombstoneReap(mutation);
                AdvanceProjectionClock(mutation.Timestamp);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation),
                    mutation.Kind,
                    $"Unknown {nameof(MutationKind)} '{mutation.Kind}'.");
        }
    }

    Task<long> ILeafProjection.GetCheckpointOffsetAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Return the most recent offset the caller has communicated
        // (pending or persisted, whichever is higher) for the partition
        // the caller is currently scoped under. The "durably committed"
        // notion is preserved via FlushCheckpointAsync; this accessor
        // reports the materialiser's current view so a read-modify-
        // write caller observes its own most-recent advance.
        var partition = LatticeApplyOffsetContext.CurrentPartition ?? 0;
        return Task.FromResult(GetCurrentCheckpointForPartition(partition));
    }

    /// <summary>
    /// Returns the materialiser's current per-partition view of the
    /// projection checkpoint (max of pending and persisted) for
    /// <paramref name="partition"/>. Partition <c>0</c> always reflects
    /// the scalar <c>ProjectionCheckpointOffset</c> slot for wire-compat
    /// with legacy single-partition state.
    /// </summary>
    internal long GetCurrentCheckpointForPartition(int partition)
    {
        var persisted = GetPersistedCheckpointForPartition(partition);
        if (_pendingCheckpointOffsetsByPartition is not null
            && _pendingCheckpointOffsetsByPartition.TryGetValue(partition, out var pending))
        {
            return Math.Max(persisted, pending);
        }
        return persisted;
    }

    private long GetPersistedCheckpointForPartition(int partition)
    {
        if (partition == 0)
            return state.State.ProjectionCheckpointOffset;
        var arr = state.State.ProjectionCheckpointOffsetsByPartition;
        if (arr is null || partition >= arr.Length)
            return -1L; // "nothing applied" sentinel - legacy state has no per-partition value.
        return arr[partition];
    }

    private void SetPersistedCheckpointForPartition(int partition, long value)
    {
        if (partition == 0)
        {
            state.State.ProjectionCheckpointOffset = value;
        }
        // Mirror partition 0 into the array slot (when present) so a
        // host that later reads ProjectionCheckpointOffsetsByPartition
        // observes a consistent picture; mirror non-zero partitions
        // into the array slot, growing it on first write. We never
        // shrink: the array's length is the maximum partition count
        // ever observed on this leaf.
        var arr = state.State.ProjectionCheckpointOffsetsByPartition;
        if (arr is null)
        {
            if (partition == 0)
                return; // legacy single-partition state, scalar slot suffices.
            arr = new long[partition + 1];
            // Seed every slot to the -1 "nothing applied" sentinel
            // except partition 0, which mirrors the scalar slot.
            for (var i = 0; i < arr.Length; i++)
                arr[i] = -1L;
            arr[0] = state.State.ProjectionCheckpointOffset;
            arr[partition] = value;
            state.State.ProjectionCheckpointOffsetsByPartition = arr;
            return;
        }
        if (partition >= arr.Length)
        {
            var grown = new long[partition + 1];
            arr.CopyTo(grown, 0);
            for (var i = arr.Length; i < grown.Length; i++)
                grown[i] = -1L;
            grown[partition] = value;
            state.State.ProjectionCheckpointOffsetsByPartition = grown;
            return;
        }
        arr[partition] = value;
    }

    async Task ILeafProjection.SetCheckpointOffsetAsync(long offset, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var partition = LatticeApplyOffsetContext.CurrentPartition ?? 0;
        var persisted = GetPersistedCheckpointForPartition(partition);
        long current = persisted;
        if (_pendingCheckpointOffsetsByPartition is not null
            && _pendingCheckpointOffsetsByPartition.TryGetValue(partition, out var p))
        {
            current = Math.Max(persisted, p);
        }

        if (offset < current)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset),
                offset,
                $"Projection checkpoint must be monotonically non-decreasing; current offset for partition {partition} is {current}.");
        }

        // Clamp the requested advance back behind any unresolved saga
        // prepare for this partition. See the multi-partition note in
        // RemovePendingTxOffsetsForTransaction: the clamp is partition-
        // scoped because cross-partition offsets are disjoint.
        if (MinUnresolvedPrepareOffsetForPartition(partition) is long minPrepare)
        {
            var clampFloor = minPrepare - 1;
            if (offset > clampFloor)
            {
                offset = clampFloor;
            }
        }

        if (offset < current)
        {
            // Clamp drove the requested offset back behind the current
            // materialised position. Silent no-op.
            return;
        }

        if (offset == current)
        {
            // Idempotent re-assert is a force-flush signal.
            await FlushPendingCheckpointAsync(persistEvenWithoutPendingAdvance: true);
            return;
        }

        (_pendingCheckpointOffsetsByPartition ??= new Dictionary<int, long>())[partition] = offset;

        // Coalescing predicate: persist if either threshold has been
        // exceeded. Zero interval means every-entry mode.
        var options = await GetOptionsAsync();
        var pendingEntries = offset - persisted;

        if (options.MaterialiserCheckpointInterval == TimeSpan.Zero
            || pendingEntries >= options.MaterialiserCheckpointEntries
            || HasIntervalElapsed(options.MaterialiserCheckpointInterval))
        {
            await FlushPendingCheckpointAsync(persistEvenWithoutPendingAdvance: false);
        }
    }

    async Task ILeafProjection.FlushCheckpointAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await FlushPendingCheckpointAsync(persistEvenWithoutPendingAdvance: false);
    }

    /// <summary>
    /// Synchronously flushes any pending checkpoint advance to durable
    /// storage. Called from <see cref="ILeafProjection.FlushCheckpointAsync"/>,
    /// from idempotent re-assert in <see cref="ILeafProjection.SetCheckpointOffsetAsync"/>, 
    /// from the coalescing fast-path when a threshold is met, and from
    /// the grain's graceful-deactivation hook so an unflushed advance
    /// is not lost on a clean shutdown.
    /// </summary>
    /// <param name="persistEvenWithoutPendingAdvance">
    /// When <c>true</c>, persist the leaf state even if no checkpoint
    /// advance is pending. Used by idempotent re-assert to commit
    /// in-memory Apply work that has accumulated since the previous
    /// persist.
    /// </param>
    private async Task FlushPendingCheckpointAsync(bool persistEvenWithoutPendingAdvance)
    {
        if (_pendingCheckpointOffsetsByPartition is { Count: > 0 } pending)
        {
            foreach (var (partition, offset) in pending)
            {
                SetPersistedCheckpointForPartition(partition, offset);
            }
            _pendingCheckpointOffsetsByPartition = null;
            // The checkpoint offset is a field of the published
            // ChildDigestSnapshot, so an advance must propagate upward
            // even when the projection hash itself is unchanged - the
            // parent's SubtreeHighestCheckpointOffset aggregate
            // depends on it.
            MarkDigestDirty();
            await PersistAsync();
            _lastCheckpointPersistTimestamp = Stopwatch.GetTimestamp();
            await ReportCursorIfActiveAsync();
            // Structural callers bypass the c2-xxviii coalescing
            // window so the parent's chained-fold observes the new
            // checkpoint offset before this method returns.
            await PublishDigestUpwardInlineAsync();
            await MaybeRunPeriodicSnapshotRecheckAsync();
            return;
        }

        if (persistEvenWithoutPendingAdvance)
        {
            await PersistAsync();
            _lastCheckpointPersistTimestamp = Stopwatch.GetTimestamp();
            await ReportCursorIfActiveAsync();
            // Apply work may have updated ProjectionHash since the
            // previous publish; flush any pending dirt. Structural
            // flush boundary - bypass the c2-xxviii coalescing window.
            await PublishDigestUpwardInlineAsync();
            await MaybeRunPeriodicSnapshotRecheckAsync();
        }
    }

    private bool HasIntervalElapsed(TimeSpan interval)
    {
        if (interval == Timeout.InfiniteTimeSpan)
            return false;
        var elapsedTicks = Stopwatch.GetTimestamp() - _lastCheckpointPersistTimestamp;
        var elapsedMs = elapsedTicks * 1000.0 / Stopwatch.Frequency;
        return elapsedMs >= interval.TotalMilliseconds;
    }

    private void ApplySet(in LatticeMutation mutation)
    {
        var incoming = new LwwValue<byte[]>
        {
            Value = mutation.IsTombstone ? null : mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.IsTombstone,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        MergeIntoProjection(mutation.Key, incoming);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    /// <summary>
    /// Replay path for a prepared-phase Set mutation. Routes the entry
    /// into the per-leaf pending-tx map rather than the visible
    /// projection so concurrent readers see pre-saga state until the
    /// saga's terminal mark surfaces.
    /// </summary>
    private void ApplyPreparedSet(in LatticeMutation mutation)
    {
        var incoming = new LwwValue<byte[]>
        {
            Value = mutation.IsTombstone ? null : mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.IsTombstone,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        // Carry the WAL-stamped typed CRDT delta and merge mode into the
        // pending-tx delta side-map so the activation-time replay
        // reconstructs the same fold state the foreground commit recorded.
        // The prepared WAL record carries both Delta and Mode (see
        // WalRecordConverter), and WalRecordConverter.FromWalRecord copies
        // them onto the mutation, so an LWW prepared write (Mode ==
        // LwwRegister, Delta == null) reconstructs no side-map entry and
        // replays byte-for-byte as before.
        AddPreparedMutation(
            mutation.TransactionId,
            mutation.Key,
            incoming,
            delta: mutation.Delta,
            mode: mutation.Mode);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void ApplyDelete(in LatticeMutation mutation)
    {
        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        MergeIntoProjection(mutation.Key, tombstone);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    /// <summary>
    /// Replay path for a prepared-phase Delete mutation. Routes the
    /// tombstone into the per-leaf pending-tx map rather than the
    /// visible projection.
    /// </summary>
    private void ApplyPreparedDelete(in LatticeMutation mutation)
    {
        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        AddPreparedMutation(mutation.TransactionId, mutation.Key, tombstone);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void ApplyDeleteRange(in LatticeMutation mutation)
    {
        var endExclusive = mutation.EndExclusiveKey;
        if (endExclusive is null)
            return;

        var startInclusive = mutation.Key;
        if (string.CompareOrdinal(startInclusive, endExclusive) >= 0)
            return;

        // Tombstone every existing entry inside the range. The mutation
        // carries one HLC for the whole batch; replays converge under LWW
        // because the tombstone's timestamp dominates any earlier write
        // and is dominated by any later write.
        //
        // A predicate-filtered range delete carries the explicit set of
        // matched keys (evaluated once at the authoring leaf). Replay
        // tombstones exactly that set - never re-deriving membership from a
        // predicate - so recovery is deterministic and independent of the
        // value bytes this projection currently holds.
        List<string>? toRewrite = null;
        var matchedKeys = mutation.MatchedKeys;
        if (matchedKeys is not null)
        {
            foreach (var key in matchedKeys)
            {
                if (string.CompareOrdinal(key, startInclusive) < 0
                    || string.CompareOrdinal(key, endExclusive) >= 0)
                    continue;
                if (Cache.TryGetRow(key, out _))
                    (toRewrite ??= []).Add(key);
            }
        }
        else
        {
            foreach (var (key, _) in Cache.EnumerateRows())
            {
                if (string.CompareOrdinal(key, startInclusive) < 0)
                    continue;
                if (string.CompareOrdinal(key, endExclusive) >= 0)
                    break;
                (toRewrite ??= []).Add(key);
            }
        }

        if (toRewrite is null)
            return;

        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };

        foreach (var key in toRewrite)
        {
            MergeIntoProjection(key, tombstone);
        }

        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void MergeIntoProjection(string key, LwwValue<byte[]> incoming)
    {
        // Replay-path projection writes are non-migration: they
        // mirror the foreground commit / drain / backstop semantics
        // (cross-shard migrations are persisted via PersistAsync, not
        // through the WAL, so they never re-emerge here at replay
        // time). The incoming value carries IsMigrated=false by default
        // (every WAL-authored LwwValue is non-migration), so when it
        // wins the merge inside StoreEntry it naturally clears any
        // stale migration marker, keeping replay's post-state bit-
        // identical to foreground's.
        StoreEntry(key, incoming);
    }

    /// <summary>
    /// Replay path for a <see cref="MutationKind.Tombstone"/> reap
    /// envelope authored by <c>CompactTombstonesAsync</c>. Physically
    /// removes the stamped key from the visible projection if the
    /// existing entry is a tombstone (or an already-expired live
    /// entry) whose timestamp does not dominate the reap envelope's
    /// timestamp. The HLC guard preserves LWW convergence under
    /// replay reordering - a reap envelope from an earlier compaction
    /// pass cannot resurrect-and-then-remove a freshly-written live
    /// entry that the same WAL slice already replayed.
    /// </summary>
    private void ApplyTombstoneReap(in LatticeMutation mutation)
    {
        if (!Cache.TryGetRow(mutation.Key, out var existing))
            return;
        if (existing.Timestamp > mutation.Timestamp)
            return;
        // Reap is well-formed only against tombstones or expired live
        // entries - any other shape indicates a stale envelope whose
        // counterpart Set replay landed later, and the live entry must
        // stay. The compactor only emits Tombstone envelopes for entries
        // that already met the tombstone-or-expired predicate at
        // compaction time, so this guard is a defence-in-depth check.
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (!existing.IsTombstone && !existing.IsExpired(nowTicks))
            return;
        RemoveEntry(mutation.Key);
    }

    private void AdvanceProjectionClock(HybridLogicalClock incoming)
    {
        if (incoming > state.State.Clock)
            state.State.Clock = incoming;
    }
}

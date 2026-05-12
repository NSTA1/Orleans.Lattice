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
    /// </summary>
    private long? _pendingCheckpointOffset;

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
        // (pending or persisted, whichever is higher). The "durably
        // committed" notion is preserved via FlushCheckpointAsync; this
        // accessor reports the materialiser's current view so a
        // read-modify-write caller observes its own most-recent advance.
        var persisted = state.State.ProjectionCheckpointOffset;
        var pending = _pendingCheckpointOffset;
        return Task.FromResult(pending is null ? persisted : Math.Max(persisted, pending.Value));
    }

    async Task ILeafProjection.SetCheckpointOffsetAsync(long offset, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var persisted = state.State.ProjectionCheckpointOffset;
        var current = _pendingCheckpointOffset is { } p ? Math.Max(persisted, p) : persisted;

        if (offset < current)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset),
                offset,
                $"Projection checkpoint must be monotonically non-decreasing; current offset is {current}.");
        }

        // Clamp the requested advance back behind any unresolved saga
        // prepare. Advancing the persisted checkpoint past the WAL
        // offset of an unresolved prepare would silently lose the
        // saga's writes if the leaf crashes before the terminal mark
        // replays - crash recovery would resume from offset+1 and
        // never see the prepare again. The clamp floor is
        // (min unresolved prepare offset) - 1 so a future replay
        // re-emits the prepare exactly once. Foreground commits leave
        // _pendingTxOffsets untouched (no ambient apply offset to
        // stamp), so this clamp degrades to a no-op for foreground-
        // only leaves.
        if (MinUnresolvedPrepareOffset is long minPrepare)
        {
            var clampFloor = minPrepare - 1;
            if (offset > clampFloor)
            {
                offset = clampFloor;
            }
        }

        if (offset < current)
        {
            // The clamp drove the requested offset back behind the
            // current materialised position. Silent no-op: the caller's
            // intent is preserved by the still-buffered prepare, and a
            // subsequent SetCheckpointOffsetAsync after the prepare's
            // terminal mark will be free to advance.
            return;
        }

        if (offset == current)
        {
            // Idempotent re-assert is a force-flush signal: durably
            // commit any in-memory Apply work issued since the previous
            // persist, even if the offset itself has not advanced.
            await FlushPendingCheckpointAsync(persistEvenWithoutPendingAdvance: true);
            return;
        }

        _pendingCheckpointOffset = offset;

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
        if (_pendingCheckpointOffset is { } pending)
        {
            state.State.ProjectionCheckpointOffset = pending;
            _pendingCheckpointOffset = null;
            await PersistAsync();
            _lastCheckpointPersistTimestamp = Stopwatch.GetTimestamp();
            await ReportCursorIfActiveAsync();
            return;
        }

        if (persistEvenWithoutPendingAdvance)
        {
            await PersistAsync();
            _lastCheckpointPersistTimestamp = Stopwatch.GetTimestamp();
            await ReportCursorIfActiveAsync();
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
        AddPreparedMutation(mutation.TransactionId, mutation.Key, incoming);
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
        List<string>? toRewrite = null;
        foreach (var (key, _) in state.State.Entries)
        {
            if (string.CompareOrdinal(key, startInclusive) < 0)
                continue;
            if (string.CompareOrdinal(key, endExclusive) >= 0)
                break;
            (toRewrite ??= []).Add(key);
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
        StoreEntry(key, incoming);
    }

    private void AdvanceProjectionClock(HybridLogicalClock incoming)
    {
        if (incoming > state.State.Clock)
            state.State.Clock = incoming;
    }
}

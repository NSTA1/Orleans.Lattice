using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-leaf in-memory pending-transaction map for the saga
/// reader-isolation primitive. Prepared mutations route here instead of
/// the visible projection until the saga's terminal mark
/// (<see cref="MutationKind.TxCommit"/> or
/// <see cref="MutationKind.TxAbort"/>) flips or drops them.
/// <para>
/// Strictly in-memory: under the WAL-as-sole-commit-point model the WAL
/// is the durable record, and the pending-tx map is rebuilt
/// deterministically on activation from the WAL replay. Reads filter
/// pending entries via a local hash lookup with zero RPC cost.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Keyed by <see cref="LatticeMutation.TransactionId"/> -&gt; key
    /// -&gt; the prepared <see cref="LwwValue{T}"/>. Entries here are
    /// invisible to readers until a matching terminal mark surfaces; on
    /// <see cref="MutationKind.TxCommit"/> every value is merged into
    /// <c>state.State.Entries</c> via
    /// <see cref="LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/>; on
    /// <see cref="MutationKind.TxAbort"/> every value is dropped.
    /// <para>
    /// Lazily allocated on the first prepared-mutation apply. The vast
    /// majority of leaves never participate in a saga, so an upfront
    /// allocation per activation would be pure waste — leaf activation
    /// density is the dominant memory-cost knob and the dict's empty
    /// footprint (~80 B) multiplied across thousands of activations is
    /// not free.
    /// </para>
    /// </summary>
    private Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>>? _pendingTx;

    /// <summary>
    /// Per-transaction earliest WAL offset of any prepared mutation
    /// recorded under that transaction id. Populated when the replay
    /// coordinator drives <c>ILeafProjection.Apply</c> with a
    /// <see cref="LatticeApplyOffsetContext"/> scope active; left
    /// untouched on the foreground commit path (where there is no WAL
    /// offset to stamp). The minimum value across this map is the
    /// projection-checkpoint clamp floor — advancing the persisted
    /// checkpoint past <c>min - 1</c> would silently lose any prepare
    /// whose terminal mark has not yet replayed, so
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> clamps
    /// requested advances back to that floor.
    /// <para>
    /// Lazily allocated on the first prepared-mutation apply that
    /// carries an ambient offset. The vast majority of leaves never
    /// participate in a saga or are not driven by the replay
    /// coordinator, so an upfront allocation per activation would be
    /// pure waste — see the rationale on <see cref="_pendingTx"/>.
    /// </para>
    /// </summary>
    private Dictionary<Guid, long>? _pendingTxOffsets;

    /// <summary>
    /// Idempotency dedup set. Populated as terminal marks replay so a
    /// re-applied <see cref="MutationKind.TxCommit"/> /
    /// <see cref="MutationKind.TxAbort"/> for the same transaction id is
    /// a no-op rather than crashing on a missing pending bucket.
    /// Survives only as long as the activation; rebuilt by the replay
    /// coordinator on next activation. Lazily allocated for the same
    /// reason as <see cref="_pendingTx"/>.
    /// </summary>
    private HashSet<Guid>? _recentlyTerminal;

    /// <summary>
    /// Records a prepared-phase per-key mutation in the pending-tx map.
    /// The entry is invisible to readers until a matching terminal mark
    /// flips or drops it. Idempotent under LWW: a re-applied prepare
    /// for the same <c>(txid, key)</c> uses
    /// <see cref="LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/> so the
    /// strictly-greater HLC always wins.
    /// </summary>
    private void AddPreparedMutation(Guid transactionId, string key, in LwwValue<byte[]> incoming)
    {
        if (transactionId == Guid.Empty)
        {
            // A prepared mutation must carry a non-empty transaction id
            // so the matching terminal mark can find it; surface this
            // as a programmer error rather than silently leaking the
            // mutation into a never-flushed bucket.
            throw new InvalidOperationException(
                "A prepared mutation must carry a non-empty TransactionId. "
                + "The saga coordinator stamps the id via LatticeTransactionContext "
                + "before opening a LatticePreparedContext scope.");
        }

        var pending = _pendingTx ??= new Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>>();
        if (!pending.TryGetValue(transactionId, out var bucket))
        {
            bucket = new Dictionary<string, LwwValue<byte[]>>();
            pending[transactionId] = bucket;
        }

        if (bucket.TryGetValue(key, out var existing))
        {
            bucket[key] = LwwValue<byte[]>.Merge(existing, incoming);
        }
        else
        {
            bucket[key] = incoming;
        }

        // Record the earliest WAL offset of any prepare under this
        // transaction id, but only when an apply scope is active —
        // foreground commits author the WAL and have no offset to
        // stamp, so they leave _pendingTxOffsets untouched and the
        // checkpoint clamp degrades to a no-op for foreground-only
        // leaves.
        var ambientOffset = LatticeApplyOffsetContext.Current;
        if (ambientOffset is long offset)
        {
            var offsets = _pendingTxOffsets ??= new Dictionary<Guid, long>();
            if (offsets.TryGetValue(transactionId, out var existingOffset))
            {
                if (offset < existingOffset)
                {
                    offsets[transactionId] = offset;
                }
            }
            else
            {
                offsets[transactionId] = offset;
            }
        }
    }

    /// <summary>
    /// Flips every pending-tx entry under <paramref name="transactionId"/>
    /// into the visible projection via
    /// <see cref="LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/>. The
    /// linearization point for the saga on this leaf — every reader
    /// observes either zero of the saga's keys or every one of them
    /// after this call returns. Idempotent: repeated applies for the
    /// same transaction id are no-ops via
    /// <see cref="_recentlyTerminal"/>.
    /// </summary>
    private void ApplyTxCommit(Guid transactionId)
    {
        if (transactionId == Guid.Empty)
            return;

        // Fast-path: leaf never saw a prepared mutation. Record the
        // terminal so a late-arriving prepared mutation under the same
        // id does not silently leak, then exit without touching
        // _pendingTx (which may still be null).
        if (_pendingTx is null || !_pendingTx.Remove(transactionId, out var bucket))
        {
            _pendingTxOffsets?.Remove(transactionId);
            (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
            return;
        }

        foreach (var kvp in bucket)
        {
            // LWW-merge each prepared value into the visible projection
            // exactly as a non-prepared Set would. The HLC stamped on
            // the prepare phase by the leaf grain is strictly greater
            // than any prior commit for the same key, so the prepared
            // value wins LWW even against a concurrent non-saga write
            // that happened to interleave with the saga.
            StoreEntry(kvp.Key, kvp.Value);
            AdvanceProjectionClock(kvp.Value.Timestamp);
        }

        _pendingTxOffsets?.Remove(transactionId);
        (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
    }

    /// <summary>
    /// Drops every pending-tx entry under <paramref name="transactionId"/>
    /// without ever making it visible to readers — the saga's
    /// prepare-phase writes are undone in a single linearization step.
    /// Idempotent.
    /// </summary>
    private void ApplyTxAbort(Guid transactionId)
    {
        if (transactionId == Guid.Empty)
            return;

        _pendingTx?.Remove(transactionId);
        _pendingTxOffsets?.Remove(transactionId);
        (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
    }

    /// <summary>
    /// Returns <c>true</c> if any pending-tx entry under any transaction
    /// id covers <paramref name="key"/>. Used by the read-path filter
    /// to hide saga prepare-phase writes from concurrent readers
    /// without a per-call RPC. O(pending-txs) — bounded by the small
    /// cardinality of in-flight sagas and the concurrent saga rate;
    /// returns immediately when the pending-tx map has never been
    /// allocated (the steady state for every leaf that has not
    /// participated in a saga since activation).
    /// </summary>
    private bool IsKeyPending(string key)
    {
        if (_pendingTx is null || _pendingTx.Count == 0)
            return false;

        foreach (var bucket in _pendingTx.Values)
        {
            if (bucket.ContainsKey(key))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Pending-transaction count snapshot for tests. Not on any
    /// public surface.
    /// </summary>
    internal int PendingTransactionCount => _pendingTx?.Count ?? 0;

    /// <summary>
    /// Recently-terminal count snapshot for tests. Not on any
    /// public surface.
    /// </summary>
    internal int RecentlyTerminalCount => _recentlyTerminal?.Count ?? 0;

    /// <summary>
    /// Returns the minimum WAL offset across every unresolved
    /// pending-tx prepare on this leaf, or <c>null</c> when no
    /// prepare-with-offset is currently buffered. Used by
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> to clamp
    /// the persisted checkpoint to <c>min(requested, value - 1)</c>
    /// so crash recovery does not advance past an unresolved prepare.
    /// O(pending-txs) — bounded by the small cardinality of in-flight
    /// sagas; returns immediately when the offset map has never been
    /// allocated (the steady state for foreground-driven leaves).
    /// </summary>
    internal long? MinUnresolvedPrepareOffset
    {
        get
        {
            if (_pendingTxOffsets is null || _pendingTxOffsets.Count == 0)
                return null;

            long min = long.MaxValue;
            foreach (var offset in _pendingTxOffsets.Values)
            {
                if (offset < min)
                    min = offset;
            }
            return min;
        }
    }

    /// <inheritdoc />
    public Task ApplyTxTerminalAsync(Guid transactionId, bool committed)
    {
        if (transactionId == Guid.Empty)
            return Task.CompletedTask;

        // Idempotency dedup: a re-broadcast under the same transaction
        // id (e.g. after a coordinator retry on a transient shard-root
        // RPC failure) is a no-op rather than a redundant projection
        // update.
        if (_recentlyTerminal is not null && _recentlyTerminal.Contains(transactionId))
            return Task.CompletedTask;

        // Fast-path: leaf never saw a prepared mutation under this id.
        // Record the terminal for late-arriving prepares and return.
        var hadPending = _pendingTx is not null && _pendingTx.ContainsKey(transactionId);
        if (!hadPending)
        {
            (_recentlyTerminal ??= new HashSet<Guid>()).Add(transactionId);
            return Task.CompletedTask;
        }

        if (committed)
            ApplyTxCommit(transactionId);
        else
            ApplyTxAbort(transactionId);

        // Zero leaf I/O: the terminal mark is durable on the per-shard
        // WAL (appended by the shard root before this RPC fans out),
        // and the in-memory flip is reconstructed on activation by the
        // replay coordinator driving Apply over the WAL slice. The
        // foreground commit path already commits zero leaf-state
        // writes under the zero-leaf-I/O contract — the terminal
        // handler must hold the same contract or it would re-introduce
        // the leaf-state shadow write the project explicitly removed.
        return Task.CompletedTask;
    }
}

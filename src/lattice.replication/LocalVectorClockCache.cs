using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Per-<c>(silo, tree)</c> in-memory local vector clock cache used by
/// the producer-side commit-time observer to stamp a tree-global
/// vector clock onto every emitted <see cref="WalRecord"/> when the
/// caller has not supplied one via
/// <see cref="LatticeVectorClockContext"/>.
/// <para>
/// Without this cache, multi-shard user writes (range delete, multi-leaf
/// saga, multi-shard fan-out) produce per-grain vector clocks that
/// disagree on cross-shard origins because each grain's local
/// observation of <see cref="IReplicationHighWaterMarkGrain.GetVectorAsync"/>
/// reflects only the inbound applies routed to that activation. The
/// cache provides a consistent silo-wide producer view: a single
/// cluster-wide cold-start RPC per tree per silo lifetime, then in-process
/// monotonic advances on local WAL append and foreign apply.
/// </para>
/// <para>
/// Concurrency model: per-tree lock-protected <see cref="VersionVector"/>
/// stored in a <see cref="ConcurrentDictionary{TKey, TValue}"/> keyed by
/// tree id. Cold-start is single-flight per tree: the first caller per
/// tree initiates the
/// <see cref="IReplicationHighWaterMarkGrain.GetVectorAsync"/> RPC and
/// concurrent readers await the same task. Advances apply in-process
/// without any grain hop and are pointwise-max under the per-tree lock.
/// </para>
/// <para>
/// The cache stores per-origin HLCs that semantically represent two
/// distinct facts:
/// <list type="bullet">
///   <item><description>
///   <em>Local diagonal</em>: the highest HLC the silo has appended to
///   the WAL for the local cluster id. The receiver-side HWM grain
///   never advances this entry (the apply pipeline filters local-origin
///   entries before reaching <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/>),
///   so the cache is the only seam that tracks it. Producers read this
///   entry to stamp <see cref="WalRecord.VectorClock"/> on outbound
///   entries.
///   </description></item>
///   <item><description>
///   <em>Foreign entries</em>: the highest HLC the silo has applied
///   from each remote origin. Mirrors the receiver-side HWM grain so
///   the producer view is internally consistent without re-issuing
///   <see cref="IReplicationHighWaterMarkGrain.GetAsync"/> per emit.
///   </description></item>
/// </list>
/// </para>
/// <para>
/// The cache is intentionally non-persistent: a silo restart loses the
/// in-memory state, the next emit per tree triggers a fresh cold-start
/// RPC, and the local diagonal re-converges as new local writes append
/// to the WAL. Worst case is bounded re-emission of slightly older VC
/// frontiers across the restart boundary; receivers tolerate this
/// because the per-origin high-water-mark check is the authoritative
/// dedupe key.
/// </para>
/// </summary>
internal sealed class LocalVectorClockCache(IGrainFactory grainFactory)
{
    /// <summary>
    /// Per-tree state, lazily created on first observation of a tree id.
    /// The map itself is concurrent because Orleans grain calls into a
    /// singleton cache may interleave across trees; per-tree concurrency
    /// is enforced by each <see cref="TreeState"/>'s internal lock.
    /// </summary>
    private readonly ConcurrentDictionary<string, TreeState> _trees =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Returns a defensive snapshot of the cached local vector clock
    /// for <paramref name="treeId"/>. The first call per tree triggers
    /// a single
    /// <see cref="IReplicationHighWaterMarkGrain.GetVectorAsync"/> RPC
    /// to seed initial state from the receiver-side HWM grain;
    /// subsequent calls return the in-memory state without a grain hop.
    /// </summary>
    /// <param name="treeId">The tree whose local vector clock to snapshot.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task<VersionVector> GetSnapshotAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var state = _trees.GetOrAdd(
            treeId,
            static (id, factory) => new TreeState(id, factory),
            grainFactory);
        return state.GetSnapshotAsync(cancellationToken);
    }

    /// <summary>
    /// Advances the local cluster's diagonal entry monotonically. Called
    /// from <see cref="ShardedReplogSink"/> after a successful WAL append
    /// for an entry whose <see cref="WalRecord.OriginClusterId"/>
    /// matches the local cluster id. No-op when
    /// <paramref name="candidate"/> is less than or equal to the
    /// currently cached value (the advance is pointwise-max).
    /// </summary>
    /// <param name="treeId">The tree whose diagonal to advance.</param>
    /// <param name="originClusterId">
    /// The local cluster id (the diagonal entry to advance).
    /// </param>
    /// <param name="candidate">
    /// The candidate HLC. Typically the just-appended entry's
    /// <see cref="WalRecord.Timestamp"/>.
    /// </param>
    public void AdvanceLocal(string treeId, string originClusterId, HybridLogicalClock candidate)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        var state = _trees.GetOrAdd(
            treeId,
            static (id, factory) => new TreeState(id, factory),
            grainFactory);
        state.Advance(originClusterId, candidate);
    }

    /// <summary>
    /// Advances a foreign origin's entry monotonically. Called from
    /// <see cref="ReplicationApplier"/> after a successful
    /// <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/> so
    /// the producer's view of foreign progress mirrors the receiver-side
    /// HWM grain. No-op when <paramref name="candidate"/> is less than
    /// or equal to the currently cached value (the advance is
    /// pointwise-max).
    /// </summary>
    /// <param name="treeId">The tree whose vector to advance.</param>
    /// <param name="originClusterId">
    /// The foreign origin cluster id whose entry to advance.
    /// </param>
    /// <param name="candidate">
    /// The candidate HLC. Typically the just-applied entry's
    /// <see cref="WalRecord.Timestamp"/>.
    /// </param>
    public void AdvanceForeign(string treeId, string originClusterId, HybridLogicalClock candidate)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        var state = _trees.GetOrAdd(
            treeId,
            static (id, factory) => new TreeState(id, factory),
            grainFactory);
        state.Advance(originClusterId, candidate);
    }

    /// <summary>
    /// Per-tree state. Holds the cached <see cref="VersionVector"/>
    /// behind a per-tree lock plus a single-flight cold-start task so
    /// concurrent first-readers share one
    /// <see cref="IReplicationHighWaterMarkGrain.GetVectorAsync"/> RPC.
    /// </summary>
    private sealed class TreeState(string treeId, IGrainFactory grainFactory)
    {
        private readonly Lock _gate = new();
        private readonly VersionVector _vector = new();
        private Task? _coldStart;

        public Task<VersionVector> GetSnapshotAsync(CancellationToken cancellationToken)
        {
            Task coldStart;
            lock (_gate)
            {
                coldStart = _coldStart ??= LoadFromGrainAsync();

                // Steady-state fast path: once the cold-start RPC has
                // completed successfully, every subsequent producer emit
                // can clone the cached vector synchronously. Returning a
                // pre-completed Task<VersionVector> via Task.FromResult
                // avoids the per-call AwaitColdStartThenSnapshotAsync
                // state-machine box (~80–100 B) on the dominant hot path
                // (every emit after the first one per (silo, tree)).
                // The clone is taken under the lock for the same reason
                // the await-path's clone is - a concurrent Advance must
                // not race with the dictionary copy.
                if (coldStart.IsCompletedSuccessfully && !cancellationToken.IsCancellationRequested)
                {
                    return Task.FromResult(_vector.Clone());
                }
            }

            return AwaitColdStartThenSnapshotAsync(coldStart, cancellationToken);
        }

        private async Task<VersionVector> AwaitColdStartThenSnapshotAsync(
            Task coldStart,
            CancellationToken cancellationToken)
        {
            // WaitAsync respects the caller's cancellation token without
            // cancelling the underlying single-flight RPC, so a cancelled
            // reader does not abort other concurrent waiters.
            await coldStart.WaitAsync(cancellationToken).ConfigureAwait(false);
            lock (_gate)
            {
                return _vector.Clone();
            }
        }

        private async Task LoadFromGrainAsync()
        {
            // Yield before any work so the catch block's `_coldStart = null`
            // reset is guaranteed to run as a continuation, after the caller's
            // `_coldStart ??= LoadFromGrainAsync()` has stored this task.
            // Without the yield, a grain stub that throws synchronously
            // (NSubstitute callbacks, argument validation, etc.) makes
            // `LoadFromGrainAsync()` return a *completed* Task, the catch's
            // null-reset clobbers a still-null field, the caller then assigns
            // the completed Task into `_coldStart`, and subsequent calls reuse
            // it instead of retrying the cold-start.
            await Task.Yield();
            var grain = grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeId);
            // Cold-start RPC runs without a caller-supplied cancellation
            // token: a single cancelled reader must not tear down the
            // shared single-flight task and force every other reader to
            // restart.
            VersionVector? snapshot = null;
            try
            {
                snapshot = await grain.GetVectorAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort cold-start: a transient grain failure
                // leaves the cache empty for this tree and the next
                // GetSnapshotAsync call retries the cold-start (the
                // _coldStart task is reset to null below). Producer
                // correctness is not at risk: the receiver-side
                // per-origin high-water-mark check is the authoritative
                // dedupe key, and a producer emitting an empty VC
                // frontier simply causes receivers to perform the
                // dep-check against an empty set, which always succeeds.
                // Faulting the producer's emit on a transient HWM grain
                // failure would be a strictly-worse outcome than
                // emitting with a stale (or empty) frontier, which is
                // the canonical fail-safe shape for this kind of
                // best-effort optimisation.
                lock (_gate)
                {
                    _coldStart = null;
                }
                return;
            }

            if (snapshot is not null)
            {
                lock (_gate)
                {
                    // MergeFrom is pointwise-max so any advances that landed
                    // concurrently with the cold-start RPC are preserved
                    // (their value is at least the grain's snapshot, by
                    // construction of the local emit / apply paths).
                    _vector.MergeFrom(snapshot);
                }
            }
        }

        public void Advance(string originClusterId, HybridLogicalClock candidate)
        {
            lock (_gate)
            {
                var existing = _vector.GetClock(originClusterId);
                if (candidate > existing)
                {
                    _vector.Entries[originClusterId] = candidate;
                }
            }
        }
    }
}

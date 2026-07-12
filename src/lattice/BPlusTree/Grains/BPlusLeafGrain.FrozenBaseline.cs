using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Frozen-baseline capture partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>. Adds the
/// two read-only seams the per-shard snapshot-baseline capture
/// (<see cref="ShardRootGrain.CaptureSnapshotBaselineAsync"/>) drives across a
/// shard's leaf chain:
/// <list type="number">
/// <item><description>
/// <see cref="FreezeProjectionAsync"/> - copies this leaf's committed cache,
/// its per-partition projection frontier, and its in-flight prepared sagas
/// into a serializable <see cref="LeafBaselineFreeze"/> without mutating any
/// leaf state.
/// </description></item>
/// <item><description>
/// <see cref="FoldTailOntoFrozenAsync"/> - replays only this leaf's own
/// <c>(frontier_p, capturedHead_p]</c> WAL tail on top of the frozen cache
/// (re-seeded from the freeze) and returns the materialised rows, which the
/// shard root unions into the durable per-shard
/// <see cref="State.SnapshotShardBaseline"/>.
/// </description></item>
/// </list>
/// Splitting freeze and fold lets the shard root capture <c>capturedHead</c>
/// uniformly <b>after</b> every leaf has frozen, guaranteeing
/// <c>frontier_p &lt;= capturedHead_p</c> with no overshoot, while folding each
/// leaf's tail exactly once (CRDT folds are not idempotent). The frozen state
/// travels to the shard root and back rather than being held on the leaf
/// between the two calls, so a leaf reactivation in that window cannot lose it.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task<LeafBaselineFreeze> FreezeProjectionAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var resolved = await GetOptionsAsync();
        var partitionCount = Math.Max(1, resolved.WalPartitions);

        var treeId = state.State.TreeId
            ?? throw new InvalidOperationException(
                "FreezeProjectionAsync was called on a leaf whose tree id is unset; the shard root "
                + "must only freeze leaves it has already attached.");

        // Read each partition's WAL head BEFORE copying the cache. This leaf is
        // non-reentrant, so no foreground write on this activation can interleave
        // between the head read and the cache copy: every owned record at an
        // offset below the head has already been folded into the warm cache, and
        // any record appended at or above the head lands in the fold tail. The
        // head is therefore the exact per-partition frontier the cache reflects,
        // unlike the persisted checkpoint, which lags the warm cache and would
        // make the tail re-fold records already present (double-counting CRDTs).
        var heads = await CaptureWalHeadsByPartitionAsync(treeId);

        // Copy the committed cache rows under this single grain turn; the
        // resulting list is a self-contained value snapshot that survives
        // subsequent foreground mutations on this activation.
        var rows = new List<LeafSnapshotRow>(Cache.Count);
        foreach (var kv in Cache.EnumerateRows())
        {
            rows.Add(new LeafSnapshotRow(kv.Key, kv.Value, Cache.GetMergeMode(kv.Key)));
        }

        // Per-partition frontier the cache already reflects, expressed as the
        // ReadSliceAsync fromExclusive lower bound. The head is exclusive (a
        // record count), so the cache spans [0, head) and the highest cached
        // offset is head - 1; the fold tail (frontier, capturedHead] therefore
        // begins at the first un-cached offset and applies every WAL record
        // exactly once relative to this frozen cache. An empty partition
        // (head 0) yields the -1 "from the beginning" sentinel.
        var frontier = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++)
        {
            var head = p < heads.Length ? heads[p] : 0;
            frontier[p] = head - 1;
        }

        var pending = FlattenPendingForFreeze();

        return new LeafBaselineFreeze
        {
            Rows = rows,
            FrontierPerPartition = frontier,
            Pending = pending,
        };
    }

    /// <summary>
    /// Flattens the leaf's in-memory prepared-saga buckets
    /// (<see cref="_pendingTx"/> plus the parallel
    /// <see cref="_pendingTxDeltas"/> CRDT side-map) into a flat, serializable
    /// list. A prepared saga whose terminal lands in the fold tail must be
    /// re-seeded into the capture-time fold first, or the tail's terminal
    /// would drain an empty bucket and silently lose the committed write.
    /// </summary>
    private IReadOnlyList<LeafBaselinePendingEntry> FlattenPendingForFreeze()
    {
        if (_pendingTx is null || _pendingTx.Count == 0)
        {
            return Array.Empty<LeafBaselinePendingEntry>();
        }

        var pending = new List<LeafBaselinePendingEntry>();
        foreach (var (txId, bucket) in _pendingTx)
        {
            var deltas = _pendingTxDeltas is not null
                && _pendingTxDeltas.TryGetValue(txId, out var db)
                ? db
                : null;
            foreach (var (key, value) in bucket)
            {
                byte[]? delta = null;
                var mode = LatticeMergeMode.LwwRegister;
                if (deltas is not null && deltas.TryGetValue(key, out var dm))
                {
                    delta = dm.Delta;
                    mode = dm.Mode;
                }
                pending.Add(new LeafBaselinePendingEntry(txId, key, value, delta, mode));
            }
        }
        return pending;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<LeafSnapshotRow>> FoldTailOntoFrozenAsync(
        LeafBaselineFreeze freeze,
        long[] capturedHead,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(freeze);
        ArgumentNullException.ThrowIfNull(capturedHead);
        cancellationToken.ThrowIfCancellationRequested();

        var treeId = state.State.TreeId
            ?? throw new InvalidOperationException(
                "FoldTailOntoFrozenAsync was called on a leaf whose tree id is unset; the shard root "
                + "must only fold baseline tails for leaves it has already attached.");

        var folder = new SnapshotProjectionFolder(treeId, ResolveCrdtShapeRegistry(), ResolveEnvelopeCodec());

        // Seed the frozen committed cache and prepared sagas before the tail
        // fold so a terminal in the tail resolves against the real prepared
        // mutation, exactly as a live activation's two-pass replay does.
        foreach (var row in freeze.Rows)
        {
            folder.SeedRow(row.Key, row.Value, row.MergeMode);
        }
        foreach (var p in freeze.Pending)
        {
            folder.SeedPending(p.TransactionId, p.Key, p.Value, p.Delta, p.Mode);
        }

        var replayShardMap = await ResolveReplayShardMapAsync(treeId);
        var frontier = freeze.FrontierPerPartition;
        var partitionCount = Math.Min(frontier.Count, capturedHead.Length);

        // Pass 1: fold each partition's own (frontier, capturedHead] tail.
        // Saga terminals and DeleteRange are deferred to pass 2 for the same
        // multi-partition saga-atomicity and tombstone-ordering reasons the
        // live activation replay defers them.
        var deferred = new List<LatticeMutation>();
        for (var partition = 0; partition < partitionCount; partition++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var fromExclusive = frontier[partition];
            var toInclusive = capturedHead[partition] - 1;
            if (fromExclusive >= toInclusive)
                continue;

            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{treeId}/{partition}");

            while (fromExclusive < toInclusive)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var slice = await coordinator.ReadSliceAsync(
                    fromExclusive,
                    toInclusive,
                    ReplaySliceBudget,
                    cancellationToken);

                if (slice.Count == 0)
                    break;

                foreach (var entry in slice)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!ShouldApplyDuringReplay(
                        entry.Mutation,
                        state.State.ShardIndex,
                        state.State.LowKeyInclusive,
                        state.State.HighKeyExclusive,
                        replayShardMap))
                    {
                        continue;
                    }

                    if (SnapshotProjectionFolder.IsDeferredKind(entry.Mutation.Kind))
                    {
                        deferred.Add(entry.Mutation);
                    }
                    else
                    {
                        folder.Apply(entry.Mutation);
                    }
                }

                var lastOffset = slice[^1].Offset;
                if (lastOffset <= fromExclusive)
                    break; // defensive: never spin if the slice failed to advance.
                fromExclusive = lastOffset;
            }
        }

        // Pass 2: drain every deferred saga terminal and range-delete tombstone
        // once every partition's per-key Set/Delete/prepare records have been
        // absorbed into the folder.
        foreach (var mutation in deferred)
        {
            cancellationToken.ThrowIfCancellationRequested();
            folder.Apply(mutation);
        }

        return folder.Materialize();
    }
}

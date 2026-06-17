using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Apply-side seam used by <c>Orleans.Lattice.Replication</c>. Routes a
/// remote mutation to the owning shard with the source HLC and origin
/// cluster id preserved verbatim, so the persisted
/// <see cref="LwwValue{T}"/> matches the authoring cluster's metadata
/// exactly.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public Task ApplySetAsync(
        string key,
        byte[] value,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        long expiresAtTicks)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        var lww = LwwValue<byte[]>.CreateWithExpiry(value, sourceHlc, expiresAtTicks)
            with
            {
                OriginClusterId = originClusterId,
                VectorClock = sourceVectorClock,
            };

        return ApplyMergeOneAsync(key, lww);
    }

    /// <inheritdoc />
    public Task ApplyDeleteAsync(
        string key,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        var tombstone = LwwValue<byte[]>.Tombstone(sourceHlc)
            with
            {
                OriginClusterId = originClusterId,
                VectorClock = sourceVectorClock,
            };

        return ApplyMergeOneAsync(key, tombstone);
    }

    /// <inheritdoc />
    public async Task ApplyDeleteRangeAsync(
        string startInclusive,
        string endExclusive,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        IReadOnlyList<string>? explicitMatchedKeys = null)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        if (string.CompareOrdinal(startInclusive, endExclusive) >= 0)
        {
            return;
        }

        // A predicate-filtered range delete ships the explicit set of keys the
        // authoring leaf matched. The receiver must tombstone exactly that set
        // - never re-deriving membership from the range bounds (its stored
        // values may differ, and re-evaluating a predicate it does not carry is
        // impossible). Route those keys through the batched merge-apply path as
        // tombstone items so each lands on its owning shard with the producer's
        // origin / frontier / HLC preserved bit-identically.
        if (explicitMatchedKeys is not null)
        {
            if (explicitMatchedKeys.Count == 0)
            {
                return;
            }

            var items = new ApplyMergeItem[explicitMatchedKeys.Count];
            for (var i = 0; i < explicitMatchedKeys.Count; i++)
            {
                items[i] = new ApplyMergeItem
                {
                    Key = explicitMatchedKeys[i],
                    Value = null,
                    SourceHlc = sourceHlc,
                    OriginClusterId = originClusterId,
                    SourceVectorClock = sourceVectorClock,
                    ExpiresAtTicks = 0,
                    IsTombstone = true,
                };
            }

            await ApplyMergeManyAsync(items);
            return;
        }

        // Wrap the range walk in LatticeOriginContext + LatticeVectorClockContext
        // scopes so the per-leaf tombstones produced by the local walk are
        // stamped with the remote origin and the remote frontier. The
        // shard-root range-delete observer then publishes a single
        // per-shard mutation that carries both pieces of metadata, and the
        // outbound ship loop filters the resulting WAL entries back out -
        // preventing the range from looping back to the authoring cluster.
        //
        // Additionally pin every per-leaf tombstone to the producer's
        // issue HLC via LatticeHlcOverrideContext so the cross-origin LWW
        // invariant is preserved: a DeleteRange authored at frontier T
        // must not overwrite a foreign-origin write whose HLC is strictly
        // greater than T. The override is suppressed when sourceHlc is
        // HybridLogicalClock.Zero - that wire-default is produced by
        // legacy peers that pre-date the parameter and the receiver falls
        // back to the historical fresh-local-HLC stamping path so
        // single-cluster and same-version multi-cluster topologies are
        // unaffected by the wire-shape change.
        using var originScope = LatticeOriginContext.With(originClusterId);
        using var vcScope = LatticeVectorClockContext.With(sourceVectorClock);
        // Pin every per-leaf tombstone to the producer's issue HLC via
        // LatticeHlcOverrideContext so the cross-origin LWW invariant is
        // preserved: a DeleteRange authored at frontier T must not
        // overwrite a foreign-origin write whose HLC is strictly greater
        // than T. Passing Zero (the wire-default produced by legacy
        // peers that pre-date the sourceHlc parameter) yields a
        // null-override scope - the leaf falls back to the historical
        // fresh-local-HLC stamping path so single-cluster and
        // same-version multi-cluster topologies are unaffected by the
        // wire-shape change.
        using var hlcScope = LatticeHlcOverrideContext.With(
            sourceHlc == HybridLogicalClock.Zero ? null : sourceHlc);

        await RetryOnStaleRoutingAsync(
            (self: this, startInclusive, endExclusive),
            static args => args.self.ApplyDeleteRangeCoreAsync(args.startInclusive, args.endExclusive),
            CancellationToken.None);
    }

    /// <summary>
    /// Routes a single LWW entry through <see cref="IShardRootGrain.MergeManyAsync"/>
    /// - the only entry point that preserves the source HLC end-to-end -
    /// retrying once for each of the three transient routing-staleness
    /// classes the public write paths handle (stale shard map, stale tree
    /// alias, and the <see cref="InvalidOperationException"/> the registry
    /// raises when a virtual tree id maps to an evicted physical tree).
    /// </summary>
    private async Task ApplyMergeOneAsync(string key, LwwValue<byte[]> lww)
    {
        var batch = new Dictionary<string, LwwValue<byte[]>>(capacity: 1) { [key] = lww };
        await RetryOnStaleRoutingAsync(
            (self: this, key, batch),
            static async args =>
            {
                var shard = await args.self.GetShardGrainAsync(args.key);
                await shard.MergeManyAsync(args.batch);
            },
            CancellationToken.None);
    }

    private async Task ApplyDeleteRangeCoreAsync(string startInclusive, string endExclusive)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();
        var tasks = new Task<int>[physicalShards.Count];
        for (var i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            tasks[i] = shard.DeleteRangeAsync(startInclusive, endExclusive);
        }

        await Task.WhenAll(tasks);
    }

    /// <inheritdoc />
    public async Task ApplyMergeManyAsync(IReadOnlyList<ApplyMergeItem> items)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(items);

        if (items.Count == 0)
        {
            return;
        }

        if (items.Count == 1)
        {
            // Single-item fast path: no win from grouping, reuse the
            // existing single-item helper which already carries the
            // retry-on-stale-routing chain.
            var only = items[0];
            ArgumentNullException.ThrowIfNull(only.Key);
            ArgumentException.ThrowIfNullOrEmpty(only.OriginClusterId);
            await ApplyMergeOneAsync(only.Key, BuildApplyMergeLww(only));
            return;
        }

        await RetryOnStaleRoutingAsync(
            (self: this, items),
            static args => args.self.ApplyMergeManyCoreAsync(args.items),
            CancellationToken.None);
    }

    private async Task ApplyMergeManyCoreAsync(IReadOnlyList<ApplyMergeItem> items)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();

        // Group items by shard. Most batches in steady-state replication
        // come from a single producer's ship phase and will land on a
        // small number of shards, so we lazily promote from a single-shard
        // dictionary to a per-shard map only when a second shard appears.
        Dictionary<int, Dictionary<string, LwwValue<byte[]>>>? byShard = null;
        var firstShard = -1;
        Dictionary<string, LwwValue<byte[]>>? firstBatch = null;

        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            ArgumentNullException.ThrowIfNull(item.Key);
            ArgumentException.ThrowIfNullOrEmpty(item.OriginClusterId);

            var lww = BuildApplyMergeLww(item);
            var shardIndex = shardMap.Resolve(item.Key);

            if (firstBatch is null)
            {
                firstShard = shardIndex;
                firstBatch = new Dictionary<string, LwwValue<byte[]>>(capacity: items.Count)
                {
                    [item.Key] = lww,
                };
                continue;
            }

            if (byShard is null && shardIndex == firstShard)
            {
                firstBatch[item.Key] = lww;
                continue;
            }

            byShard ??= new Dictionary<int, Dictionary<string, LwwValue<byte[]>>>
            {
                [firstShard] = firstBatch,
            };

            if (!byShard.TryGetValue(shardIndex, out var batch))
            {
                batch = new Dictionary<string, LwwValue<byte[]>>();
                byShard[shardIndex] = batch;
            }

            batch[item.Key] = lww;
        }

        if (byShard is null)
        {
            // All items targeted a single shard.
            var shardKey = $"{physicalTreeId}/{firstShard}";
            var shard = grainFactory.GetGrain<IShardRootGrain>(shardKey);
            await shard.MergeManyAsync(firstBatch!);
            return;
        }

        var tasks = new Task[byShard.Count];
        var idx = 0;
        foreach (var (shardIndex, batch) in byShard)
        {
            var shardKey = $"{physicalTreeId}/{shardIndex}";
            var shard = grainFactory.GetGrain<IShardRootGrain>(shardKey);
            tasks[idx++] = shard.MergeManyAsync(batch);
        }

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Reconstructs the persisted <see cref="LwwValue{T}"/> for an
    /// <see cref="ApplyMergeItem"/>. Mirrors the per-entry shape used by
    /// <see cref="ApplySetAsync"/> (Set) and <see cref="ApplyDeleteAsync"/>
    /// (tombstone) so the batched path is bit-identical to the per-entry
    /// path on the wire, only with one shard RPC per shard instead of one
    /// per item.
    /// </summary>
    private static LwwValue<byte[]> BuildApplyMergeLww(ApplyMergeItem item)
    {
        if (item.IsTombstone)
        {
            return LwwValue<byte[]>.Tombstone(item.SourceHlc) with
            {
                OriginClusterId = item.OriginClusterId,
                VectorClock = item.SourceVectorClock,
            };
        }

        return LwwValue<byte[]>.CreateWithExpiry(item.Value!, item.SourceHlc, item.ExpiresAtTicks) with
        {
            OriginClusterId = item.OriginClusterId,
            VectorClock = item.SourceVectorClock,
        };
    }

    /// <inheritdoc />
    public async Task ApplyCrdtDeltaManyAsync(IReadOnlyList<ApplyCrdtDeltaItem> items)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(items);

        if (items.Count == 0)
        {
            return;
        }

        // Fold every delta inside this single grain turn. The grain is
        // non-reentrant, so no other apply or local write to this tree
        // interleaves between the per-item folds - that is what lets the
        // batch avoid the per-entry optimistic-concurrency retry loop the
        // applier-side read-merge-write path required. Each item routes
        // through the same producer-side ApplyCrdtDeltaAsync seam (which
        // resolves the registered CrdtShape, folds the typed delta into the
        // current visible state, and appends the delta-only WAL record),
        // wrapped in the source-cluster ambient scopes so the receiver
        // stamps the origin id, vector-clock frontier, and HLC verbatim
        // (LatticeHlcOverrideContext makes AdvanceClockOrOverride re-stamp
        // the source HLC instead of advancing the local clock). Items are
        // folded in arrival order; CRDT commutativity makes the converged
        // state independent of that order, and same-key deltas compose
        // because each fold reads the prior post-fold state back out.
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            ArgumentNullException.ThrowIfNull(item.Key);
            ArgumentNullException.ThrowIfNull(item.Delta);
            ArgumentException.ThrowIfNullOrEmpty(item.OriginClusterId);
            if (item.Mode == LatticeMergeMode.LwwRegister)
            {
                throw new ArgumentException(
                    "ApplyCrdtDeltaManyAsync does not accept LatticeMergeMode.LwwRegister items; "
                    + "LWW Set/Delete writes ride ApplyMergeManyAsync instead.",
                    nameof(items));
            }

            using (LatticeOriginContext.With(item.OriginClusterId))
            using (LatticeVectorClockContext.With(item.SourceVectorClock))
            using (LatticeHlcOverrideContext.With(item.SourceHlc))
            {
                await ApplyCrdtDeltaAsync(item.Key, item.Mode, item.Delta);
            }
        }
    }

    /// <inheritdoc />
    public async Task ApplyPreparedSetAsync(
        string key,
        byte[] value,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        long expiresAtTicks,
        Guid transactionId,
        int atomicBatchSize,
        int atomicBatchIndex,
        byte[]? delta = null,
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        if (transactionId == Guid.Empty)
        {
            throw new ArgumentException(
                "ApplyPreparedSetAsync requires a non-empty transactionId so the receiver leaf can route the entry into its per-tx pending bucket.",
                nameof(transactionId));
        }

#if LATTICE_DIAG
        Orleans.Lattice.BPlusTree.Grains.DiagSink.Write(
            $"[DIAG xc-apply-prepared-set] tree={TreeId} key={key} tx={transactionId} hlc={sourceHlc} " +
            $"origin={originClusterId} batchSize={atomicBatchSize} batchIndex={atomicBatchIndex} " +
            $"mode={mode} deltaLen={(delta?.Length ?? -1)}");
#endif

        // Re-establish the same ambient-context stack the source-side
        // saga's prepare step produced so the receiver leaf:
        //   - routes this mutation into its _pendingTx[transactionId]
        //     bucket (LatticePreparedContext);
        //   - re-stamps the source's HLC bit-identically
        //     (LatticeHlcOverrideContext);
        //   - persists OriginClusterId, VectorClock, AtomicBatchSize,
        //     AtomicBatchIndex, and TransactionId on the resulting
        //     LatticeMutation verbatim;
        //   - records the typed CRDT delta + merge mode in the leaf's
        //     pending-tx delta side-map (LatticeDeltaContext) so the
        //     saga's terminal commit folds the per-replica delta into
        //     the receiver's current visible state instead of installing
        //     the source's merged LWW value verbatim. A null delta (plain
        //     LWW prepared write) leaves the side-map untouched and keeps
        //     the byte-for-byte unchanged LWW terminal-drain path.
        // The terminal mark that arrives subsequently via
        // ApplyTxTerminalAsync flips the pending bucket into the
        // visible projection.
        LatticeTransactionContext.Set(transactionId);
        using (LatticeAtomicBatchContext.With(
            atomicBatchSize > 0 ? (atomicBatchSize, atomicBatchIndex) : null))
        using (LatticePreparedContext.BeginScope())
        using (LatticeOriginContext.With(originClusterId))
        using (LatticeVectorClockContext.With(sourceVectorClock))
        using (LatticeHlcOverrideContext.With(sourceHlc))
        using (mode != LatticeMergeMode.LwwRegister && delta is not null
            ? LatticeDeltaContext.With(delta)
            : null)
        {
            if (expiresAtTicks > 0)
            {
                var remainingTicks = expiresAtTicks - DateTimeOffset.UtcNow.UtcTicks;
                if (remainingTicks <= 0)
                {
                    // Absolute expiry already elapsed - treat as absent
                    // by routing as a tombstone, matching the
                    // public-read semantics for expired entries.
                    await DeleteAsync(key);
                }
                else
                {
                    await SetAsync(key, value, TimeSpan.FromTicks(remainingTicks));
                }
            }
            else
            {
                await SetAsync(key, value);
            }
        }
    }

    /// <inheritdoc />
    public async Task ApplyPreparedDeleteAsync(
        string key,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        Guid transactionId,
        int atomicBatchSize,
        int atomicBatchIndex)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        if (transactionId == Guid.Empty)
        {
            throw new ArgumentException(
                "ApplyPreparedDeleteAsync requires a non-empty transactionId so the receiver leaf can route the tombstone into its per-tx pending bucket.",
                nameof(transactionId));
        }

        LatticeTransactionContext.Set(transactionId);
        using (LatticeAtomicBatchContext.With(
            atomicBatchSize > 0 ? (atomicBatchSize, atomicBatchIndex) : null))
        using (LatticePreparedContext.BeginScope())
        using (LatticeOriginContext.With(originClusterId))
        using (LatticeVectorClockContext.With(sourceVectorClock))
        using (LatticeHlcOverrideContext.With(sourceHlc))
        {
            await DeleteAsync(key);
        }
    }

    /// <inheritdoc />
    public async Task ApplyTxTerminalAsync(
        Guid transactionId,
        bool committed,
        int shardIndex,
        HybridLogicalClock terminalHlc,
        string originClusterId,
        int atomicShardCount = 0,
        string? crossTreeOperationId = null,
        IReadOnlyList<string>? crossTreeWaitSet = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        if (transactionId == Guid.Empty)
        {
            throw new ArgumentException(
                "ApplyTxTerminalAsync requires a non-empty transactionId.",
                nameof(transactionId));
        }
        cancellationToken.ThrowIfCancellationRequested();

        // Step 1 (cross-cluster all-or-nothing visibility gate) -
        // Record this per-source-shard terminal arrival against the
        // per-tree TxRegistry's tally. While the tally is pending we
        // do NOT mark the registry and do NOT fan the terminal out to
        // the receiver's leaves: a receiver reader finding a pending
        // entry on a leaf dials back through the registry, gets
        // InFlight, and falls through to the pre-saga value. The
        // per-leaf pending buckets stay in place so the dial-back has
        // something to resolve against.
        //
        // Only when the tally is final (every per-source-shard
        // terminal observed, or the producer did not stamp a gate /
        // atomicShardCount == 0) do we (a) flip the per-tree
        // linearization mark to the saga's outcome and (b) fan the
        // terminal out to every observed source-shard's transitive
        // closure on the receiver. This delivers strict cross-cluster
        // atomic visibility: a reader concurrent with a multi-shard
        // SetManyAtomicAsync's replication observes either the
        // pre-saga value on every key or the post-saga value on every
        // key, never a partial subset.
        //
        // The gate handles producer/receiver shard-count divergence
        // naturally: the tally key is the source-side shard index
        // stamped on the incoming terminal, not the receiver's own
        // shard layout. A receiver whose adaptive splits or operator
        // resize have produced a different shard count than the
        // source still sees exactly atomicShardCount distinct source
        // terminals (one per source shard the saga touched). The
        // receiver's transitive split-forward closure runs per
        // observed source-shard so per-saga keys that have been
        // resharded locally still reach every destination.
        var registry = grainFactory.GetGrain<ITxRegistryGrain>(TreeId);
        var tally = await registry.RecordTerminalArrivalAsync(
            transactionId, shardIndex, committed, atomicShardCount);

#if LATTICE_DIAG
        Orleans.Lattice.BPlusTree.Grains.DiagSink.Write(
            $"[DIAG xc-tx-terminal-arrival] tree={TreeId} tx={transactionId} committed={committed} " +
            $"shardIndex={shardIndex} atomicShardCount={atomicShardCount} terminalHlc={terminalHlc} " +
            $"origin={originClusterId} tallyFinal={tally.IsFinal} observedShards=[{string.Join(",", tally.ObservedSourceShards)}]");
#endif

        if (!tally.IsFinal)
        {
            // Saga's per-source-shard tally is incomplete - leave the
            // registry mark unset and the receiver leaves' pending
            // buckets undrained so reads remain all-or-nothing. The
            // next arrival will re-evaluate.
            return;
        }

        if (string.IsNullOrEmpty(crossTreeOperationId))
        {
            // Legacy single-tree path: the per-shard gate is the only
            // barrier, so mark the per-tree linearization point and fan
            // the terminal out as soon as the gate completes.
            if (committed)
            {
                await registry.MarkCommittedAsync(transactionId);
            }
            else
            {
                await registry.MarkAbortedAsync(transactionId);
            }

            await ApplyTerminalPostGateAsync(
                transactionId, committed, tally.ObservedSourceShards,
                terminalHlc, originClusterId, cancellationToken);
            return;
        }

        // Cross-tree receiver barrier. Each participating tree's terminals
        // replicate independently, so a per-tree mark + fan-out here would let a
        // remote reader observe one tree committed while a sibling tree is still
        // pre-saga - a partial cross-tree view the authoring cluster never
        // exposes. Defer the mark and fan-out to a receiver coordinator that
        // flips every replicated participating tree visible together.
        //
        // Strict ordering (never reversed): (a) durably register this tree's
        // registry to delegate the sub-saga's status to the receiver
        // coordinator, THEN (b) durably notify the coordinator of this tree's
        // terminal. (a)-before-(b) guarantees no reader can resolve this tree
        // committed (local mark) while a sibling is still legacy-local: until
        // the coordinator decides, the delegated read returns InFlight.
        var receiverKey = LatticeCrossTreeReceiverGrain.ComputeKey(originClusterId, crossTreeOperationId);
        await registry.RegisterReceiverDecisionAuthorityAsync(transactionId, receiverKey);

        var waitSet = crossTreeWaitSet is { Count: > 0 } ? crossTreeWaitSet : new[] { TreeId };
        var coordinator = grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(receiverKey);
        var decision = await coordinator.NotifyTerminalAsync(new CrossTreeReceiverTerminal
        {
            OriginClusterId = originClusterId,
            OperationId = crossTreeOperationId,
            TreeId = TreeId,
            TransactionId = transactionId,
            Committed = committed,
            WaitSet = waitSet,
            ObservedSourceShards = tally.ObservedSourceShards,
            TerminalHlc = terminalHlc,
        });

        if (!decision.Decided)
        {
            // Barrier still pending other replicated participants. Leave every
            // tree's mark unset; delegated reads return InFlight everywhere.
            return;
        }

        // Barrier complete: materialize every tree's slice. The coordinator only
        // RETURNS data (never calls back), so finalizing here is deadlock-free.
        // Self-tree finalize is inline (no reentrancy); sibling trees are
        // finalized via their own apply grains. The full set is returned on
        // every decided notify, so a redelivered terminal re-heals
        // materialization idempotently.
        foreach (var finalize in decision.TreesToFinalize)
        {
            if (string.Equals(finalize.TreeId, TreeId, StringComparison.Ordinal))
            {
                await FinalizeCrossTreeTerminalCoreAsync(
                    finalize.TransactionId, decision.Committed, finalize.ObservedSourceShards,
                    finalize.TerminalHlc, finalize.OriginClusterId, cancellationToken);
            }
            else
            {
                var sibling = grainFactory.GetGrain<IReplicationApplyGrain>(finalize.TreeId);
                await sibling.FinalizeCrossTreeTerminalAsync(
                    finalize.TransactionId, decision.Committed, finalize.ObservedSourceShards,
                    finalize.TerminalHlc, finalize.OriginClusterId, cancellationToken);
            }
        }
    }

    /// <inheritdoc />
    public Task FinalizeCrossTreeTerminalAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyList<int> observedSourceShards,
        HybridLogicalClock terminalHlc,
        string originClusterId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        ArgumentNullException.ThrowIfNull(observedSourceShards);
        if (transactionId == Guid.Empty)
        {
            throw new ArgumentException(
                "FinalizeCrossTreeTerminalAsync requires a non-empty transactionId.",
                nameof(transactionId));
        }
        cancellationToken.ThrowIfCancellationRequested();

        return FinalizeCrossTreeTerminalCoreAsync(
            transactionId, committed, observedSourceShards, terminalHlc, originClusterId, cancellationToken);
    }

    /// <summary>
    /// Marks this tree's per-tree registry with the (global) verdict and fans
    /// the terminal out to the tree's leaves. Shared by the legacy single-tree
    /// path's sibling-less finalize and the cross-tree barrier's per-tree
    /// materialization.
    /// </summary>
    private async Task FinalizeCrossTreeTerminalCoreAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyList<int> observedSourceShards,
        HybridLogicalClock terminalHlc,
        string originClusterId,
        CancellationToken cancellationToken)
    {
        var registry = grainFactory.GetGrain<ITxRegistryGrain>(TreeId);
        if (committed)
        {
            await registry.MarkCommittedAsync(transactionId);
        }
        else
        {
            await registry.MarkAbortedAsync(transactionId);
        }

        await ApplyTerminalPostGateAsync(
            transactionId, committed, observedSourceShards,
            terminalHlc, originClusterId, cancellationToken);
    }

    /// <summary>
    /// Drives the per-shard terminal-mark fan-out for a saga whose
    /// linearization mark has already been recorded. Pre-resolves the
    /// transitive split-forward closure of every observed source-shard index
    /// via <see cref="TerminalFanOutResolver"/> (under cascading mid-saga
    /// splits on the receiver cluster, the inbound records' authoring-cluster
    /// shard indices may have further split locally), then drives the per-shard
    /// terminal-mark primitive under the source's HLC + origin so the receiver's
    /// local WAL append re-stamps the source cluster's terminal HLC and origin
    /// verbatim. The shard root's <c>ComputeTerminalHlcAsync</c> honours
    /// <see cref="LatticeHlcOverrideContext.Current"/> and returns the override
    /// unchanged - preserving the cross-cluster ordering invariant on receiver
    /// replays.
    /// </summary>
    private async Task ApplyTerminalPostGateAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyList<int> observedSourceShards,
        HybridLogicalClock terminalHlc,
        string originClusterId,
        CancellationToken cancellationToken)
    {
        using (LatticeOriginContext.With(originClusterId))
        using (LatticeHlcOverrideContext.With(terminalHlc))
        {
            var (physicalTreeId, _) = await GetRoutingAsync();
            var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
                grainFactory,
                physicalTreeId,
                observedSourceShards,
                cancellationToken);

            var tasks = new List<Task>(targets.Count);
            foreach (var target in targets)
            {
                tasks.Add(ApplyTerminalToShardAsync(
                    physicalTreeId, target, transactionId, committed, cancellationToken));
            }
            await Task.WhenAll(tasks);
        }
    }

    /// <summary>
    /// Per-shard terminal apply with a single stale-routing /
    /// stale-tree-alias retry, preserving the bounded retry semantics
    /// the cross-cluster replication apply path has always used for
    /// terminal marks. The resolver in
    /// <see cref="ApplyTxTerminalAsync"/> pre-fans the closure across
    /// every transitively-discovered destination, so each call here
    /// targets exactly one shard.
    /// </summary>
    private Task ApplyTerminalToShardAsync(
        string physicalTreeId,
        int shardIndex,
        Guid transactionId,
        bool committed,
        CancellationToken cancellationToken)
    {
        // Resolve the physical tree id on every retry attempt so that
        // after a stale-alias / stale-shard-map invalidation the loop
        // targets the freshly resolved physical tree, not the snapshot
        // captured at fan-out time. GetRoutingAsync hits the per-activation
        // cache on the steady-state path so this adds no per-call cost.
        return RetryOnStaleRoutingAsync(
            (self: this, grainFactory, transactionId, shardIndex, committed, cancellationToken),
            static async args =>
            {
                var (resolvedPhysicalTreeId, _) = await args.self.GetRoutingAsync();
                var shard = args.grainFactory.GetGrain<IShardRootGrain>($"{resolvedPhysicalTreeId}/{args.shardIndex}");
                await shard.AppendTxTerminalAsync(args.transactionId, args.committed, committedValues: null, args.cancellationToken);
            },
            cancellationToken);
    }
}

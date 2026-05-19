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
        VersionVector? sourceVectorClock)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        if (string.CompareOrdinal(startInclusive, endExclusive) >= 0)
        {
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

        try
        {
            await ApplyDeleteRangeCoreAsync(startInclusive, endExclusive);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            await ApplyDeleteRangeCoreAsync(startInclusive, endExclusive);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            await ApplyDeleteRangeCoreAsync(startInclusive, endExclusive);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            await ApplyDeleteRangeCoreAsync(startInclusive, endExclusive);
        }
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
        try
        {
            var shard = await GetShardGrainAsync(key);
            await shard.MergeManyAsync(batch);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            var shard = await GetShardGrainAsync(key);
            await shard.MergeManyAsync(batch);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            var shard = await GetShardGrainAsync(key);
            await shard.MergeManyAsync(batch);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            var shard = await GetShardGrainAsync(key);
            await shard.MergeManyAsync(batch);
        }
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

        try
        {
            await ApplyMergeManyCoreAsync(items);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            await ApplyMergeManyCoreAsync(items);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            await ApplyMergeManyCoreAsync(items);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            await ApplyMergeManyCoreAsync(items);
        }
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
    public async Task ApplyPreparedSetAsync(
        string key,
        byte[] value,
        HybridLogicalClock sourceHlc,
        string originClusterId,
        VersionVector? sourceVectorClock,
        long expiresAtTicks,
        Guid transactionId,
        int atomicBatchSize,
        int atomicBatchIndex)
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
            $"origin={originClusterId} batchSize={atomicBatchSize} batchIndex={atomicBatchIndex}");
#endif

        // Re-establish the same ambient-context stack the source-side
        // saga's prepare step produced so the receiver leaf:
        //   - routes this mutation into its _pendingTx[transactionId]
        //     bucket (LatticePreparedContext);
        //   - re-stamps the source's HLC bit-identically
        //     (LatticeHlcOverrideContext);
        //   - persists OriginClusterId, VectorClock, AtomicBatchSize,
        //     AtomicBatchIndex, and TransactionId on the resulting
        //     LatticeMutation verbatim.
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

        if (committed)
        {
            await registry.MarkCommittedAsync(transactionId);
        }
        else
        {
            await registry.MarkAbortedAsync(transactionId);
        }

        // Step 2 (foreground visibility + WAL re-stamp) - drive the
        // per-shard terminal-mark primitive under the source's HLC and
        // origin so the receiver's local WAL append re-stamps the
        // source cluster's terminal HLC and origin verbatim. The shard
        // root's ComputeTerminalHlcAsync honours
        // LatticeHlcOverrideContext.Current and returns the override
        // unchanged - preserving the cross-cluster ordering invariant
        // on receiver replays.
        //
        // Pre-resolve the transitive split-forward closure of every
        // observed source-shard index via TerminalFanOutResolver: under
        // cascading mid-saga splits on the receiver cluster, the
        // inbound records' authoring-cluster shardIndices may have
        // further split locally. The resolver BFS-expands the seeds
        // against each shard's GetSplitForwardTargetsAsync so the
        // terminal mark reaches every destination of every chain, in a
        // single parallel hop - replacing the previous recursive
        // forward that compounded RPC depth into Orleans' response
        // timeout. Seeding with the full ObservedSourceShards set
        // (not just the current arrival's shardIndex) means a saga
        // touching N source shards fans the terminal out across the
        // union of all N transitive closures in a single pass on the
        // final arrival.
        using (LatticeOriginContext.With(originClusterId))
        using (LatticeHlcOverrideContext.With(terminalHlc))
        {
            var (physicalTreeId, _) = await GetRoutingAsync();
            var seeds = tally.ObservedSourceShards;
            var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
                grainFactory,
                physicalTreeId,
                seeds,
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
    private async Task ApplyTerminalToShardAsync(
        string physicalTreeId,
        int shardIndex,
        Guid transactionId,
        bool committed,
        CancellationToken cancellationToken)
    {
        var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
        try
        {
            await shard.AppendTxTerminalAsync(transactionId, committed, committedValues: null, cancellationToken);
        }
        catch (StaleShardRoutingException) when (InvalidateShardMap())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (retryPhysicalTreeId, _) = await GetRoutingAsync();
            var retryShard = grainFactory.GetGrain<IShardRootGrain>($"{retryPhysicalTreeId}/{shardIndex}");
            await retryShard.AppendTxTerminalAsync(transactionId, committed, committedValues: null, cancellationToken);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (retryPhysicalTreeId, _) = await GetRoutingAsync();
            var retryShard = grainFactory.GetGrain<IShardRootGrain>($"{retryPhysicalTreeId}/{shardIndex}");
            await retryShard.AppendTxTerminalAsync(transactionId, committed, committedValues: null, cancellationToken);
        }
    }
}

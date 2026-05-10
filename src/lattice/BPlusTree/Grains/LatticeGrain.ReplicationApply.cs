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
        // outbound ship loop filters the resulting WAL entries back out —
        // preventing the range from looping back to the authoring cluster.
        using var originScope = LatticeOriginContext.With(originClusterId);
        using var vcScope = LatticeVectorClockContext.With(sourceVectorClock);

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
    /// — the only entry point that preserves the source HLC end-to-end —
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
    public Task<AtomicApplyResult> ApplyManyAtomicAsync(
        IReadOnlyList<AtomicApplyEntry> applyEntries,
        Guid transactionId,
        string originClusterId,
        VersionVector? sourceVectorClock,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(applyEntries);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        if (transactionId == Guid.Empty)
        {
            throw new ArgumentException(
                "ApplyManyAtomicAsync requires a non-empty transactionId for idempotent retry.",
                nameof(transactionId));
        }
        cancellationToken.ThrowIfCancellationRequested();

        if (applyEntries.Count == 0)
        {
            return Task.FromResult(new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Committed,
                AppliedCount = 0,
                FailureReason = null,
            });
        }

        // The saga's PrepareAsync captures LatticeVectorClockContext.Current
        // as the saga-wide pre-saga frontier; stamp it here so the saga
        // sees the remote cluster's frontier verbatim. Per-entry
        // VectorClock values override this saga-wide stamp during each
        // per-key dispatch (see ExecuteApplyStepAsync).
        using var vcScope = LatticeVectorClockContext.With(sourceVectorClock);

        // Materialize the apply list once. The interface's IReadOnlyList<T>
        // shape lets callers pass any collection; the saga's persisted state
        // requires a concrete List<T>. The cost is a single allocation per
        // batch — small relative to the per-shard saga overhead.
        var entries = applyEntries as List<AtomicApplyEntry> ?? new List<AtomicApplyEntry>(applyEntries);

        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{transactionId:N}");
        return saga.ExecuteApplyAsync(TreeId, entries, originClusterId);
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

        // Mirror AtomicWriteGrain.ExecuteApplyStepAsync's per-step
        // ambient-context stack so the receiver leaf:
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
                    // Absolute expiry already elapsed — treat as absent
                    // by routing as a tombstone, matching the
                    // public-read semantics for expired entries (see
                    // AtomicWriteGrain.ExecuteApplyStepAsync).
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

        // Step 1 (linearization point) — mark the per-tree TxRegistry
        // with the saga's outcome. Receiver-side readers that find a
        // pending entry on a leaf dial back through the registry to
        // resolve their read against the already-recorded outcome,
        // delivering strict atomic-visibility regardless of how the
        // per-leaf terminal fan-out is interleaved.
        var registry = grainFactory.GetGrain<ITxRegistryGrain>(TreeId);
        if (committed)
        {
            await registry.MarkCommittedAsync(transactionId);
        }
        else
        {
            await registry.MarkAbortedAsync(transactionId);
        }

        // Step 2 (foreground visibility + WAL re-stamp) — drive the
        // per-shard terminal-mark primitive under the source's HLC and
        // origin so the receiver's local WAL append re-stamps the
        // source cluster's terminal HLC and origin verbatim. The shard
        // root's ComputeTerminalHlcAsync honours
        // LatticeHlcOverrideContext.Current and returns the override
        // unchanged — preserving the cross-cluster ordering invariant
        // on receiver replays.
        using (LatticeOriginContext.With(originClusterId))
        using (LatticeHlcOverrideContext.With(terminalHlc))
        {
            var (physicalTreeId, _) = await GetRoutingAsync();
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            try
            {
                await shard.AppendTxTerminalAsync(transactionId, committed, cancellationToken);
            }
            catch (StaleShardRoutingException) when (InvalidateShardMap())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var (retryPhysicalTreeId, _) = await GetRoutingAsync();
                var retryShard = grainFactory.GetGrain<IShardRootGrain>($"{retryPhysicalTreeId}/{shardIndex}");
                await retryShard.AppendTxTerminalAsync(transactionId, committed, cancellationToken);
            }
            catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var (retryPhysicalTreeId, _) = await GetRoutingAsync();
                var retryShard = grainFactory.GetGrain<IShardRootGrain>($"{retryPhysicalTreeId}/{shardIndex}");
                await retryShard.AppendTxTerminalAsync(transactionId, committed, cancellationToken);
            }
        }
    }
}

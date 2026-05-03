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
}

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
        long expiresAtTicks)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        var lww = LwwValue<byte[]>.CreateWithExpiry(value, sourceHlc, expiresAtTicks)
            with
            { OriginClusterId = originClusterId };

        return ApplyMergeOneAsync(key, lww);
    }

    /// <inheritdoc />
    public Task ApplyDeleteAsync(
        string key,
        HybridLogicalClock sourceHlc,
        string originClusterId)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        var tombstone = LwwValue<byte[]>.Tombstone(sourceHlc)
            with
            { OriginClusterId = originClusterId };

        return ApplyMergeOneAsync(key, tombstone);
    }

    /// <inheritdoc />
    public async Task ApplyDeleteRangeAsync(
        string startInclusive,
        string endExclusive,
        string originClusterId)
    {
        ThrowIfSystemTree();
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        if (string.CompareOrdinal(startInclusive, endExclusive) >= 0)
        {
            return;
        }

        // Wrap the range walk in a LatticeOriginContext scope so the
        // shard-root range-delete observer publishes the remote origin
        // and the outbound ship loop filters the resulting WAL entries
        // back out — preventing the range from looping back to the
        // authoring cluster.
        using var scope = LatticeOriginContext.With(originClusterId);

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
}

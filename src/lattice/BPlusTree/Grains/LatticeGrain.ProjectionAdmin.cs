namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Public-surface forwarders for the projection rebuild and
/// materialiser-lag operator tooling. Resolves the per-tree
/// <see cref="ShardMap"/> via the existing
/// <see cref="LatticeGrain.GetRoutingAsync"/> helper, validates the
/// physical shard index, and dispatches to the
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain"/> admin seams. Guarded by
/// <see cref="LatticeGrain.ThrowIfSystemTree"/> so reserved system
/// trees (registry, replication WAL prefix) cannot be rebuilt or
/// inspected through the public surface.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public async Task RebuildLeafProjectionAsync(
        int shardIndex,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);

        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Validate the supplied shard index against the per-tree map;
        // catches off-by-one mistakes early instead of dispatching
        // against a non-existent shard grain that would activate empty.
        var physicalShards = shardMap.GetPhysicalShardIndices();
        if (!physicalShards.Contains(shardIndex))
        {
            throw new ArgumentOutOfRangeException(
                nameof(shardIndex),
                shardIndex,
                $"Shard index {shardIndex} is not a physical shard of tree '{TreeId}'. " +
                $"Valid indices: [{string.Join(", ", physicalShards)}].");
        }

        var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
        await ShardActivationRetry.RunAsync(
            () => shard.RebuildShardProjectionAsync(cancellationToken),
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> CompactShardAsync(
        int shardIndex,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);

        var (_, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();

        var physicalShards = shardMap.GetPhysicalShardIndices();
        if (!physicalShards.Contains(shardIndex))
        {
            throw new ArgumentOutOfRangeException(
                nameof(shardIndex),
                shardIndex,
                $"Shard index {shardIndex} is not a physical shard of tree '{TreeId}'. " +
                $"Valid indices: [{string.Join(", ", physicalShards)}].");
        }

        // Operator-initiated requests bypass the cooldown gate by
        // virtue of carrying the `operator` trigger label; the
        // coordinator grain still rejects the request when compaction
        // is disabled or a pass is already in flight.
        var compactor = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        return await ShardActivationRetry.RunAsync(
            () => compactor.RequestCompactionAsync(shardIndex, TombstoneCompactionGrain.TriggerOperator),
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<long> GetMaterialiserLagAsync(
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // This call previously performed no gate call at all, unlike both of its
        // siblings in this file (RebuildLeafProjectionAsync, CompactShardAsync),
        // which enforce Admin. It fans out across every physical shard, so an
        // ungated call discloses that the tree exists, reveals its shard
        // topology through the fan-out, and forces shard-grain activation on
        // behalf of an unauthorized caller. Read rather than Admin because the
        // verb only observes lag and never mutates, matching the choice #1722
        // made for the observe-only metadata verbs.
        await EnforceWholeTreeAsync(LatticeOperation.Read, cancellationToken);

        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();

        var physicalShards = shardMap.GetPhysicalShardIndices();
        if (physicalShards.Count == 0)
            return 0;

        // Fan out across every physical shard concurrently; the
        // returned scalar is the max-reduced per-shard lag because
        // back-pressure is dominated by the slowest shard, not the
        // mean. Each shard's call is wrapped in its own
        // ShardActivationRetry envelope so a single shard's seed-timeout
        // only retries that shard, not every sibling.
        var tasks = new Task<long>[physicalShards.Count];
        for (var i = 0; i < physicalShards.Count; i++)
        {
            var shardIndex = physicalShards[i];
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            tasks[i] = ShardActivationRetry.RunAsync(
                () => shard.GetShardMaterialiserLagAsync(cancellationToken),
                cancellationToken);
        }

        var perShardLags = await Task.WhenAll(tasks);
        cancellationToken.ThrowIfCancellationRequested();

        long maxLag = 0;
        foreach (var lag in perShardLags)
        {
            if (lag > maxLag)
                maxLag = lag;
        }
        return maxLag;
    }
}

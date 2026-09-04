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

        // Drive the shard's work-bounded batches to completion. Each batch
        // rebuilds a bounded number of leaves and then releases the shard, so
        // an operator rebuild no longer holds it for the length of the whole
        // leaf chain (issue 1972). Each batch keeps its own ShardActivationRetry
        // envelope, so a seed timeout retries just that batch.
        //
        // A batch boundary is observationally the same as the partial state
        // this verb already documents: the rebuild applies leaf by leaf, each
        // leaf's rebuild is independently idempotent, and cancelling already
        // stops the fan-out before the next leaf.
        string? cursor = null;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var currentCursor = cursor;
            var page = await ShardActivationRetry.RunAsync(
                () => shard.RebuildShardProjectionBoundedAsync(currentCursor, cancellationToken),
                cancellationToken);

            if (page.ResumeFromInclusive is not { } next) return;
            cursor = next;
        }
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
            tasks[i] = GetShardLagAsync(shard, cancellationToken);
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

    /// <summary>
    /// Drives one shard's work-bounded materialiser-lag batches to completion.
    /// <para>
    /// Each batch reduces the projection checkpoint across a bounded number of
    /// leaves and then releases the shard, so a lag query no longer holds it
    /// for the length of the whole leaf chain (issue 1972). The WAL heads come
    /// from the first batch only and the per-batch minima are reduced with
    /// <c>min</c>, which is the same reduction the single-call walk performed -
    /// and keeps the heads pinned to one instant, so a tree committing during
    /// the walk cannot inflate the reported lag.
    /// </para>
    /// </summary>
    private static async Task<long> GetShardLagAsync(
        IShardRootGrain shard, CancellationToken cancellationToken)
    {
        var page = await ShardActivationRetry.RunAsync(
            () => shard.GetShardMaterialiserLagBoundedAsync(null, cancellationToken),
            cancellationToken);

        var heads = page.WalHeadOffsets;
        var minCheckpoint = page.MinCheckpointOffset;

        var cursor = page.ResumeFromInclusive;
        while (cursor is not null)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var currentCursor = cursor;
            page = await ShardActivationRetry.RunAsync(
                () => shard.GetShardMaterialiserLagBoundedAsync(currentCursor, cancellationToken),
                cancellationToken);

            if (page.MinCheckpointOffset < minCheckpoint)
                minCheckpoint = page.MinCheckpointOffset;
            cursor = page.ResumeFromInclusive;
        }

        return ShardRootGrain.ReduceMaterialiserLag(heads, minCheckpoint);
    }
}

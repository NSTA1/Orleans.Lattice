namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Public-surface forwarder for
/// <see cref="ILattice.GetLeafProjectionDigestAsync"/>. Resolves the per-tree
/// <see cref="ShardMap"/> via the existing <see cref="LatticeGrain.GetRoutingAsync"/>
/// helper, validates that <c>shardIndex</c> corresponds to a physical shard
/// owned by this tree, and dispatches to <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetShardProjectionDigestAsync"/>.
/// Guarded by <see cref="LatticeGrain.ThrowIfSystemTree"/> so reserved
/// system trees (registry, replication WAL prefix) cannot be inspected
/// through the public surface, and short-circuits with
/// <see cref="InvalidOperationException"/> when the per-tree
/// <see cref="LatticeOptions.MaintainProjectionDigest"/> opt-out is in
/// effect so polls against a digest-quiescent tree fail loudly rather
/// than silently returning stale aggregates.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public async Task<LeafProjectionDigest> GetLeafProjectionDigestAsync(
        int shardIndex,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // Fast-fail at the public surface so a routing-table fetch and a
        // cross-grain hop to the shard root are not paid on every poll
        // against a tree that has explicitly opted out of digest
        // maintenance. The leaf and internal grains both repeat this
        // check with the same message for defence-in-depth - any direct
        // grain-handle caller hits the same exception.
        if (!Options.MaintainProjectionDigest)
        {
            throw new InvalidOperationException(
                $"Projection-digest maintenance is disabled for tree '{TreeId}' " +
                $"({nameof(LatticeOptions)}.{nameof(LatticeOptions.MaintainProjectionDigest)} = false), " +
                "so the persisted aggregates are not the source of truth and the " +
                "digest API is unavailable. Set the option to true to resume maintenance.");
        }

        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Validate the supplied shard index against the per-tree map; this
        // catches off-by-one mistakes early instead of dispatching against
        // a non-existent shard grain that would activate empty.
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
        return await ShardActivationRetry.RunAsync(
            () => shard.GetShardProjectionDigestAsync(cancellationToken),
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<LeafProjectionDigest> GetLeafProjectionDigestForRangeAsync(
        int shardIndex,
        string? startKeyInclusive,
        string? endKeyExclusive,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // Same fast-fail as GetLeafProjectionDigestAsync: a digest-quiescent
        // tree has no maintained aggregates, so a range probe would return
        // stale bytes. Fail loudly at the public surface before paying a
        // routing fetch and a shard-root hop.
        if (!Options.MaintainProjectionDigest)
        {
            throw new InvalidOperationException(
                $"Projection-digest maintenance is disabled for tree '{TreeId}' " +
                $"({nameof(LatticeOptions)}.{nameof(LatticeOptions.MaintainProjectionDigest)} = false), " +
                "so the persisted aggregates are not the source of truth and the " +
                "digest API is unavailable. Set the option to true to resume maintenance.");
        }

        var (physicalTreeId, shardMap) = await GetRoutingAsync();
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

        var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
        return await ShardActivationRetry.RunAsync(
            () => shard.GetShardProjectionDigestForRangeAsync(startKeyInclusive, endKeyExclusive, cancellationToken),
            cancellationToken);
    }
}

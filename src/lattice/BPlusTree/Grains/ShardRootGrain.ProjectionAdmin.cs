namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Operator-tooling partial for <see cref="ShardRootGrain"/>. Fans the
/// leaf-projection rebuild and materialiser-lag queries across this
/// shard's leaf chain, mirroring the existing chain-walk used by
/// <see cref="ShardRootGrain.GetDiagnosticsAsync"/>.
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <inheritdoc />
    public async Task RebuildShardProjectionAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        var leftmostId = await GetLeftmostLeafIdAsync();
        if (leftmostId is null)
        {
            // Empty shard - no leaves to rebuild. Returning here matches
            // the diagnostics-walk semantics for a shard whose root has
            // never been assigned.
            return;
        }

        var leafId = leftmostId.Value;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());

            // Read the next-sibling pointer BEFORE driving the rebuild
            // because RebuildProjectionFromWalAsync deactivates the
            // grain; any subsequent call on the same grain handle would
            // simply reactivate it (wasting an activation) and we
            // already have everything we need from the pre-rebuild
            // state to continue the chain walk.
            var next = await leaf.GetNextSiblingAsync();

            await leaf.RebuildProjectionFromWalAsync();

            if (next is null)
                break;
            leafId = next.Value;
        }
    }

    /// <inheritdoc />
    public async Task<long> GetShardMaterialiserLagAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // WAL head is shared across every leaf in this shard's chain;
        // fetch it once and compare against each leaf's persisted
        // projection-checkpoint offset. The V1 per-shard WAL is keyed
        // by tree id + shard index and presented through the existing
        // ILeafReplayCoordinatorGrain seam.
        var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
            $"{TreeId}/{ShardIndex}");
        var head = await coordinator.GetHeadOffsetAsync(cancellationToken);

        var leftmostId = await GetLeftmostLeafIdAsync();
        if (leftmostId is null)
        {
            // Empty shard - no projection state exists, so the WAL
            // head IS the lag. This degenerates to zero for a brand
            // new tree (head also zero) and is the natural value for
            // a shard whose root has been cleared but whose WAL still
            // carries history.
            return head;
        }

        long minCheckpoint = long.MaxValue;
        var leafId = leftmostId.Value;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());
            var checkpoint = await leaf.GetProjectionCheckpointOffsetAsync();
            if (checkpoint < minCheckpoint)
                minCheckpoint = checkpoint;

            var next = await leaf.GetNextSiblingAsync();
            if (next is null)
                break;
            leafId = next.Value;
        }

        var lag = head - minCheckpoint;
        return lag < 0 ? 0 : lag;
    }

    /// <inheritdoc />
    public async Task<long> SnapshotWalHeadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // The replay coordinator's head offset IS the next-to-be-
        // assigned WAL sequence number on this shard: the canonical
        // "open snapshot at this moment" capture point for the
        // zero-observable-writes cursor design. Snapshot leaves will
        // replay records [0, value) to materialise the shard's
        // projection view of the snapshot.
        var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
            $"{TreeId}/{ShardIndex}");
        return await coordinator.GetHeadOffsetAsync(cancellationToken);
    }
}


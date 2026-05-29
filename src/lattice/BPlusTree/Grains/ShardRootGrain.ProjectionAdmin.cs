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

        // WAL head is shared across every leaf in this shard's chain
        // but - under multi-partition WAL - is partitioned across
        // multiple ILeafReplayCoordinatorGrain activations. We capture
        // the head per partition and compute the per-shard lag as the
        // SUM of per-partition lags: each leaf's projection checkpoint
        // is also per-partition, and the operator-facing "how far
        // behind is this shard" number must reflect total replay work
        // outstanding across every partition (not just the deepest
        // one). Under the default single-partition shape the sum
        // collapses to the legacy scalar lag.
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var partitionCount = Math.Max(1, resolved.WalPartitions);
        var perPartitionHeads = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++)
        {
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{TreeId}/{p}");
            perPartitionHeads[p] = await coordinator.GetHeadOffsetAsync(cancellationToken);
        }

        var leftmostId = await GetLeftmostLeafIdAsync();
        if (leftmostId is null)
        {
            // Empty shard - no projection state exists, so the sum of
            // WAL heads IS the lag.
            long emptyShardLag = 0;
            for (var p = 0; p < partitionCount; p++) emptyShardLag += perPartitionHeads[p];
            return emptyShardLag;
        }

        // For each partition independently, find the chain-walk
        // minimum checkpoint; the partition's lag is head - min. Sum
        // across partitions to surface the shard-total lag.
        var perPartitionMinCheckpoint = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++) perPartitionMinCheckpoint[p] = long.MaxValue;

        var leafId = leftmostId.Value;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());
            // GetProjectionCheckpointOffsetAsync returns the legacy
            // scalar (partition 0) projection checkpoint; under multi-
            // partition the per-partition state lives on
            // ProjectionCheckpointOffsetsByPartition. For per-partition
            // accuracy we'd need a per-partition accessor on the leaf
            // grain - which isn't ship today and would be a separate
            // public-API expansion. As a conservative approximation the
            // legacy scalar (a lower bound on partition 0's actual
            // checkpoint) is treated as the floor across every
            // partition's min, yielding a slightly-overcounted lag
            // figure - which is the right direction for an operator
            // alarm signal (better to over-report lag than miss it).
            var legacyCheckpoint = await leaf.GetProjectionCheckpointOffsetAsync();
            for (var p = 0; p < partitionCount; p++)
            {
                if (legacyCheckpoint < perPartitionMinCheckpoint[p])
                    perPartitionMinCheckpoint[p] = legacyCheckpoint;
            }

            var next = await leaf.GetNextSiblingAsync();
            if (next is null)
                break;
            leafId = next.Value;
        }

        long totalLag = 0;
        for (var p = 0; p < partitionCount; p++)
        {
            var lag = perPartitionHeads[p] - perPartitionMinCheckpoint[p];
            if (lag > 0) totalLag += lag;
        }
        return totalLag;
    }

    /// <inheritdoc />
    public async Task<long[]> SnapshotWalHeadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Per-partition WAL-head capture: each partition's coordinator
        // returns its next-to-be-assigned offset. The snapshot leaf's
        // ReplayWalAsync drives a per-partition slice walk against the
        // same coordinator set, so the returned array is the canonical
        // "open snapshot at this moment" capture point for the
        // multi-partition zero-observable-writes cursor design. Under
        // the default single-partition shape this collapses to a
        // single-element array matching the pre-multi-partition
        // scalar shape exactly.
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var partitionCount = Math.Max(1, resolved.WalPartitions);
        var heads = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{TreeId}/{p}");
            heads[p] = await coordinator.GetHeadOffsetAsync(cancellationToken);
        }
        return heads;
    }
}


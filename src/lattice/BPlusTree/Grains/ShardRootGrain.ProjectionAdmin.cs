using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Operator-tooling partial for <see cref="Orleans.Lattice.BPlusTree.Grains.ShardRootGrain"/>. Fans the
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

    /// <inheritdoc />
    public async Task<SnapshotBaselineCaptureResult> CaptureSnapshotBaselineAsync(Guid token, CancellationToken cancellationToken)
    {
        if (token == Guid.Empty)
            throw new ArgumentException("Snapshot baseline token must not be empty.", nameof(token));
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Pass 1 (freeze): walk this shard's leaf chain and freeze each leaf's
        // committed cache, per-partition projection frontier, and in-flight
        // prepared sagas. Reading the next-sibling pointer before the freeze is
        // unnecessary here (the freeze is read-only and does not deactivate the
        // leaf), but we keep the leaf handle alongside its freeze so the fold
        // pass can target the exact same leaf with the uniform capturedHead.
        var leftmostId = await GetLeftmostLeafIdAsync();
        var frozen = new List<(IBPlusLeafGrain Leaf, LeafBaselineFreeze Freeze)>();
        if (leftmostId is not null)
        {
            var leafId = leftmostId.Value;
            // DELIBERATELY NOT WORK-BOUNDED (issue 1956). Do not apply
            // LeafWalkBudget here. capturedHead is read AFTER every freeze has
            // returned (see below), which is what makes the baseline a single
            // consistent point. Releasing the non-reentrant shard mid-freeze
            // would let writes land between two leaves' freezes, so the
            // baseline would no longer describe one instant and the snapshot
            // cursor's zero-observable-writes guarantee would not hold.
            var walk = new AtomicLeafWalk(nameof(CaptureSnapshotBaselineAsync));
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());
                var freeze = await leaf.FreezeProjectionAsync(cancellationToken);
                frozen.Add((leaf, freeze));
                walk.RecordLeafVisited();

                var next = await leaf.GetNextSiblingAsync();
                if (next is null)
                    break;
                leafId = next.Value;
            }

            walk.ReportIfSlow(logger, context.GrainId);
        }

        // Capture capturedHead AFTER every freeze has returned. Because each
        // freeze happened earlier in wall-clock than this head read,
        // head_now >= head_at_freeze >= frontier_at_freeze for every leaf and
        // partition, so frontier_p <= capturedHead_p uniformly with no
        // overshoot. The uniform head also keeps a cross-leaf saga atomic: a
        // terminal beyond capturedHead leaves its saga pending (invisible) on
        // every leaf the saga touched.
        var capturedHead = await SnapshotWalHeadAsync(cancellationToken);

        // Pass 2 (fold + union): fold each leaf's own (frontier, capturedHead]
        // tail on top of its frozen cache and union the results. Leaves own
        // disjoint key ranges, so a collision here can only be a donor-orphan
        // duplicate left behind by an adaptive split; LWW-merging on collision
        // keeps the highest-timestamp variant (the snapshot leaf's read-time
        // IsKeyOwned filter then drops the orphan for the non-owning shard).
        var union = new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
        var unionModes = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
        foreach (var (leaf, freeze) in frozen)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var rows = await leaf.FoldTailOntoFrozenAsync(freeze, capturedHead, cancellationToken);
            foreach (var row in rows)
            {
                if (union.TryGetValue(row.Key, out var existing))
                {
                    var merged = LwwValue<byte[]>.Merge(existing, row.Value);
                    union[row.Key] = merged;
                    // On a donor-orphan collision the per-key merge mode must
                    // follow whichever value the LWW merge kept. If the incoming
                    // row won (its timestamp is the survivor), adopt its mode;
                    // otherwise leave the existing mode untouched.
                    if (ReferenceEquals(merged.Value, row.Value.Value)
                        || merged.Timestamp.Equals(row.Value.Timestamp))
                    {
                        if (row.MergeMode is { } wonMode)
                            unionModes[row.Key] = wonMode;
                        else
                            unionModes.Remove(row.Key);
                    }
                }
                else
                {
                    union[row.Key] = row.Value;
                    if (row.MergeMode is { } mode)
                        unionModes[row.Key] = mode;
                }
            }
        }

        var materialised = new List<LeafSnapshotRow>(union.Count);
        long rowBytes = 0;
        foreach (var (key, value) in union)
        {
            var mergeMode = unionModes.TryGetValue(key, out var m) ? m : (LatticeMergeMode?)null;
            materialised.Add(new LeafSnapshotRow(key, value, mergeMode));
            rowBytes += LeafEntryCache.EntryBytes(key, value.IsTombstone ? null : value.Value);
        }

        var baseline = new SnapshotShardBaseline
        {
            Rows = materialised,
            CapturedHeadPerPartition = capturedHead,
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            RowBytes = rowBytes,
        };

        // Issue #916: seed the materialised baseline directly into the transient
        // per-shard snapshot leaf the cursor will reach, in memory, instead of
        // writing it to durable storage here. A snapshot that drains in a single
        // page never touches the durable store at all (no capture-time write, no
        // close-time delete). The leaf flushes the baseline to
        // ISnapshotBaselineStorageGrain lazily, only once the owning cursor's
        // first page returns HasMore = true (EnsurePersistedAsync), so any cursor
        // that survives past page 1 still has a durable baseline before the
        // client ever sees a continuation token.
        var snapshotLeaf = grainFactory.GetGrain<ISnapshotLeafGrain>(
            SnapshotLeafGrain.BuildBaselineKey(TreeId, ShardIndex, token));
        await snapshotLeaf.SeedAsync(TreeId, ShardIndex, baseline, token, cancellationToken);

        return new SnapshotBaselineCaptureResult(capturedHead, materialised.Count);
    }
}


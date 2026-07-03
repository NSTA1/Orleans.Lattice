using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Diagnostic aggregation for the shard root grain. Computes depth,
/// live/tombstone counts, and hotness in a single grain call for the
/// diagnostics surface (<see cref="ILattice.DiagnoseAsync"/>).
/// </summary>
internal sealed partial class ShardRootGrain
{
    // Activation-scoped per-leaf byte-footprint cache. Not persisted: a
    // reactivation starts empty and the totals converge as leaves
    // re-publish on their next commit. The operator-driven
    // RefreshLeafByteFootprintsAsync re-anchors them exactly when an
    // authoritative figure is needed. Keeping these off the persistent
    // state row avoids a noisy WriteStateAsync on every leaf commit
    // racing the foreground saga writes through the same etag CAS.
    private readonly Dictionary<Guid, LeafByteFootprint> _leafByteFootprints = new();
    private long _leafStateBytesTotal;
    private long _snapshotBytesTotal;
    private long _liveKeyCountTotal;

    /// <inheritdoc />
    public async Task<ShardDiagnosticReport> GetDiagnosticsAsync(bool deep)
    {
        // Ensure persistent state is loaded before inspection.
        if (state.RecordExists == false)
        {
            // Nothing persisted yet - treat as empty leaf-only shard.
        }

        var rootIsLeaf = state.State.RootIsLeaf;
        var rootNodeId = state.State.RootNodeId;
        var splitInProgress = state.State.SplitInProgress is not null;
        var bulkPending = state.State.PendingBulkGraft is not null;

        var depth = 1;
        long liveKeys = 0;
        long tombstones = 0;

        if (rootNodeId is null)
        {
            // No root yet - empty shard.
            depth = 0;
        }
        else if (RootIsLeafTyped)
        {
            if (deep)
            {
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(rootNodeId.Value.GetGuidKey());
                var stats = await leaf.GetStatsAsync();
                liveKeys = stats.LiveKeys;
                tombstones = stats.Tombstones;
            }
            else
            {
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(rootNodeId.Value.GetGuidKey());
                liveKeys = await leaf.CountAsync();
            }
        }
        else
        {
            // Walk leftmost path to compute depth.
            var currentId = rootNodeId.Value;
            var childrenAreLeaves = false;
            while (!childrenAreLeaves)
            {
                depth++;
                var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId.GetGuidKey());
                var next = await internalGrain.GetLeftmostChildWithMetadataAsync();
                currentId = next.ChildId;
                childrenAreLeaves = next.ChildrenAreLeaves;
            }

            // Walk the leaf chain (via existing sibling pointers) to aggregate counts.
            var leafId = currentId;
            while (true)
            {
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());
                if (deep)
                {
                    var stats = await leaf.GetStatsAsync();
                    liveKeys += stats.LiveKeys;
                    tombstones += stats.Tombstones;
                }
                else
                {
                    liveKeys += await leaf.CountAsync();
                }

                var next = await leaf.GetNextSiblingAsync();
                if (next is null) break;
                leafId = next.Value;
            }
        }

        var hotness = await GetHotnessAsync();
        var opsPerSec = hotness.Window.TotalSeconds > 0
            ? (hotness.Reads + hotness.Writes) / hotness.Window.TotalSeconds
            : 0.0;
        var ratio = (liveKeys + tombstones) > 0
            ? (double)tombstones / (liveKeys + tombstones)
            : 0.0;

        return new ShardDiagnosticReport
        {
            // ShardIndex stamped by caller.
            Depth = depth,
            RootIsLeaf = rootIsLeaf,
            LiveKeys = liveKeys,
            Tombstones = tombstones,
            TombstoneRatio = ratio,
            OpsPerSecond = opsPerSec,
            Reads = hotness.Reads,
            Writes = hotness.Writes,
            HotnessWindow = hotness.Window,
            SplitInProgress = splitInProgress,
            BulkOperationPending = bulkPending,
        };
    }

    /// <inheritdoc />
    public Task<ShardStorageUsage> GetStorageUsageAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // O(1) read off the activation-scoped running totals. Leaves
        // publish their per-leaf footprint via
        // PublishLeafByteFootprintAsync on every commit boundary, so the
        // sum here matches a fresh deep walk by construction once every
        // leaf has published at least once since activation. A
        // freshly-reactivated shard root reports zero until the first
        // leaf publish lands; the operator-driven re-anchor below
        // returns the exact figure when authoritative numbers are needed.
        return Task.FromResult(new ShardStorageUsage
        {
            LeafStateBytes = _leafStateBytesTotal,
            SnapshotBytes = _snapshotBytesTotal,
            LiveKeys = _liveKeyCountTotal,
        });
    }

    /// <inheritdoc />
    public Task PublishLeafByteFootprintAsync(Guid leafKey, LeafByteFootprint footprint)
    {
        // Activation-scoped: no WriteStateAsync, so we cannot race the
        // foreground saga writes through the shard root's etag CAS.
        if (footprint.StateBytes < 0 && footprint.SnapshotBytes < 0)
        {
            if (_leafByteFootprints.Remove(leafKey, out var prev))
            {
                _leafStateBytesTotal -= prev.StateBytes;
                _snapshotBytesTotal -= prev.SnapshotBytes;
                _liveKeyCountTotal -= prev.LiveKeys;
            }
            return Task.CompletedTask;
        }

        if (_leafByteFootprints.TryGetValue(leafKey, out var existing))
        {
            if (existing.StateBytes == footprint.StateBytes
                && existing.SnapshotBytes == footprint.SnapshotBytes
                && existing.LiveKeys == footprint.LiveKeys)
            {
                return Task.CompletedTask;
            }
            _leafStateBytesTotal += footprint.StateBytes - existing.StateBytes;
            _snapshotBytesTotal += footprint.SnapshotBytes - existing.SnapshotBytes;
            _liveKeyCountTotal += footprint.LiveKeys - existing.LiveKeys;
        }
        else
        {
            _leafStateBytesTotal += footprint.StateBytes;
            _snapshotBytesTotal += footprint.SnapshotBytes;
            _liveKeyCountTotal += footprint.LiveKeys;
        }
        _leafByteFootprints[leafKey] = footprint;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<ShardStorageUsage> RefreshLeafByteFootprintsAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var rollup = await DeepWalkLeafFootprintsAsync(cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();

        // Re-anchor the cached totals to the freshly-walked values so any
        // drift accumulated since the last activation self-heals.
        _leafStateBytesTotal = rollup.LeafStateBytes;
        _snapshotBytesTotal = rollup.SnapshotBytes;
        _liveKeyCountTotal = rollup.LiveKeys;
        return rollup;
    }

    private async Task<ShardStorageUsage> DeepWalkLeafFootprintsAsync(CancellationToken cancellationToken)
    {
        var rootNodeId = state.State.RootNodeId;
        if (rootNodeId is null)
        {
            return default;
        }

        if (RootIsLeafTyped)
        {
            return await AccumulateLeafUsageAsync(rootNodeId.Value.GetGuidKey(), cancellationToken);
        }

        var currentId = rootNodeId.Value;
        var childrenAreLeaves = false;
        while (!childrenAreLeaves)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var internalGrain = grainFactory.GetGrain<IBPlusInternalGrain>(currentId.GetGuidKey());
            var next = await internalGrain.GetLeftmostChildWithMetadataAsync();
            currentId = next.ChildId;
            childrenAreLeaves = next.ChildrenAreLeaves;
        }

        long leafStateBytes = 0;
        long snapshotBytes = 0;
        long liveKeys = 0;
        var leafId = currentId;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var usage = await AccumulateLeafUsageAsync(leafId.GetGuidKey(), cancellationToken);
            leafStateBytes += usage.LeafStateBytes;
            snapshotBytes += usage.SnapshotBytes;
            liveKeys += usage.LiveKeys;

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.GetGuidKey());
            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            leafId = next.Value;
        }

        return new ShardStorageUsage
        {
            LeafStateBytes = leafStateBytes,
            SnapshotBytes = snapshotBytes,
            LiveKeys = liveKeys,
        };
    }

    /// <summary>
    /// Reads a single leaf's state-byte footprint and its persisted-snapshot
    /// footprint, returning them as a one-leaf <see cref="ShardStorageUsage"/>.
    /// The leaf-state figure comes from the leaf grain's
    /// <see cref="LeafStats.StateBytes"/>; the snapshot figure comes from the
    /// per-leaf snapshot storage grain keyed by the same <see cref="System.Guid"/>.
    /// </summary>
    private async Task<ShardStorageUsage> AccumulateLeafUsageAsync(Guid leafKey, CancellationToken cancellationToken)
    {
        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafKey);
        var stats = await leaf.GetStatsAsync();

        var snapshot = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(leafKey);
        var snapshotBytes = await snapshot.GetSnapshotByteSizeAsync(cancellationToken);

        return new ShardStorageUsage
        {
            LeafStateBytes = stats.StateBytes,
            SnapshotBytes = snapshotBytes,
            LiveKeys = stats.LiveKeys,
        };
    }
}

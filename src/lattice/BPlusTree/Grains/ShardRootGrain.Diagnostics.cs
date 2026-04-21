using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Diagnostic aggregation for the shard root grain. Computes depth,
/// live/tombstone counts, and hotness in a single grain call for the
/// diagnostics surface (<see cref="ILattice.DiagnoseAsync"/>).
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <inheritdoc />
    public async Task<ShardDiagnosticReport> GetDiagnosticsAsync(bool deep)
    {
        // Ensure persistent state is loaded before inspection.
        if (state.RecordExists == false)
        {
            // Nothing persisted yet — treat as empty leaf-only shard.
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
            // No root yet — empty shard.
            depth = 0;
        }
        else if (rootIsLeaf)
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
}

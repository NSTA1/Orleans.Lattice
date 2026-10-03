using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Single-step drivers for the online topology coordinators, for chaos fixtures that
/// pump a reshard or resize on a background loop while a workload runs. Each step runs
/// one pass of the coordinator and of every per-shard migration it has started, and
/// reports whether the operation has completed.
/// </summary>
internal static class TopologyDrivers
{
    /// <summary>
    /// Returns a step that drives an online reshard of <paramref name="treeId"/>, in
    /// either direction: the coordinator, every split it dispatches (grow) and every
    /// consolidation fold it starts (shrink). Split and fold coordinators are keyed by
    /// the logical tree id and a shard index, so the step remembers every index the
    /// map has routed to - a fold's donor leaves the map before its fold finishes.
    /// </summary>
    public static Func<CancellationToken, Task<bool>> ReshardStep(IGrainFactory grainFactory, string treeId)
    {
        var reshard = grainFactory.GetGrain<ITreeReshardGrain>(treeId);
        var seen = new SortedSet<int>();

        return async ct =>
        {
            if (await reshard.IsIdleAsync()) return true;

            await reshard.RunReshardPassAsync();

            foreach (var index in await PhysicalShardsAsync(grainFactory, treeId)) seen.Add(index);

            foreach (var index in seen)
            {
                if (ct.IsCancellationRequested) break;
                var split = grainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{index}");
                if (!await split.IsIdleAsync()) await split.RunSplitPassAsync();
                var fold = grainFactory.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{index}");
                if (!await fold.IsIdleAsync()) await fold.RunConsolidationPassAsync();
            }

            return false;
        };
    }

    /// <summary>Returns a step that drives an online resize of <paramref name="treeId"/> to completion.</summary>
    public static Func<CancellationToken, Task<bool>> ResizeStep(IGrainFactory grainFactory, string treeId)
    {
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(treeId);
        return async _ =>
        {
            if (await resize.IsIdleAsync()) return true;
            await resize.RunResizePassAsync();
            return false;
        };
    }

    /// <summary>The distinct physical shards <paramref name="treeId"/>'s live map routes to.</summary>
    public static async Task<IReadOnlyList<int>> PhysicalShardsAsync(IGrainFactory grainFactory, string treeId)
    {
        var routing = await grainFactory.GetGrain<ILattice>(treeId).GetRoutingAsync(forceRefresh: true);
        return routing.Map.GetPhysicalShardIndices();
    }

    /// <summary>
    /// Recognises the faults a coordinator pass may raise and simply retry on its next
    /// tick: a shard activation that timed out under load.
    /// </summary>
    public static bool IsRetryableStepFault(Exception ex) => ex is ShardActivationTimeoutException;
}

using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Wal;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Stages the durable state a purge leaves behind when it dies part-way: the
/// shard root has recorded that it began clearing its leaves (issue #4654), and
/// the test then clears the leaves it wants to have been reached.
/// </summary>
internal static class PurgeInterruptionStaging
{
    /// <summary>
    /// Marks <paramref name="shard"/>'s durable row as having begun clearing its
    /// leaves, exactly as <c>ShardRootGrain.ClearTopologyAsync</c> does before its
    /// first leaf clear, with the shard deactivated so its next activation reads it.
    /// </summary>
    public static async Task MarkLeafClearsBegunAsync(IShardRootGrain shard)
    {
        var services = SiloServiceProviderCaptureForWalTests.Captured
            ?? throw new InvalidOperationException("Silo IServiceProvider was not captured by the fixture.");
        await shard.ForceDeactivateAsync();
        await Task.Delay(200);

        var storage = services.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);
        var row = new GrainState<ShardRootState>();
        await storage.ReadStateAsync("shardroot", shard.GetGrainId(), row);
        if (!row.RecordExists)
            throw new InvalidOperationException("precondition: the shard root has a durable row");
        row.State.LeafClearsBegun = true;
        await storage.WriteStateAsync("shardroot", shard.GetGrainId(), row);
    }
}

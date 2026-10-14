using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Wires a substitute shard's <see cref="IShardRootGrain.GetSamplingStatusAsync"/>
/// to compose whatever <see cref="IShardRootGrain.GetHotnessAsync"/>,
/// <see cref="IShardRootGrain.IsSplittingAsync"/> and
/// <see cref="IShardRootGrain.HasPendingBulkOperationAsync"/> are stubbed to
/// return at call time, so a test keeps arranging the individual probes while
/// the sampler under test reads them through the combined call.
/// </summary>
internal static class ShardSamplingStatusStub
{
    /// <summary>Bridges the combined sampling call onto the shard's individual probe stubs.</summary>
    public static void BridgeFromProbes(IShardRootGrain shard)
    {
        shard.GetSamplingStatusAsync().Returns(async _ => new ShardSamplingStatus
        {
            Hotness = await shard.GetHotnessAsync(),
            IsSplitting = await shard.IsSplittingAsync(),
            HasPendingBulkOperation = await shard.HasPendingBulkOperationAsync(),
        });
    }
}

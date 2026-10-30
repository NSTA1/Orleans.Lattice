using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Wires a substitute shard's <see cref="IShardRootGrain.GetSamplingStatusAsync"/>
/// to compose the hotness and pending-bulk values arranged with
/// <see cref="StubHotness"/> and <see cref="StubPendingBulkOperation"/>, plus
/// whatever <see cref="IShardRootGrain.IsSplittingAsync"/> is stubbed to return,
/// read at call time. A test keeps arranging the individual probe values while
/// the sampler under test reads them through the combined call.
/// </summary>
internal static class ShardSamplingStatusStub
{
    private static readonly ConditionalWeakTable<IShardRootGrain, Probes> ProbesByShard = new();

    /// <summary>Bridges the combined sampling call onto the shard's arranged probe values.</summary>
    public static void BridgeFromProbes(IShardRootGrain shard)
    {
        var probes = ProbesByShard.GetOrCreateValue(shard);
        shard.GetSamplingStatusAsync().Returns(async _ => new ShardSamplingStatus
        {
            Hotness = probes.Hotness,
            IsSplitting = await shard.IsSplittingAsync(),
            HasPendingBulkOperation = probes.HasPendingBulkOperation,
        });
    }

    /// <summary>Arranges the hotness counters the bridged sampling call reports for <paramref name="shard"/>.</summary>
    public static void StubHotness(this IShardRootGrain shard, ShardHotness hotness) =>
        ProbesByShard.GetOrCreateValue(shard).Hotness = hotness;

    /// <summary>Arranges the pending-bulk status the bridged sampling call reports for <paramref name="shard"/>.</summary>
    public static void StubPendingBulkOperation(this IShardRootGrain shard, bool pending) =>
        ProbesByShard.GetOrCreateValue(shard).HasPendingBulkOperation = pending;

    private sealed class Probes
    {
        public ShardHotness Hotness;
        public bool HasPendingBulkOperation;
    }
}

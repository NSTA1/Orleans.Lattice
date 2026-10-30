using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.Fakes;

internal static class ShardSubstituteCombinedCalls
{
    internal static IShardRootGrain ComposeMigrationFreeze(this IShardRootGrain shard)
    {
        shard.SealMovedSlotsAndEnterRejectAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(async call =>
        {
            await shard.MarkLeavesMovedAwayAsync(call.Arg<int[]>(), call.Arg<int>());
            await shard.EnterRejectPhaseAsync();
        });
        return shard;
    }
}

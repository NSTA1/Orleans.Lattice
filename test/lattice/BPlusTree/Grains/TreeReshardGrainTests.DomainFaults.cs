using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeReshardGrainTests
{
    [Test]
    public void MigrateAsync_preserves_domain_refusals_from_split_dispatch()
    {
        var (grain, state, factory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 4;
        var split = Substitute.For<ITreeShardSplitGrain>();
        var fault = new LeafProjectionStaleException("projection unavailable");
        split.SplitAsync(Arg.Any<int>()).Returns(Task.FromException(fault));
        factory.GetGrain<ITreeShardSplitGrain>(Arg.Any<string>()).Returns(split);

        var actual = Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.MigrateAsync());

        Assert.That(actual, Is.SameAs(fault));
    }

    [Test]
    public void MigrateAsync_preserves_domain_refusals_from_consolidation_dispatch()
    {
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 2);
        var fault = new LeafProjectionStaleException("projection unavailable");
        h.Folds[1].StartAsync(Arg.Any<int>()).Returns(Task.FromException(fault));

        var actual = Assert.ThrowsAsync<LeafProjectionStaleException>(() => h.Grain.MigrateAsync());

        Assert.That(actual, Is.SameAs(fault));
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.EqualTo(new[] { 1 }),
            "Keep the durable intent until reconciliation establishes the coordinator's actual outcome.");
    }
}

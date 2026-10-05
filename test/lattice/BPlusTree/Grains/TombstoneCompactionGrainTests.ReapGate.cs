using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4615: a compaction pass bounds each leaf's reap by the host's
/// <see cref="ITombstoneReapGate"/>. An ungated tree takes the original call
/// unchanged; a gate failure reaps nothing.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    private static IServiceProvider WithGate(ITombstoneReapGate gate) =>
        new ServiceCollection().AddSingleton(gate).BuildServiceProvider();

    private static IBPlusLeafGrain SingleLeafShard(IGrainFactory grainFactory)
    {
        var leafId = GrainId.Create("leaf", "reap-gate-leaf");
        SetupShardWithLeaves(grainFactory, 0, leafId);
        SetupShardWithLeaves(grainFactory, 1);
        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        leaf.CompactTombstonesBelowAsync(Arg.Any<TimeSpan>(), Arg.Any<HybridLogicalClock>())
            .Returns(Task.FromResult(LeafCompactionResult.Complete(0)));
        return leaf;
    }

    [Test]
    public async Task A_gated_tree_compacts_each_leaf_below_the_gates_ceiling()
    {
        var ceiling = new HybridLogicalClock { WallClockTicks = 42, Counter = 1 };
        var gate = Substitute.For<ITombstoneReapGate>();
        gate.GetReapCeilingAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Task.FromResult<HybridLogicalClock?>(ceiling));
        var (grain, _, _, grainFactory, _) = CreateGrain(activationServices: WithGate(gate));
        var leaf = SingleLeafShard(grainFactory);

        await grain.RunCompactionPassAsync();

        await leaf.Received(1).CompactTombstonesBelowAsync(Arg.Any<TimeSpan>(), ceiling);
        await leaf.DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task An_ungated_tree_compacts_each_leaf_on_the_grace_period_alone()
    {
        var gate = Substitute.For<ITombstoneReapGate>();
        gate.GetReapCeilingAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Task.FromResult<HybridLogicalClock?>(null));
        var (grain, _, _, grainFactory, _) = CreateGrain(activationServices: WithGate(gate));
        var leaf = SingleLeafShard(grainFactory);

        await grain.RunCompactionPassAsync();

        await leaf.Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await leaf.DidNotReceive().CompactTombstonesBelowAsync(Arg.Any<TimeSpan>(), Arg.Any<HybridLogicalClock>());
    }

    [Test]
    public async Task A_gate_that_cannot_answer_reaps_nothing()
    {
        var gate = Substitute.For<ITombstoneReapGate>();
        gate.GetReapCeilingAsync(TreeId, Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException("frontier unreachable"));
        var (grain, _, _, grainFactory, _) = CreateGrain(activationServices: WithGate(gate));
        var leaf = SingleLeafShard(grainFactory);

        Assert.That(async () => await grain.RunCompactionPassAsync(), Throws.InstanceOf<TimeoutException>());
        await leaf.DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await leaf.DidNotReceive().CompactTombstonesBelowAsync(Arg.Any<TimeSpan>(), Arg.Any<HybridLogicalClock>());
    }
}

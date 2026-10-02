using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The shrink half of the reshard atomic-visibility fixture. The grow test above
/// proves a saga stays atomically visible while splits move slots onto new shards;
/// these prove the same of the consolidation folds a shrinking
/// <see cref="ILattice.ReshardAsync"/> drives, where slots move the other way - off a
/// donor that is then retired - and of a shrink that runs on a tree an earlier grow
/// already re-mapped, whose donors include split-allocated shard indices above the
/// tree's pinned shard count.
/// <para>
/// Unlike the grow test's per-round reader, the readers here run continuously across
/// every round, and atomic batches keep being written until the shrink has finished,
/// so the fold's drain, freeze, swap and retirement are each guaranteed to race a
/// saga. Every poll must see the whole universe at one round, never below the last
/// round committed before the poll began.
/// </para>
/// </summary>
public partial class ReshardTopologyTests
{
    private const int ShrinkTarget = 2;
    private static readonly TimeSpan PhaseBudget = TimeSpan.FromSeconds(90);

    [Test]
    public async Task Continuous_reader_observes_zero_or_all_keys_through_mid_saga_shrinking_reshard()
    {
        var treeId = $"reshard-shrink-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var probe = new AtomicRoundProbe(tree, "shrink-tx");
        await probe.SeedAsync();
        probe.StartReaders();

        await tree.ReshardAsync(ShrinkTarget);
        var shrink = await probe.RunPhaseAsync(
            "shrink 4->2", TopologyDrivers.ReshardStep(_cluster.GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault);

        await probe.StopReadersAsync();
        var problems = await probe.VerifyQuiescedAsync("after shrink");
        var live = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);
        var retired = await RetiredShardStatesAsync(treeId, FourShardClusterFixture.TestShardCount, live);
        TestContext.Out.WriteLine($"{shrink}; {probe.Summary()}");

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic visibility violation across a shrinking reshard:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(shrink.Completed, Is.True, "The shrink must complete within its budget.");
            Assert.That(shrink.RoundsDuringChange, Is.GreaterThan(0),
                "At least one atomic batch must have been written while the shrink was in flight.");
            Assert.That(probe.UniformPolls, Is.GreaterThan(0), "The readers must have observed the universe.");
            Assert.That(problems, Is.Empty, string.Join(Environment.NewLine, problems));
            Assert.That(live, Has.Count.EqualTo(ShrinkTarget));
            Assert.That(retired, Is.All.Matches<(int Index, bool Retired)>(s => s.Retired),
                "every shard that left the map must be retired");
        });
    }

    [Test]
    public async Task Continuous_reader_observes_zero_or_all_keys_through_a_grow_then_shrink_reshard()
    {
        var treeId = $"reshard-grow-shrink-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var probe = new AtomicRoundProbe(tree, "grow-shrink-tx");
        await probe.SeedAsync();
        probe.StartReaders();

        await tree.ReshardAsync(ReshardTarget);
        var grow = await probe.RunPhaseAsync(
            "grow 4->8", TopologyDrivers.ReshardStep(_cluster.GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault);
        var afterGrow = await probe.VerifyQuiescedAsync("after grow");
        var grown = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);

        // A shrink of a re-mapped tree: its donors include shard indices the grow's
        // splits allocated above the pinned count of 4.
        const int shrunkTarget = 3;
        await tree.ReshardAsync(shrunkTarget);
        var shrink = await probe.RunPhaseAsync(
            "shrink 8->3", TopologyDrivers.ReshardStep(_cluster.GrainFactory, treeId), PhaseBudget,
            isToleratedStepFault: TopologyDrivers.IsRetryableStepFault);

        await probe.StopReadersAsync();
        var afterShrink = await probe.VerifyQuiescedAsync("after shrink");
        var live = await TopologyDrivers.PhysicalShardsAsync(_cluster.GrainFactory, treeId);
        var retired = await RetiredShardStatesAsync(treeId, grown.Max() + 1, live);
        TestContext.Out.WriteLine($"{grow}; {shrink}; {probe.Summary()}");

        Assert.Multiple(() =>
        {
            Assert.That(probe.Failures, Is.Empty,
                "Atomic visibility violation across a grow then shrink:" + Environment.NewLine
                + string.Join(Environment.NewLine, probe.Failures.Take(30)));
            Assert.That(grow.Completed, Is.True, "The grow must complete within its budget.");
            Assert.That(shrink.Completed, Is.True, "The shrink must complete within its budget.");
            Assert.That(grow.RoundsDuringChange, Is.GreaterThan(0));
            Assert.That(shrink.RoundsDuringChange, Is.GreaterThan(0));
            Assert.That(afterGrow, Is.Empty, string.Join(Environment.NewLine, afterGrow));
            Assert.That(afterShrink, Is.Empty, string.Join(Environment.NewLine, afterShrink));
            Assert.That(grown, Has.Count.EqualTo(ReshardTarget));
            Assert.That(grown.Max(), Is.GreaterThanOrEqualTo(FourShardClusterFixture.TestShardCount),
                "precondition: the grow allocated shard indices above the pinned count");
            Assert.That(live, Has.Count.EqualTo(shrunkTarget));
            Assert.That(retired, Is.All.Matches<(int Index, bool Retired)>(s => s.Retired),
                "every shard that left the map must be retired");
        });
    }

    private async Task<List<(int Index, bool Retired)>> RetiredShardStatesAsync(
        string treeId, int indexCeiling, IReadOnlyList<int> live)
    {
        var states = new List<(int, bool)>();
        foreach (var index in Enumerable.Range(0, indexCeiling).Except(live))
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{index}");
            states.Add((index, await shard.IsRetiredAsync()));
        }

        return states;
    }
}

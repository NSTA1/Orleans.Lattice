using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A split keeps its migration record on its source shard only, yet from its
/// swap onwards its target is in the routing map and still owed the split's
/// final drain. These tests pin that a fold never takes such a target as either
/// side of its pair: folding it away would retire it under the drain, leaving
/// the split unable to finish and the entries it forwards on an unrouted shard.
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    /// <summary>
    /// Three contiguous shards: shard 0 owns slots 0-5, shard 1 slots 6-10 and
    /// shard 2 slots 11-15, so donor 2 folds into its neighbour, survivor 1,
    /// while shard 0 sits outside the pair as the source of some other
    /// migration aimed at <paramref name="otherMigrationTarget"/>.
    /// </summary>
    private static (Harness Harness, IShardRootGrain OtherSource) CreateGrainBesideAnInFlightMigration(
        int? otherMigrationTarget)
    {
        var slots = new int[VirtualShardCount];
        for (var i = 0; i < slots.Length; i++) slots[i] = i <= 5 ? 0 : i <= 10 ? 1 : 2;

        var h = CreateGrain(
            donorShardIndex: 2,
            survivorShardIndex: 1,
            existingMap: new ShardMap { Slots = slots, Version = 3 });

        var otherSource = Substitute.For<IShardRootGrain>();
        otherSource.IsSplittingAsync().Returns(Task.FromResult(otherMigrationTarget is not null));
        otherSource.GetMigrationTargetShardIndexAsync().Returns(Task.FromResult(otherMigrationTarget));
        h.Factory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(otherSource);

        h.Donor.GetMigrationTargetShardIndexAsync().Returns(Task.FromResult<int?>(null));
        h.Survivor.GetMigrationTargetShardIndexAsync().Returns(Task.FromResult<int?>(null));
        return (h, otherSource);
    }

    [Test]
    public async Task StartAsync_refuses_to_fold_away_the_target_of_another_shards_in_flight_split()
    {
        var (h, _) = CreateGrainBesideAnInFlightMigration(otherMigrationTarget: 2);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(1));

        Assert.That(ex!.Message, Does.Contain("in-flight migration from shard 0"));
        Assert.That(h.State.State.InProgress, Is.False, "a refused fold must persist no intent");
        await h.Donor.DidNotReceive().BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public async Task StartAsync_refuses_to_fold_into_the_target_of_another_shards_in_flight_split()
    {
        var (h, _) = CreateGrainBesideAnInFlightMigration(otherMigrationTarget: 1);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.StartAsync(1));

        Assert.That(ex!.Message, Does.Contain("in-flight migration from shard 0"));
        Assert.That(h.State.State.InProgress, Is.False);
        await h.Donor.DidNotReceive().BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public async Task StartAsync_proceeds_while_another_shards_migration_aims_outside_the_pair()
    {
        var (h, _) = CreateGrainBesideAnInFlightMigration(otherMigrationTarget: 3);

        await h.Grain.StartAsync(1);

        Assert.That(h.State.State.InProgress, Is.True);
        await h.Donor.Received(1).BeginSplitAsync(1, Arg.Any<int[]>(), VirtualShardCount);
    }
}

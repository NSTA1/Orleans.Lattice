using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Registry-shard identity and per-shard admission (issue #3501). A shard is
/// keyed <c>_lattice_txshard_{n}_{treeId}</c>; it must report the bare tree id it belongs to,
/// and its admission budget covers only its own row.
/// </summary>
public partial class TxRegistryGrainTests
{
    private static LatticeOptions SmallBudgetOptions() => new()
    {
        TxDecisionRetention = TimeSpan.FromMinutes(1),
        TxRegistryAdmissionBudgetBytes = 16 * 1024,
    };

    [Test]
    public void Shard_keyed_registry_refusal_names_the_bare_tree_id()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(treeId: TxRegistryRouting.ShardKeyAt("tree-x", 3), options: SmallBudgetOptions(), timeProvider: clock);
        SeedTombstones(state.State, 200, clock.GetUtcNow());
        Assume.That(grain.EstimatedRowBytes(), Is.GreaterThanOrEqualTo(16 * 1024));

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(() => grain.EnsureSagaAdmissionAsync());

        Assert.Multiple(() =>
        {
            Assert.That(ex!.SaturationSource, Is.EqualTo(LatticeSaturationSource.TxRegistryCapacity));
            Assert.That(ex.TreeId, Is.EqualTo("tree-x"), "A shard reports the tree it belongs to, not its grain key.");
        });
    }

    [Test]
    public async Task A_full_shard_does_not_refuse_admission_on_a_sibling_shard()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (full, fullState) = CreateGrain(treeId: TxRegistryRouting.ShardKeyAt("tree-x", 0), options: SmallBudgetOptions(), timeProvider: clock);
        var (sibling, siblingState) = CreateGrain(treeId: TxRegistryRouting.ShardKeyAt("tree-x", 1), options: SmallBudgetOptions(), timeProvider: clock);
        SeedTombstones(fullState.State, 200, clock.GetUtcNow());
        Assume.That(full.EstimatedRowBytes(), Is.GreaterThanOrEqualTo(16 * 1024));

        Assert.ThrowsAsync<LatticeSaturatedException>(() => full.EnsureSagaAdmissionAsync());
        await sibling.EnsureSagaAdmissionAsync();

        Assert.That(siblingState.WriteCount, Is.Zero, "The sibling shard's own row is empty, so it admits.");
    }

    [Test]
    public async Task Shard_raises_the_tree_high_water_before_its_first_write_and_only_once()
    {
        var factory = Substitute.For<IGrainFactory>();
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        highWater.RaiseShardHighWaterAsync(Arg.Any<int>()).Returns(ci => Task.FromResult(ci.Arg<int>()));
        factory.GetGrain<ITxRegistryHighWaterGrain>("tree-x").Returns(highWater);
        var raisedBeforeWrite = new List<int>();
        var state = new FakePersistentState<TxRegistryState>();
        state.BeforeWrite = () =>
        {
            raisedBeforeWrite.Add(highWater.ReceivedCalls().Count());
            return Task.CompletedTask;
        };
        var (grain, _) = CreateGrain(state: state, treeId: TxRegistryRouting.ShardKeyAt("tree-x", 5), grainFactory: factory);

        await grain.MarkCommittedAsync(Guid.NewGuid());
        await grain.MarkCommittedAsync(Guid.NewGuid());

        Assert.Multiple(() =>
        {
            Assert.That(raisedBeforeWrite, Is.EqualTo(new[] { 1, 1 }),
                "The mark is raised before the first write and not again for later writes.");
            Assert.That(TxRegistryHighWaterCache.Get(factory, "tree-x"), Is.EqualTo(6),
                "The raised mark is recorded in this silo's cache.");
        });
        await highWater.Received(1).RaiseShardHighWaterAsync(6);
    }

    [Test]
    public async Task Legacy_registry_never_raises_the_high_water()
    {
        var factory = Substitute.For<IGrainFactory>();
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        factory.GetGrain<ITxRegistryHighWaterGrain>(Arg.Any<string>()).Returns(highWater);
        var (grain, _) = CreateGrain(treeId: "tree-x", grainFactory: factory);

        await grain.MarkCommittedAsync(Guid.NewGuid());

        await highWater.DidNotReceiveWithAnyArgs().RaiseShardHighWaterAsync(default);
    }

    [Test]
    public async Task A_failed_high_water_raise_fails_the_write_and_is_retried_on_the_next()
    {
        var factory = Substitute.For<IGrainFactory>();
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        highWater.RaiseShardHighWaterAsync(Arg.Any<int>()).Returns(
            _ => Task.FromException<int>(new InvalidOperationException("high-water down")),
            _ => Task.FromResult(3));
        factory.GetGrain<ITxRegistryHighWaterGrain>("tree-x").Returns(highWater);
        var (grain, state) = CreateGrain(treeId: TxRegistryRouting.ShardKeyAt("tree-x", 2), grainFactory: factory);
        var first = Guid.NewGuid();

        Assert.ThrowsAsync<TxRegistryWriteFailedException>(() => grain.MarkCommittedAsync(first));

        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.Zero, "A shard must not write before its index is durable in the mark.");
            Assert.That(state.State.Decisions.ContainsKey(first), Is.False, "The failed mutation is rolled back.");
        });
        await grain.MarkCommittedAsync(Guid.NewGuid());
        Assert.That(state.WriteCount, Is.EqualTo(1));
        await highWater.Received(2).RaiseShardHighWaterAsync(3);
    }

    [Test]
    public async Task Legacy_registry_of_a_tree_named_like_an_old_shard_key_keeps_its_own_identity()
    {
        // A tree may literally be named "orders~s3" (tree ids are arbitrary
        // strings). Its legacy registry must be that tree's, not shard 3 of
        // "orders": no high-water raise on "orders", and refusals name it whole.
        var factory = Substitute.For<IGrainFactory>();
        var highWater = Substitute.For<ITxRegistryHighWaterGrain>();
        factory.GetGrain<ITxRegistryHighWaterGrain>(Arg.Any<string>()).Returns(highWater);
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(treeId: "orders~s3", options: SmallBudgetOptions(), timeProvider: clock, grainFactory: factory);

        await grain.MarkCommittedAsync(Guid.NewGuid());
        await highWater.DidNotReceiveWithAnyArgs().RaiseShardHighWaterAsync(default);

        SeedTombstones(state.State, 200, clock.GetUtcNow());
        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(() => grain.EnsureSagaAdmissionAsync());
        Assert.That(ex!.TreeId, Is.EqualTo("orders~s3"));
    }

    [Test]
    public async Task Legacy_keyed_registry_still_resolves_a_pre_upgrade_decision()
    {
        // The legacy row keeps answering for a version-4 txid until it drains.
        var (grain, _) = CreateGrain(treeId: "tree-x");
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Committed));
    }
}

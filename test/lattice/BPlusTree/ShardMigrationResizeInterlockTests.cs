using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Tests for <see cref="ShardMigrationResizeInterlock"/>, the two reads the
/// resize and the shard migrations use to exclude each other (issue #4452).
/// </summary>
[TestFixture]
public class ShardMigrationResizeInterlockTests
{
    private static (IGrainFactory Factory, ILatticeRegistry Registry) CreateFactory()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry()));
        return (factory, registry);
    }

    private static ITreeResizeGrain Resize(IGrainFactory factory, string treeId, bool idle)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.IsIdleAsync().Returns(Task.FromResult(idle));
        factory.GetGrain<ITreeResizeGrain>(treeId, Arg.Any<string?>()).Returns(resize);
        return resize;
    }

    [Test]
    public async Task No_resize_is_in_flight_when_the_trees_coordinator_is_idle()
    {
        var (factory, _) = CreateFactory();
        Resize(factory, "t", idle: true);

        Assert.That(await ShardMigrationResizeInterlock.IsResizeInFlightAsync(factory, "t"), Is.False);
    }

    [Test]
    public async Task A_resize_is_in_flight_when_the_trees_coordinator_is_busy()
    {
        var (factory, registry) = CreateFactory();
        Resize(factory, "t", idle: false);

        Assert.That(await ShardMigrationResizeInterlock.IsResizeInFlightAsync(factory, "t"), Is.True);
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    [Test]
    public async Task A_resized_copy_reports_the_resize_of_the_tree_it_was_derived_from()
    {
        // A consolidation driven by the healing orchestrator is keyed by the
        // physical copy id, whose own resize coordinator is never used.
        var (factory, registry) = CreateFactory();
        Resize(factory, "t/resized/op", idle: true);
        Resize(factory, "t", idle: false);
        registry.GetEntryAsync("t/resized/op").Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { DerivedFrom = "t" }));

        Assert.That(await ShardMigrationResizeInterlock.IsResizeInFlightAsync(factory, "t/resized/op"), Is.True);
    }

    [Test]
    public async Task A_resized_copy_whose_owner_is_idle_has_no_resize_in_flight()
    {
        var (factory, registry) = CreateFactory();
        Resize(factory, "t/resized/op", idle: true);
        Resize(factory, "t", idle: true);
        registry.GetEntryAsync("t/resized/op").Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { DerivedFrom = "t" }));

        Assert.That(await ShardMigrationResizeInterlock.IsResizeInFlightAsync(factory, "t/resized/op"), Is.False);
    }

    [Test]
    public async Task FindMigratingShard_names_the_first_shard_carrying_a_migration_record()
    {
        var (factory, _) = CreateFactory();
        foreach (var (index, splitting) in new[] { (0, false), (3, true), (5, true) })
        {
            var shard = Substitute.For<IShardRootGrain>();
            shard.IsSplittingAsync().Returns(Task.FromResult(splitting));
            factory.GetGrain<IShardRootGrain>($"p/{index}", Arg.Any<string?>()).Returns(shard);
        }

        Assert.That(await ShardMigrationResizeInterlock.FindMigratingShardAsync(factory, "p", [0, 3, 5]), Is.EqualTo(3));
        Assert.That(await ShardMigrationResizeInterlock.FindMigratingShardAsync(factory, "p", [0]), Is.Null);
    }
}

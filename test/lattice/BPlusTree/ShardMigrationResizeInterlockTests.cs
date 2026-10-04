using NSubstitute;
using NSubstitute.ExceptionExtensions;
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

    private static ITreeResizeGrain Resize(IGrainFactory factory, string treeId, bool holds)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.HoldsShardMigrationsAsync().Returns(Task.FromResult(holds));
        factory.GetGrain<ITreeResizeGrain>(treeId, Arg.Any<string?>()).Returns(resize);
        return resize;
    }

    [Test]
    public async Task No_hold_when_the_trees_coordinator_holds_none()
    {
        var (factory, _) = CreateFactory();
        Resize(factory, "t", holds: false);

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardMigrationsAsync(factory, "t"), Is.False);
    }

    [Test]
    public async Task A_hold_when_the_trees_coordinator_holds_migrations()
    {
        var (factory, registry) = CreateFactory();
        Resize(factory, "t", holds: true);

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardMigrationsAsync(factory, "t"), Is.True);
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    [Test]
    public async Task A_resized_copy_reports_the_hold_of_the_tree_it_was_derived_from()
    {
        // A consolidation driven by the healing orchestrator is keyed by the
        // physical copy id, whose own resize coordinator is never used.
        var (factory, registry) = CreateFactory();
        Resize(factory, "t/resized/op", holds: false);
        Resize(factory, "t", holds: true);
        registry.GetEntryAsync("t/resized/op").Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { DerivedFrom = "t" }));

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardMigrationsAsync(factory, "t/resized/op"), Is.True);
    }

    [Test]
    public async Task A_resized_copy_whose_owner_holds_nothing_has_no_hold()
    {
        var (factory, registry) = CreateFactory();
        Resize(factory, "t/resized/op", holds: false);
        Resize(factory, "t", holds: false);
        registry.GetEntryAsync("t/resized/op").Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { DerivedFrom = "t" }));

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardMigrationsAsync(factory, "t/resized/op"), Is.False);
    }

    [Test]
    public async Task A_coordinator_that_cannot_answer_holds_migrations()
    {
        // Fail closed: a coordinator on a silo that predates the method, or one
        // that is unreachable, must refuse the migration rather than admit it.
        var (factory, _) = CreateFactory();
        var resize = Resize(factory, "t", holds: false);
        resize.HoldsShardMigrationsAsync().ThrowsAsync(new NotImplementedException("older silo"));

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardMigrationsAsync(factory, "t"), Is.True);
    }

    [Test]
    public async Task A_registry_that_cannot_answer_holds_migrations()
    {
        var (factory, registry) = CreateFactory();
        Resize(factory, "t", holds: false);
        registry.GetEntryAsync("t").ThrowsAsync(new TimeoutException());

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardMigrationsAsync(factory, "t"), Is.True);
    }

    [Test]
    public void ReadResizeHold_propagates_a_fault_rather_than_reporting_a_hold()
    {
        // The reshard coordinator's faulted tick runs its own recovery (it
        // abandons a reshard on a purged tree); a fault read as a hold would
        // pause it forever instead.
        var (factory, _) = CreateFactory();
        var resize = Resize(factory, "t", holds: false);
        resize.HoldsShardMigrationsAsync().ThrowsAsync(new InvalidOperationException("purged"));

        Assert.ThrowsAsync<InvalidOperationException>(() => ShardMigrationResizeInterlock.ReadResizeHoldAsync(factory, "t"));
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

    private static ITreeResizeGrain SplitHold(IGrainFactory factory, string treeId, bool holdsSplits)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.HoldsShardMigrationsAsync().Returns(Task.FromResult(true));
        resize.HoldsShardSplitsAsync().Returns(Task.FromResult(holdsSplits));
        factory.GetGrain<ITreeResizeGrain>(treeId, Arg.Any<string?>()).Returns(resize);
        return resize;
    }

    [Test]
    public async Task A_split_is_not_held_by_a_completed_resize_that_still_holds_other_migrations()
    {
        // Issue #4478: the mirror follows a split of the resized copy, so only
        // consolidations and reshards wait for the replaced copy's purge.
        var (factory, _) = CreateFactory();
        SplitHold(factory, "t", holdsSplits: false);

        Assert.Multiple(async () =>
        {
            Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync(factory, "t"), Is.False);
            Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardMigrationsAsync(factory, "t"), Is.True);
        });
    }

    [Test]
    public async Task A_split_is_held_while_the_trees_coordinator_holds_splits()
    {
        var (factory, registry) = CreateFactory();
        SplitHold(factory, "t", holdsSplits: true);

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync(factory, "t"), Is.True);
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
    }

    [Test]
    public async Task A_split_of_a_resized_copy_is_held_by_the_tree_it_was_derived_from()
    {
        var (factory, registry) = CreateFactory();
        SplitHold(factory, "t/resized/op", holdsSplits: false);
        SplitHold(factory, "t", holdsSplits: true);
        registry.GetEntryAsync("t/resized/op").Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { DerivedFrom = "t" }));

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync(factory, "t/resized/op"), Is.True);
    }

    [Test]
    public async Task A_coordinator_that_cannot_answer_holds_splits()
    {
        // Fail closed, including on a silo that predates the method.
        var (factory, _) = CreateFactory();
        SplitHold(factory, "t", holdsSplits: false).HoldsShardSplitsAsync()
            .ThrowsAsync(new NotImplementedException("older silo"));

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync(factory, "t"), Is.True);
    }

    [Test]
    public async Task A_registry_that_cannot_answer_holds_splits()
    {
        var (factory, registry) = CreateFactory();
        SplitHold(factory, "t", holdsSplits: false);
        registry.GetEntryAsync("t").ThrowsAsync(new TimeoutException());

        Assert.That(await ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync(factory, "t"), Is.True);
    }
}

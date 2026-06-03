using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the byte-accurate storage-usage surface on
/// <see cref="ShardRootGrain"/>: <c>GetStorageUsageAsync</c>,
/// <c>PublishLeafByteFootprintAsync</c>, and
/// <c>RefreshLeafByteFootprintsAsync</c>. Pins the headline regression
/// that <c>GetStorageUsageAsync</c> is O(1) and never activates a leaf
/// or internal grain on the read path.
/// </summary>
[TestFixture]
public sealed class ShardRootGrainStorageUsageTests
{
    private static ShardRootGrain CreateGrain(IGrainFactory factory, FakePersistentState<ShardRootState> state)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", "tree-a/0"));
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        return new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
    }

    [Test]
    public async Task GetStorageUsageAsync_returns_zero_on_a_fresh_activation()
    {
        var factory = Substitute.For<IGrainFactory>();
        var grain = CreateGrain(factory, new FakePersistentState<ShardRootState>());

        var usage = await grain.GetStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(0L));
            Assert.That(usage.SnapshotBytes, Is.EqualTo(0L));
        });
        // Headline regression: the read path must not have activated any
        // leaf, internal node, or snapshot storage grain.
        factory.DidNotReceiveWithAnyArgs().GetGrain<IBPlusLeafGrain>(default(Guid));
        factory.DidNotReceiveWithAnyArgs().GetGrain<IBPlusInternalGrain>(default(Guid));
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILeafSnapshotStorageGrain>(default(Guid));
    }

    [Test]
    public async Task PublishLeafByteFootprintAsync_then_GetStorageUsageAsync_returns_running_totals()
    {
        var factory = Substitute.For<IGrainFactory>();
        var state = new FakePersistentState<ShardRootState>();
        var grain = CreateGrain(factory, state);

        var leafA = Guid.NewGuid();
        var leafB = Guid.NewGuid();
        await grain.PublishLeafByteFootprintAsync(leafA, new LeafByteFootprint { StateBytes = 100, SnapshotBytes = 40 });
        await grain.PublishLeafByteFootprintAsync(leafB, new LeafByteFootprint { StateBytes = 250, SnapshotBytes = 0 });

        var usage = await grain.GetStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(350L));
            Assert.That(usage.SnapshotBytes, Is.EqualTo(40L));
        });

        // Activation-scoped accounting must never write the per-leaf map
        // or running totals into persisted state: doing so would race
        // every foreground saga write through the etag CAS.
        Assert.That(state.WriteCount, Is.EqualTo(0),
            "publishing a per-leaf footprint must not trigger a shard-root WriteStateAsync");
    }

    [Test]
    public async Task PublishLeafByteFootprintAsync_replaces_previous_contribution_for_the_same_leaf()
    {
        var factory = Substitute.For<IGrainFactory>();
        var grain = CreateGrain(factory, new FakePersistentState<ShardRootState>());
        var leafA = Guid.NewGuid();

        await grain.PublishLeafByteFootprintAsync(leafA, new LeafByteFootprint { StateBytes = 100, SnapshotBytes = 40 });
        await grain.PublishLeafByteFootprintAsync(leafA, new LeafByteFootprint { StateBytes = 60, SnapshotBytes = 0 });

        var usage = await grain.GetStorageUsageAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(60L));
            Assert.That(usage.SnapshotBytes, Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task PublishLeafByteFootprintAsync_Removed_drops_the_leaf_contribution()
    {
        var factory = Substitute.For<IGrainFactory>();
        var grain = CreateGrain(factory, new FakePersistentState<ShardRootState>());
        var leafA = Guid.NewGuid();
        var leafB = Guid.NewGuid();

        await grain.PublishLeafByteFootprintAsync(leafA, new LeafByteFootprint { StateBytes = 100, SnapshotBytes = 0 });
        await grain.PublishLeafByteFootprintAsync(leafB, new LeafByteFootprint { StateBytes = 30, SnapshotBytes = 5 });
        await grain.PublishLeafByteFootprintAsync(leafA, LeafByteFootprint.Removed);

        var usage = await grain.GetStorageUsageAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(usage.LeafStateBytes, Is.EqualTo(30L));
            Assert.That(usage.SnapshotBytes, Is.EqualTo(5L));
        });
    }

    [Test]
    public async Task PublishLeafByteFootprintAsync_identical_republish_is_a_noop()
    {
        var factory = Substitute.For<IGrainFactory>();
        var grain = CreateGrain(factory, new FakePersistentState<ShardRootState>());
        var leafA = Guid.NewGuid();
        var footprint = new LeafByteFootprint { StateBytes = 100, SnapshotBytes = 40 };

        await grain.PublishLeafByteFootprintAsync(leafA, footprint);
        await grain.PublishLeafByteFootprintAsync(leafA, footprint);
        await grain.PublishLeafByteFootprintAsync(leafA, footprint);

        var usage = await grain.GetStorageUsageAsync(CancellationToken.None);
        // Identical re-publishes must not double-count.
        Assert.That(usage.LeafStateBytes, Is.EqualTo(100L));
        Assert.That(usage.SnapshotBytes, Is.EqualTo(40L));
    }
}

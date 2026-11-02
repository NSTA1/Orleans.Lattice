using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for #5705 (epic #5605): a shard root of an incarnation a purge has retired
/// never re-stamps a write or re-creates its marker. A write admitted under a generation
/// at or below the tree id's routing floor surfaces the deleted-tree outcome from its
/// seam retry, rather than being re-admitted under the re-created incarnation's
/// generation; and a marker row this activation saw, found absent under a floor that
/// covers it, is not re-created, while a row merely lost (#5663, no floor) still is.
/// </summary>
[TestFixture]
public sealed class ShardRootPurgedIncarnationTests
{
    private const string Tree = "purged-incarnation";
    private static readonly WalRoutingInitiator ShardZero = WalRoutingInitiator.ShardRoot(0);

    private static ShardRootGrain CreateGrain(IGrainStorage storage)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(LatticeOptions.StorageProviderName, storage);
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{Tree}/0"));
        context.ActivationServices.Returns(services.BuildServiceProvider());
        var factory = Substitute.For<IGrainFactory>();
        return new ShardRootGrain(
            context,
            new FakePersistentState<ShardRootState>(),
            factory,
            TestOptionsResolver.Create(baseOptions: new LatticeOptions(), shardCount: 1, factory: factory),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
    }

    private static Task SeedMarkerAsync(IGrainStorage storage, long generation) =>
        HandOffMarkerRows.UpdateShardAsync(storage, Tree, 0, m =>
        {
            m.RoutingGeneration = generation;
            return true;
        });

    /// <summary>The purge's retirement of shard 0's marker: the floor over it first, then the delete.</summary>
    private static Task<long> PurgeMarkerAsync(IGrainStorage storage) =>
        TreeRoutingFenceRetirement.RetireMarkersAsync(storage, Tree, shardCount: 1);

    [Test]
    public async Task A_write_admitted_under_a_purged_generation_is_not_re_admitted_under_the_re_created_one()
    {
        // The reload path: the re-created incarnation's marker already holds floor + 1.
        var storage = new EnumerableMemoryGrainStorage();
        await SeedMarkerAsync(storage, 2);
        var grain = CreateGrain(storage);
        await grain.LoadHandOffMarkerForTestingAsync();
        var floor = await PurgeMarkerAsync(storage);
        await SeedMarkerAsync(storage, floor + 1);

        var retry = grain.RetryStaleRoutingForTestingAsync(new WalRoutingGenerationStaleException(ShardZero, 2, floor + 1));

        Assert.That(async () => await retry, Throws.InvalidOperationException.With.Message.Contains("deleted"),
            "the straddling write surfaces the deleted-tree outcome");
        var marker = (await HandOffMarkerRows.ReadShardAsync(storage, Tree, 0)).State;
        Assert.That(marker.RoutingGeneration, Is.EqualTo(floor + 1), "the re-created incarnation's marker is untouched");
    }

    [Test]
    public async Task A_write_refused_at_the_floor_never_re_creates_the_purged_marker_at_floor_plus_one()
    {
        // The refusal-driven raise (#5663) after the purge, with no re-creation yet.
        var storage = new EnumerableMemoryGrainStorage();
        await SeedMarkerAsync(storage, 2);
        var grain = CreateGrain(storage);
        var floor = await PurgeMarkerAsync(storage);

        var retry = grain.RetryStaleRoutingForTestingAsync(new WalRoutingGenerationStaleException(ShardZero, 2, floor));

        Assert.That(async () => await retry, Throws.InvalidOperationException);
        Assert.That(storage.GrainIds(HandOffMarkerRows.ShardGrainType), Is.Empty,
            "no marker row is written for the purged incarnation, so the re-created one starts above every generation");
    }

    [Test]
    public async Task A_purged_incarnations_marker_write_never_re_creates_the_row()
    {
        // A close or open of the purged incarnation, still in flight across the purge.
        var storage = new EnumerableMemoryGrainStorage();
        await SeedMarkerAsync(storage, 3);
        var grain = CreateGrain(storage);
        await grain.LoadHandOffMarkerForTestingAsync();
        await PurgeMarkerAsync(storage);

        Assert.That(async () => await grain.OpenShardMarkerAsync("split:late", "a", "m"), Throws.InvalidOperationException);
        Assert.That(storage.GrainIds(HandOffMarkerRows.ShardGrainType), Is.Empty);
    }

    [Test]
    public async Task A_lost_marker_row_with_no_purge_is_still_re_created()
    {
        // #5663: a row lost or reset, not retired; the floor is 0, so the update re-creates it.
        var inner = new EnumerableMemoryGrainStorage();
        await SeedMarkerAsync(inner, 3);
        var grain = CreateGrain(inner);
        await grain.LoadHandOffMarkerForTestingAsync();
        var row = await HandOffMarkerRows.ReadShardAsync(inner, Tree, 0);
        await inner.ClearStateAsync(
            HandOffMarkerRows.ShardGrainType,
            GrainId.Create(GrainType.Create(HandOffMarkerRows.ShardGrainType), IdSpan.Create(HandOffMarkerRows.ShardKey(Tree, 0))),
            row);

        await grain.OpenShardMarkerAsync("split:after-loss", "a", "m");

        var marker = (await HandOffMarkerRows.ReadShardAsync(inner, Tree, 0)).State;
        Assert.That(marker.Open.Keys, Does.Contain("split:after-loss"));
    }
}

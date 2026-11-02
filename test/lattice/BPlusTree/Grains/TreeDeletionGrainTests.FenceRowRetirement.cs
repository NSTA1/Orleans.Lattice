using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the purge's per-tree fence-row retirement (epic #5605, #5668): it
/// runs before the registry removal that frees the id, so a re-created tree never
/// meets the rows; a failed retirement keeps that removal owed and arms the keepalive,
/// so the purge's own re-drive retires the rows rather than leaking them; and a soft
/// delete followed by a recover retires nothing.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private static (TreeDeletionGrain Grain, FakePersistentState<TreeDeletionState> State, IGrainFactory Factory, ITreeRangeDeleteFenceGrain Fence, IReminderRegistry Reminders)
        CreateFencedGrain(EnumerableMemoryGrainStorage storage)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IGrainStorage>(LatticeOptions.StorageProviderName, storage);
        var context = Substitute.For<IGrainContext>();
        context.ActivationServices.Returns(services.BuildServiceProvider());
        var (grain, state, reminders, factory, _) = CreateGrain(grainContext: context);
        var fence = Substitute.For<ITreeRangeDeleteFenceGrain>();
        factory.GetGrain<ITreeRangeDeleteFenceGrain>(TreeId).Returns(fence);
        RegistryOf(factory).ExistsAsync(TreeId).Returns(true);
        return (grain, state, factory, fence, reminders);
    }

    private static Task SeedMarkerAsync(IGrainStorage storage, int shard, long generation) =>
        HandOffMarkerRows.UpdateShardAsync(storage, TreeId, shard, m =>
        {
            m.RoutingGeneration = generation;
            return true;
        });

    [Test]
    public async Task A_purge_retires_the_fence_rows_before_it_removes_the_registry_entry()
    {
        var storage = new EnumerableMemoryGrainStorage();
        await SeedMarkerAsync(storage, 0, 3);
        await SeedMarkerAsync(storage, 1, 5);
        var (grain, _, factory, fence, _) = CreateFencedGrain(storage);
        var markersAtRemoval = -1;
        var floorAtRemoval = -1L;
        RegistryOf(factory).UnregisterAsync(TreeId).Returns(async _ =>
        {
            markersAtRemoval = storage.GrainIds(HandOffMarkerRows.ShardGrainType).Count;
            floorAtRemoval = await WalAppendFenceRows.ReadRoutingFloorAsync(storage, TreeId, WalRoutingInitiator.ShardRoot(0));
        });

        await grain.DeleteTreeAsync();
        await grain.PurgeNowAsync();

        await fence.Received(1).RetireForPurgeAsync();
        await factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Received(1).RetireRoutingGenerationsAsync(ShardCount, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(markersAtRemoval, Is.Zero, "every shard marker is gone before the id is freed");
            Assert.That(floorAtRemoval, Is.EqualTo(5), "and the floor covering them is durable");
        });
    }

    [Test]
    public async Task A_failed_retirement_keeps_the_registry_removal_owed_and_the_re_drive_retires_the_rows()
    {
        var storage = new EnumerableMemoryGrainStorage();
        await SeedMarkerAsync(storage, 0, 4);
        var (grain, state, factory, fence, reminders) = CreateFencedGrain(storage);
        var registry = RegistryOf(factory);
        fence.RetireForPurgeAsync().ThrowsAsync(new InvalidOperationException("simulated fence failure"));

        await grain.DeleteTreeAsync();
        Assert.ThrowsAsync<InvalidOperationException>(() => grain.PurgeNowAsync());

        var markersAfterFailure = storage.GrainIds(HandOffMarkerRows.ShardGrainType).Count;
        await registry.DidNotReceive().UnregisterAsync(TreeId);
        Assert.Multiple(() =>
        {
            Assert.That(state.State.PurgeComplete, Is.True, "the walk's completion is durable");
            Assert.That(state.State.RegistryUnregisterPending, Is.True, "the removal that frees the id stays owed");
            Assert.That(markersAfterFailure, Is.EqualTo(1), "nothing after the failed step ran");
        });
        await reminders.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "deletion-keepalive", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());

        fence.RetireForPurgeAsync().Returns(Task.CompletedTask);
        await grain.PurgeNowAsync();

        await registry.Received(1).UnregisterAsync(TreeId);
        var floor = await WalAppendFenceRows.ReadRoutingFloorAsync(storage, TreeId, WalRoutingInitiator.ShardRoot(0));
        Assert.Multiple(() =>
        {
            Assert.That(storage.GrainIds(HandOffMarkerRows.ShardGrainType), Is.Empty, "the re-drive retired the rows");
            Assert.That(floor, Is.EqualTo(4));
            Assert.That(state.State.RegistryUnregisterPending, Is.False);
        });
    }

    [Test]
    public async Task A_soft_delete_followed_by_a_recover_retires_no_fence_row()
    {
        var storage = new EnumerableMemoryGrainStorage();
        await SeedMarkerAsync(storage, 0, 2);
        var (grain, _, factory, fence, _) = CreateFencedGrain(storage);

        await grain.DeleteTreeAsync();
        await grain.RecoverAsync();

        await fence.DidNotReceive().RetireForPurgeAsync();
        await factory.GetGrain<IWalShardGrain>($"{TreeId}/0").DidNotReceive()
            .RetireRoutingGenerationsAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
        var marker = (await HandOffMarkerRows.ReadShardAsync(storage, TreeId, 0)).State;
        var floor = await WalAppendFenceRows.ReadRoutingFloorAsync(storage, TreeId, WalRoutingInitiator.ShardRoot(0));
        Assert.Multiple(() =>
        {
            Assert.That(marker.RoutingGeneration, Is.EqualTo(2), "the recovered tree keeps its generation");
            Assert.That(floor, Is.Zero, "and no floor is written for it");
        });
    }
}

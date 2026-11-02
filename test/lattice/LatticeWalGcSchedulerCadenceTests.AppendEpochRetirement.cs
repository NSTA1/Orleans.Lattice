using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Detectors for the WAL GC's half of the append fence's retirement wiring (epic
/// #5605, review S4 and S5): once an orphaned leaf's materialiser pin is retired,
/// the GC retires the leaf's append-epoch entry at that partition, flooring it at
/// the leaf's epoch row (#5641) - and costs nothing beyond one storage read for a
/// leaf that has no epoch row, which never claimed or whose clear already retired.
/// </summary>
public partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    public async Task Retiring_an_orphan_pin_retires_the_leaf_append_epoch_entry_at_its_epoch_row()
    {
        const string TreeId = "orphan-fence-retire";
        var shard = Substitute.For<IWalShardGrain>();
        var leaf = BlockedLeafGrainId();
        var storage = new LeafStateBook();
        storage.PutHusk(leaf);
        storage.PutAppendEpoch(leaf, 3);

        var pins = await RetireOrphanPinAsync(TreeId, storage, shard);

        Assert.That(pins.RemainingPins, Is.Zero, "precondition: the pin itself was retired");
        await shard.Received(1).RetireAppendEpochAsync(leaf.GetGuidKey(), 3, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Retiring_an_orphan_pin_of_a_leaf_with_no_epoch_row_makes_no_fence_call()
    {
        const string TreeId = "orphan-fence-skip";
        var shard = Substitute.For<IWalShardGrain>();
        var leaf = BlockedLeafGrainId();
        var storage = new LeafStateBook();
        storage.PutHusk(leaf);

        var pins = await RetireOrphanPinAsync(TreeId, storage, shard);

        Assert.That(pins.RemainingPins, Is.Zero, "precondition: the pin itself was retired");
        await shard.DidNotReceiveWithAnyArgs().RetireAppendEpochAsync(default, default, default);
    }

    private static async Task<FakePinStore> RetireOrphanPinAsync(string treeId, LeafStateBook storage, IWalShardGrain shard)
    {
        var time = new VirtualTimeProvider();
        var consumerId = BlockedConsumerId(treeId) + "_0";
        var pins = DrivePinStore(treeId, consumerId);
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: treeId,
            cursorReporter: Substitute.For<ILeafCursorReporter>(),
            pins: pins,
            leafStateStorage: storage,
            orphanSweepCoordinator: InertSweepCoordinator(),
            configureFactory: factory => factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard));

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);
        }

        return pins;
    }
}

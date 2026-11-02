using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Detector for the WAL GC's half of the append fence's retirement wiring (epic
/// #5605, review S4 and S5): once an orphaned leaf's materialiser pin is retired,
/// the GC retires the leaf's append-epoch entry at that partition, asking the
/// partition to read the leaf's epoch row (#5641) rather than reading it itself,
/// so the sweep's own storage reads and their admission are unchanged.
/// </summary>
public partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    public async Task Retiring_an_orphan_pin_retires_the_leaf_append_epoch_entry_at_its_partition()
    {
        const string TreeId = "orphan-fence-retire";
        var time = new VirtualTimeProvider();
        var shard = Substitute.For<IWalShardGrain>();
        var leaf = BlockedLeafGrainId();
        var storage = new LeafStateBook();
        storage.PutHusk(leaf);
        var consumerId = BlockedConsumerId(TreeId) + "_0";
        var pins = DrivePinStore(TreeId, consumerId);
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: TreeId,
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

        Assert.That(pins.RemainingPins, Is.Zero, "precondition: the pin itself was retired");
        await shard.Received(1).RetireAppendEpochAsync(
            leaf.GetGuidKey(), WalAppendFenceRows.RowEpochUnread, Arg.Any<CancellationToken>());
    }
}

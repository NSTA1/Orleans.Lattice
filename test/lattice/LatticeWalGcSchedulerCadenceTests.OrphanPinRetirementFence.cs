using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression fixture for issue #5358, found by the <c>WalPinRetirement</c>
/// formal model (epic #5348): a WAL GC retirement read a purge-cleared leaf as a
/// husk, the leaf's grain id was populated again and seeded its block pin under
/// the same consumer id, and the retirement then removed that pin by consumer
/// id - so the trim could pass the re-created leaf's un-checkpointed writes.
/// <para>
/// Each fixture re-creates the leaf from the leaf-state provider's read hook,
/// which runs after the retirement's read has been served and before its
/// removal: the leaf's row becomes live, and its birth seed stamps a merge in
/// the pin store without changing the pin a read would see. Each has a control
/// that leaves the leaf a husk and must retire, so the refusal is the fence's.
/// </para>
/// </summary>
public partial class LatticeWalGcSchedulerCadenceTests
{
    [Test]
    public async Task The_sweep_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into()
    {
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        var leaf = OrphanLeafGrainId(0);
        var consumerId = OrphanConsumerId(0);
        storage.PutHusk(leaf);
        pins.Seed(OrphanSweepTree, consumerId);
        RecreateOnFirstReadAfterAToken(storage, pins, leaf, OrphanSweepTree, OrphanSweepTree, consumerId);

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pins.FencedRemovals, Does.Contain((OrphanSweepTree, consumerId)),
                "anti-vacuity: the sweep must have decided to retire on its husk read and reached the fence.");
            Assert.That(pins.RemainingPins, Is.EqualTo(1), "the re-created leaf must keep its pin.");
            Assert.That(SweepDecisions(sweep, "retired"), Is.Zero);
            Assert.That(SweepDecisions(sweep, "retire_failed"), Is.EqualTo(1),
                "a fenced refusal is a removal that did not complete, re-examined by the next sweep.");
        });
    }

    [Test]
    public async Task The_sweep_removes_the_same_pin_when_no_leaf_was_recreated()
    {
        // The control: the identical fixture with no re-creation retires, so
        // the refusal above is the fence's and not an artefact of the fixture.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        storage.PutHusk(OrphanLeafGrainId(0));
        pins.Seed(OrphanSweepTree, OrphanConsumerId(0));

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerSweeping(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pins.FencedRemovals, Is.Empty);
            Assert.That(pins.RemainingPins, Is.Zero);
            Assert.That(SweepDecisions(sweep, "retired"), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_drive_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into()
    {
        const string TreeId = "orphan-drive-fence";
        var time = new VirtualTimeProvider();
        var consumerId = BlockedConsumerId(TreeId) + "_0";
        var pins = DrivePinStore(TreeId, consumerId);
        var storage = new LeafStateBook();
        storage.PutHusk(BlockedLeafGrainId());

        // The re-created leaf seeds under its route: one of the keys the pin was
        // duplicated across. Its other copies are stale and may go.
        var routeKey = DriveReadKeys(TreeId)[0];
        RecreateOnFirstReadAfterAToken(storage, pins, BlockedLeafGrainId(), TreeId, routeKey, consumerId);

        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: TreeId,
            cursorReporter: Substitute.For<ILeafCursorReporter>(),
            pins: pins,
            leafStateStorage: storage,
            orphanSweepCoordinator: InertSweepCoordinator());

        using var decisions = new InstrumentRecorder(LatticeMetrics.WalGcDriveOrphanPinRetirements, TreeId);
        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(pins.FencedRemovals, Does.Contain((routeKey, consumerId)),
                "anti-vacuity: the drive must have reached the fence at the re-created leaf's route.");
            Assert.That(pins.Removals, Does.Not.Contain((routeKey, consumerId)),
                "the re-created leaf's pin must survive the drive's retirement.");
            Assert.That(DriveDecisions(decisions, "retire_failed", "not_driven"), Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task The_drive_does_not_remove_a_pin_whose_leaf_row_is_live_again()
    {
        // The NotDriven verdict is not the evidence any more: a leaf re-created
        // before the drive's own durable read reads live and keeps every pin.
        const string TreeId = "orphan-drive-live";
        var time = new VirtualTimeProvider();
        var consumerId = BlockedConsumerId(TreeId) + "_0";
        var pins = DrivePinStore(TreeId, consumerId);
        var storage = new LeafStateBook();
        storage.PutLive(BlockedLeafGrainId(), TreeId);

        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(null),
            consumerId: consumerId,
            treeId: TreeId,
            cursorReporter: Substitute.For<ILeafCursorReporter>(),
            pins: pins,
            leafStateStorage: storage,
            orphanSweepCoordinator: InertSweepCoordinator());

        using var decisions = new InstrumentRecorder(LatticeMetrics.WalGcDriveOrphanPinRetirements, TreeId);
        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AttemptsBeforeFirstAbandonmentAsync(time, recorder);
            await scheduler.StopAsync(CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(pins.Removals, Is.Empty);
            Assert.That(DriveDecisions(decisions, "retired"), Is.Zero);
            Assert.That(DriveDecisions(decisions, "retire_failed", "not_driven"), Is.GreaterThan(0),
                "anti-vacuity: the drive must have reported NotDriven and attempted the retirement.");
        });
    }

    /// <summary>
    /// Arms <paramref name="storage"/> so that the first read of
    /// <paramref name="leaf"/> served after the pin store has issued a
    /// retirement token re-creates the leaf: its row becomes live, then its
    /// birth seed merges under <paramref name="routeKey"/>.
    /// </summary>
    private static void RecreateOnFirstReadAfterAToken(
        LeafStateBook storage,
        FakePinStore pins,
        GrainId leaf,
        string treeId,
        string routeKey,
        string consumerId)
    {
        var fired = false;
        storage.AfterRead = read =>
        {
            if (fired || read != leaf || pins.TokensIssued == 0)
            {
                return;
            }

            fired = true;
            storage.PutLive(leaf, treeId);
            pins.Merge(routeKey, consumerId);
        };
    }

    /// <summary>The read keys of <paramref name="treeId"/> under the drive fixtures' options.</summary>
    private static IReadOnlyList<string> DriveReadKeys(string treeId) =>
        WalMaterialiserPinRouting.EnumerateReadKeys(
            treeId,
            WalMaterialiserPinRouting.ResolveShardCount(Monitor(Adaptive(floor: SweepPass))));

    /// <summary>Sums one status arm of <see cref="LatticeMetrics.WalGcOrphanPinSweep"/>.</summary>
    private static long SweepDecisions(InstrumentRecorder recorder, string status) =>
        (long)recorder.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == status)
            .Sum(m => m.Value);
}

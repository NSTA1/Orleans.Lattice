using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for issue #3613: the dormant floor-holder repair arm (arm 2) runs on a
/// tree whose cursor floor reports usable, yet it used to log through the
/// blocked arm's texts - "cannot reclaim: durable materialiser pin", "unbounded
/// WAL retention" - which describe an unusable floor and send an operator after
/// the wrong fault.
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>The distinguishing fragment of the arm-2 per-episode repair report.</summary>
    private const string FloorHolderRepairReport = "is repairing dormant floor holders";

    private static async Task<RecordingLoggerFactory> DriveRepairWithLogsAsync()
    {
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var time = new VirtualTimeProvider();
        var logs = new RecordingLoggerFactory();

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));

        var leaves = new LeafTouchBook();
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Orleans.Runtime.GrainId>())
            .Returns(call => leaves.For(call.ArgAt<Orleans.Runtime.GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        var scheduler = CreateScheduler(
            factory, gc, OrphanSweepOptions(walPartitions: 1), time,
            logger: new Logger<LatticeWalGcScheduler>(logs), leafStateStorage: storage);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(leaves.Touched, Does.Contain(RepairLeafGrainId(0)),
            "precondition: the repair arm must actually have driven the holder.");
        return logs;
    }

    [Test]
    public async Task Floor_holder_repair_does_not_log_the_blocked_floor_warning()
    {
        var logs = await DriveRepairWithLogsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(CountMatching(logs, CannotReclaimWarning), Is.Zero,
                "the repair arm runs on a usable floor; the cannot-reclaim warning describes an unusable "
                    + "one and must not be emitted from it.");
            Assert.That(CountMatching(logs, "unbounded WAL retention"), Is.Zero);
        });
    }

    [Test]
    public async Task Floor_holder_repair_reports_its_own_episode_at_information()
    {
        var logs = await DriveRepairWithLogsAsync();

        var report = SingleMatching(logs, FloorHolderRepairReport);

        Assert.Multiple(() =>
        {
            Assert.That(report.Level, Is.EqualTo(LogLevel.Information));
            Assert.That(report.Message, Does.Contain(RepairConsumerId(0)));
        });
    }
}

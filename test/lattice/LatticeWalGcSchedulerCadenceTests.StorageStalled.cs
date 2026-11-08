using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Issue #6007: a drive that times out is a storage stall, not evidence that
/// driving cannot heal the leaf, so the sweep never charges or abandons it.
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private const string StalledWithoutProgress = "stalled_without_progress";

    private static (IGrainFactory Factory, Func<int> Drives) FactoryWithStallingLeaf(
        Func<long?> pinOffsetAfterDrive, bool pinReadHangs = false)
    {
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        var drives = 0;
        long? offset = null;
        leaf.DriveStarvedCheckpointAsync().Returns(_ =>
        {
            drives++;
            offset = pinOffsetAfterDrive();
            return Task.FromResult(LeafStarvationDriveOutcome.TimedOut);
        });

        var pins = Substitute.For<IWalMaterialiserPinGrain>();
        pins.GetPinOffsetsAsync().Returns(_ => pinReadHangs
            ? new TaskCompletionSource<IReadOnlyDictionary<string, long>>().Task
            : Task.FromResult<IReadOnlyDictionary<string, long>>(
                offset is { } current
                    ? new Dictionary<string, long> { [ConsumerIdFor("leaf-a")] = current }
                    : new Dictionary<string, long>()));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pins);
        return (factory, () => drives);
    }

    [Test]
    public async Task ExecuteAsync_never_abandons_a_floor_holder_whose_drives_time_out()
    {
        var consumerId = ConsumerIdFor("leaf-a");
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(consumerId)));
        var time = new VirtualTimeProvider();

        // The pin never becomes readable, which counts as no progress.
        var (factory, drives) = FactoryWithStallingLeaf(() => null);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var (scheduler, logs) = SchedulerWithLogs(factory, gc, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromHours(3));

        var outcomes = Outcomes(recorder);
        Assert.Multiple(() =>
        {
            Assert.That(drives(), Is.GreaterThan(3 * 2),
                "instrument validation: the window must hold more drives than two attempt budgets, or a "
                + "charged outcome could not have reached abandonment inside it");
            Assert.That(Count(outcomes, "abandoned"), Is.Zero,
                "a storage stall must never spend the consumer's budget into abandonment");
            Assert.That(Count(outcomes, "storage_stalled"), Is.EqualTo(drives()),
                "every timed-out drive is counted on its own uncharged arm");
            Assert.That(Count(outcomes, "completed"), Is.Zero,
                "a timed-out drive must not be folded into the charged 'completed' arm");
            Assert.That(Count(outcomes, "attempted"), Is.EqualTo(drives()),
                "a stalled drive still ran, so it is an attempt and partitions 'attempted'");
            Assert.That(logs.Entries.Count(e => e.Message.Contains(StalledWithoutProgress, StringComparison.Ordinal)),
                Is.EqualTo(1),
                "a consumer whose pin does not move across the threshold is reported once, not abandoned");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_does_not_report_a_timed_out_floor_holder_whose_pin_keeps_moving()
    {
        var consumerId = ConsumerIdFor("leaf-a");
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(consumerId)));
        var time = new VirtualTimeProvider();

        // Each drive times out but banks a prefix, so the pin advances every time.
        var pin = 100L;
        var (factory, drives) = FactoryWithStallingLeaf(() => pin += 10);

        var (scheduler, logs) = SchedulerWithLogs(factory, gc, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromHours(3));

        Assert.Multiple(() =>
        {
            Assert.That(drives(), Is.GreaterThan(LatticeWalGcScheduler.MaxStorageStallsWithoutProgress),
                "instrument validation: more stalls than the threshold must have run");
            Assert.That(logs.Entries.Any(e => e.Message.Contains(StalledWithoutProgress, StringComparison.Ordinal)), Is.False,
                "a slow drive that keeps moving the pin is making progress and must not be reported as stalled");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_bounds_a_stalled_drive_pin_read_that_never_answers()
    {
        var consumerId = ConsumerIdFor("leaf-a");
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(consumerId)));
        var time = new VirtualTimeProvider();

        // The stall that timed the drive out also parks the pin read forever.
        var (factory, drives) = FactoryWithStallingLeaf(() => 100, pinReadHangs: true);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var (scheduler, logs) = SchedulerWithLogs(factory, gc, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromHours(3));

        var outcomes = Outcomes(recorder);
        Assert.Multiple(() =>
        {
            Assert.That(drives(), Is.GreaterThan(LatticeWalGcScheduler.MaxStorageStallsWithoutProgress),
                "a parked pin read must not hold the touch: later passes keep re-driving the consumer");
            Assert.That(Count(outcomes, "storage_stalled"), Is.EqualTo(drives()),
                "each bounded touch still completes on the uncharged arm");
            Assert.That(Count(outcomes, "abandoned"), Is.Zero,
                "an expired pin read counts as no progress, which never abandons");
            Assert.That(logs.Entries.Count(e => e.Message.Contains(StalledWithoutProgress, StringComparison.Ordinal)),
                Is.EqualTo(1),
                "an unreadable pin is no progress, so the bound is reported once");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public void A_timed_out_consumer_is_retried_on_the_admission_ladder_until_it_stalls_without_progress()
    {
        Assert.Multiple(() =>
        {
            for (var stalls = 1; stalls <= 8; stalls++)
            {
                Assert.That(
                    LatticeWalGcScheduler.StorageStalledRetryDelay(stalls, 0, 0.5),
                    Is.EqualTo(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(stalls, 0.5)),
                    "while the pin moves, the retry follows the admission ladder");
            }

            Assert.That(
                LatticeWalGcScheduler.StorageStalledRetryDelay(6, LatticeWalGcScheduler.MaxStorageStallsWithoutProgress, 0.5),
                Is.EqualTo(LatticeWalGcScheduler.StorageStalledRetryDelay(1, LatticeWalGcScheduler.MaxStorageStallsWithoutProgress, 0.0)),
                "past the threshold the retry is capped at one fixed delay, whatever the stall count");
            Assert.That(
                LatticeWalGcScheduler.StorageStalledRetryDelay(1, LatticeWalGcScheduler.MaxStorageStallsWithoutProgress, 0.0),
                Is.GreaterThanOrEqualTo(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(20, 1.0)),
                "the cap is the cooldown, the longest delay the ladder reaches");
        });
    }
}

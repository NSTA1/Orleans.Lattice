using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Pins the reactivation sweep's handling of a leaf whose starvation drive
/// reports it latched stale (issue #3478).
/// <para>
/// Issue #3451 latches a leaf whose projection fell off the write-ahead log:
/// every later drive rethrows the latched <see cref="LeafProjectionStaleException"/>
/// until an operator rebuilds the leaf, so activation provably cannot heal it.
/// The sweep used to classify that rethrow as an ordinary fault - refundable,
/// then charged, then abandoned, then re-armed - and so re-drove the same leaf
/// on every cooldown for the life of the process. One overnight run logged
/// 3,772 such retries across 187 leaves, every one of them the latched fault.
/// </para>
/// <para>
/// The fixed behaviour is a terminal outcome: the leaf is driven once, the
/// verdict is recorded on its own arm, it is never driven again, its pin is
/// left in place so the cursor floor still holds WAL for it, and the tree is
/// reported once rather than once per leaf per cooldown.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Long enough for the unfixed sweep to spend its refunds, abandon, serve
    /// the first 30-minute backoff, re-arm and abandon again several times, so a
    /// bounded drive count here is a property of the fix and not of the window.
    /// </summary>
    private static readonly TimeSpan LatchedStaleObservationWindow = TimeSpan.FromHours(12);

    /// <summary>
    /// A permanently blocked tree whose only blocking leaf rethrows the latched
    /// stale fault on every drive, with the scheduler's log captured.
    /// </summary>
    /// <remarks>
    /// The fault is returned as a faulted task rather than thrown from the stub,
    /// because that is the shape a grain call delivers it in; the copier on
    /// <see cref="LeafProjectionStaleException"/> is what lets it cross the call
    /// with its type intact, and the scheduler's classification is by type.
    /// </remarks>
    private static (LatticeWalGcScheduler Scheduler, ILatticeWalGc Gc, RecordingLoggerFactory Logs, Func<int> Drives)
        LatchedStaleTree(
            string treeId,
            VirtualTimeProvider time,
            ILeafCursorReporter? cursorReporter = null,
            Func<Exception>? fault = null,
            Func<bool>? blocked = null)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(blocked is null || blocked()
                ? BlockedReportNaming(BlockedConsumerId(treeId))
                : Report(entriesTrimmed: 12)));

        var (factory, leaf) = FactoryWithBlockedLeaf(treeId);
        var drives = 0;
        leaf.DriveStarvedCheckpointAsync().Returns(_ =>
        {
            Interlocked.Increment(ref drives);
            return Task.FromException<LeafStarvationDriveOutcome>(
                fault?.Invoke()
                ?? new LeafProjectionStaleException(
                    "latched stale",
                    new InvalidOperationException("projection fell off the WAL")));
        });
        leaf.GetTreeIdAsync().Returns(_ => Task.FromResult<string?>(treeId));

        var logs = new RecordingLoggerFactory();
        var scheduler = CreateScheduler(
            factory,
            gc,
            Adaptive(floor: SweepPass),
            time,
            logger: new Logger<LatticeWalGcScheduler>(logs),
            cursorReporter: cursorReporter);

        return (scheduler, gc, logs, () => Volatile.Read(ref drives));
    }

    private static int GcPasses(ILatticeWalGc gc) =>
        gc.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync));

    private static int WarningsContaining(RecordingLoggerFactory logs, string fragment) =>
        logs.Warnings.Count(e => e.Message.Contains(fragment, StringComparison.Ordinal));

    [Test]
    public async Task ExecuteAsync_drives_a_latched_stale_leaf_once_and_never_again()
    {
        // The regression. Before the fix the rethrown latch was an ordinary
        // fault, so this window saw the leaf driven six times per cycle (three
        // refunded, three charged), abandoned, re-armed after the backoff and
        // driven again - without end, since a latched leaf cannot heal.
        const string TreeId = "latched-stale-once";
        var time = new VirtualTimeProvider();
        var (scheduler, gc, logs, drives) = LatchedStaleTree(TreeId, time);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, TreeId);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, LatchedStaleObservationWindow, maxPasses: 5000);

        // Anti-vacuity: the sweep must still be running at the end of the
        // window, or "driven once" would be satisfied by a scheduler that died.
        var passesBefore = GcPasses(gc);
        await AdvanceAtLeastAsync(time, TimeSpan.FromHours(1), maxPasses: 5000);
        var passesAfter = GcPasses(gc);

        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(passesAfter, Is.GreaterThan(passesBefore),
                "the GC must keep running over the tree, or a bounded drive count proves nothing.");
            Assert.That(drives(), Is.EqualTo(1),
                "a latched leaf cannot heal by activation, so it must be driven exactly once and never re-driven.");
            Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(1),
                "only the single drive may be charged as an attempt.");
            Assert.That(Outcomes(recorder, "faulted"), Is.Zero,
                "the latched fault is a verdict about the leaf, not a transient fault about the silo.");
            Assert.That(Outcomes(recorder, "abandoned"), Is.Zero,
                "a terminal consumer is not abandoned, because abandonment is a pause that re-arms.");
            Assert.That(Outcomes(recorder, "rearmed"), Is.Zero,
                "re-arming is what turned the give-up into a longer cooldown and the retry loop into a forever loop.");
            Assert.That(WarningsContaining(logs, "could not reactivate leaf"), Is.Zero,
                "the per-leaf retry warning was the log volume issue #3478 measured; a latched leaf must not produce it.");
        });
    }

    [Test]
    public async Task ExecuteAsync_records_the_latched_verdict_on_its_own_arm_exactly_once()
    {
        const string TreeId = "latched-stale-arm";
        var time = new VirtualTimeProvider();
        var (scheduler, _, _, _) = LatchedStaleTree(TreeId, time);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, TreeId);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, LatchedStaleObservationWindow, maxPasses: 5000);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Outcomes(recorder, "latched_stale"), Is.EqualTo(1),
                "the verdict is recorded once, on the drive that produced it, and never again.");
            Assert.That(Outcomes(recorder, "healed"), Is.Zero,
                "a latched leaf's pin never advances, so nothing about it may be credited as a heal.");
            Assert.That(Outcomes(recorder, "completed"), Is.Zero,
                "the drive did not complete; it was refused by the latch.");
        });
    }

    [Test]
    public async Task ExecuteAsync_reports_a_tree_blocked_by_a_latched_leaf_once_per_pass()
    {
        // The per-leaf warning is gone, so the tree needs a report of its own -
        // but one per pass, not one per leaf per cooldown, which was the volume
        // the issue measured.
        const string TreeId = "latched-stale-report";
        var time = new VirtualTimeProvider();
        var (scheduler, gc, logs, drives) = LatchedStaleTree(TreeId, time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 5000 && drives() == 0; i++)
        {
            await TickAsync(time);
        }

        Assert.That(drives(), Is.EqualTo(1), "the leaf must have been driven before its report can be measured.");

        var passesBefore = GcPasses(gc);
        var reportsBefore = WarningsContaining(logs, "latched stale leaves");
        await AdvanceAtLeastAsync(time, TimeSpan.FromHours(1), maxPasses: 5000);
        var passDelta = GcPasses(gc) - passesBefore;
        var reportDelta = WarningsContaining(logs, "latched stale leaves") - reportsBefore;

        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reportsBefore, Is.EqualTo(1),
                "the pass that latched the leaf reports the tree once, not once per drive attempt.");
            Assert.That(passDelta, Is.GreaterThan(0), "anti-vacuity: the GC must have kept running.");
            Assert.That(reportDelta, Is.EqualTo(passDelta),
                "a blocked tree is reported exactly once on every pass while the latch holds.");
            Assert.That(WarningsContaining(logs, "has not been able to attempt"), Is.Zero,
                "a terminal consumer is unattempted by design, so the unreachable-block escalation must not fire.");
        });
    }

    [Test]
    public async Task ExecuteAsync_keeps_the_pin_of_a_latched_stale_leaf_registered()
    {
        // Terminal for the SWEEP, not for the pin. The pin is what keeps the
        // floor from trimming WAL the leaf still needs once an operator rebuilds
        // it, so the sweep must never retire it on the strength of the latch.
        const string TreeId = "latched-stale-pin";
        var reporter = Substitute.For<ILeafCursorReporter>();
        var time = new VirtualTimeProvider();
        var (scheduler, _, _, drives) = LatchedStaleTree(TreeId, time, cursorReporter: reporter);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, LatchedStaleObservationWindow, maxPasses: 5000);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(drives(), Is.EqualTo(1), "anti-vacuity: the latched drive must have happened.");
        await reporter.DidNotReceive().UnregisterAsync(
            TreeId, BlockedConsumerId(TreeId), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExecuteAsync_does_not_drive_a_latched_leaf_again_through_its_sibling_partition_pins()
    {
        // A leaf publishes one pin per WAL partition and the drive repairs every
        // partition at once, so the latch is a verdict on the LEAF. A sibling
        // pin rotating into the report later must inherit it, not buy the leaf
        // a fresh drive under a different consumer id.
        const string TreeId = "latched-stale-siblings";
        const int SiblingSlotPasses = 6;
        var siblings = Enumerable.Range(0, 4)
            .Select(p => $"{BlockedConsumerId(TreeId)}_{p}")
            .ToArray();

        // Sibling 0 is reported until the latch lands on it; from then on the
        // report names only the other partitions in turn, each for
        // SiblingSlotPasses floor-rate passes (30 minutes), which is long enough
        // to age past the sweep's minimum block age, so each would be driven
        // were the latch not inherited from the leaf.
        var gc = Substitute.For<ILatticeWalGc>();
        var drives = 0;
        var passesSinceLatch = 0;
        var reportedSiblings = new HashSet<string>(StringComparer.Ordinal);
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var consumer = Volatile.Read(ref drives) == 0
                    ? siblings[0]
                    : siblings[1 + (passesSinceLatch++ / SiblingSlotPasses % (siblings.Length - 1))];
                reportedSiblings.Add(consumer);
                return Task.FromResult(BlockedReportNaming(consumer));
            });

        var (factory, leaf) = FactoryWithBlockedLeaf(TreeId);
        leaf.DriveStarvedCheckpointAsync().Returns(_ =>
        {
            Interlocked.Increment(ref drives);
            return Task.FromException<LeafStarvationDriveOutcome>(
                new LeafProjectionStaleException("latched stale", new InvalidOperationException("gone")));
        });
        leaf.GetTreeIdAsync().Returns(_ => Task.FromResult<string?>(TreeId));

        var options = Adaptive(floor: SweepPass);
        options.WalPartitions = siblings.Length;
        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(factory, gc, options, time);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, TreeId);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, LatchedStaleObservationWindow, maxPasses: 5000);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reportedSiblings, Is.EquivalentTo(siblings),
                "anti-vacuity: every sibling pin must have rotated through the report.");
            Assert.That(passesSinceLatch, Is.GreaterThan(SiblingSlotPasses * (siblings.Length - 1)),
                "anti-vacuity: each later sibling must have been reported for a full rotation slot.");
            Assert.That(Volatile.Read(ref drives), Is.EqualTo(1),
                "the latched leaf is driven once in total, however many of its partition pins are reported.");
            Assert.That(Outcomes(recorder, "latched_stale"), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ExecuteAsync_does_not_credit_a_heal_to_a_latched_leaf_when_the_floor_later_clears()
    {
        // The sweep stopped driving the leaf, so a floor that clears afterwards
        // was cleared by something else - an operator rebuild. Crediting it
        // would inflate the healed/attempted ratio and advance the heal epoch
        // on evidence that says nothing about the sweep. The control for the
        // clearing transition itself is
        // ExecuteAsync_publishes_a_healed_outcome_when_a_swept_leaf_stops_blocking,
        // which credits exactly this transition for an ordinary drive.
        const string TreeId = "latched-stale-heal";
        var blocked = true;
        var time = new VirtualTimeProvider();
        var (scheduler, gc, _, drives) = LatchedStaleTree(
            TreeId, time, blocked: () => Volatile.Read(ref blocked));

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, TreeId);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 5000 && drives() == 0; i++)
        {
            await TickAsync(time);
        }

        Volatile.Write(ref blocked, false);
        var passesBefore = GcPasses(gc);
        await AdvanceAtLeastAsync(time, TimeSpan.FromHours(1), maxPasses: 5000);
        var passesAfter = GcPasses(gc);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(drives(), Is.EqualTo(1), "anti-vacuity: the latched drive must have happened.");
            Assert.That(Outcomes(recorder, "latched_stale"), Is.EqualTo(1),
                "anti-vacuity: the drive must have been classified latched, or the exclusion is not exercised.");
            Assert.That(passesAfter, Is.GreaterThan(passesBefore),
                "anti-vacuity: the GC must have observed the cleared floor.");
            Assert.That(Outcomes(recorder, "healed"), Is.Zero,
                "a consumer the sweep stopped driving did not heal because of the sweep and must not be credited.");
        });
    }

    [Test]
    public async Task ExecuteAsync_still_retries_a_leaf_whose_drive_faults_for_any_other_reason()
    {
        // The positive control. Identical rig, an ordinary fault in place of the
        // latch: the sweep must keep its existing refund-then-charge-then-abandon
        // behaviour. Without this, the tests above would also pass for a sweep
        // that simply stopped retrying faults altogether.
        const string TreeId = "latched-stale-control";
        var time = new VirtualTimeProvider();
        var (scheduler, _, _, drives) = LatchedStaleTree(
            TreeId, time, fault: () => new InvalidOperationException("transient"));

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, TreeId);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, LatchedStaleObservationWindow, maxPasses: 5000);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(drives(), Is.GreaterThan(1), "an ordinary fault is still retried.");
            Assert.That(Outcomes(recorder, "faulted"), Is.GreaterThan(0));
            Assert.That(Outcomes(recorder, "abandoned"), Is.GreaterThanOrEqualTo(1));
            Assert.That(Outcomes(recorder, "latched_stale"), Is.Zero,
                "only the typed latch is terminal; the classification is by type, not by fault.");
        });
    }
}

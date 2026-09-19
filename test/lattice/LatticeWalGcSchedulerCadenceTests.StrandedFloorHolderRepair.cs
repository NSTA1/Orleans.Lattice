using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the dormant floor-holder remedy being reachable on the evidence of
/// a retained backlog alone (issue #3229), which is the root cause of the
/// unbounded WAL growth first reported as issue #3094.
/// <para>
/// <b>The remedy was gated on configuration rather than on evidence.</b> The
/// classifying sweep that populates the repairable-floor-holder set ran only
/// when <c>BytePressureOverThreshold</c> was true, and that flag is decided by
/// <c>LatticeOptions.WalMaxRetainedBytes</c>, which <b>has no default</b>. So on
/// every silo that configures no ceiling - which is every silo that has not been
/// tuned - the sweep never classified, the candidate set was never populated,
/// and the drive at the foot of the arm never touched anything, for the life of
/// the process. The arm was wired in, instrumented, and unreachable.
/// </para>
/// <para>
/// <b>Measured on the live repocontext container.</b> Twelve of nineteen trees
/// read <c>blocked_leaf_reactivations_total{outcome="attempted"} = 0</c> and
/// <c>floor_holder_classification{classified} = 0</c> - one of them holding
/// 453 MB of WAL - with <c>over_ceiling</c> at zero for the whole process life.
/// Those zeros are readings rather than absences because the same instrument, in
/// the same process, reads non-zero on the single tree that did have a ceiling
/// configured: the series is being written, and these trees are being counted at
/// zero rather than failing to report. Counted by the disjunct instead of by the
/// outcome, fifteen of nineteen sit in the <c>stranded &gt; 0,
/// over_ceiling = 0</c> cell - the population this gate newly admits, and most
/// of the estate rather than an edge case.
/// </para>
/// <para>
/// <b>The configured silo had the mirror-image defect: hysteresis.</b> The
/// <c>else</c> arm dropped the candidate set the instant the tree fell back
/// under its ceiling - which is the instant the repair SUCCEEDED. Reclaim and
/// ratio-triggered compaction advanced together while over ceiling and both
/// froze for 25+ minutes at the sample the tree dropped under it, while the WAL
/// regrew at 4.29 MB/min to a peak higher than the one it started from. A remedy
/// that switches itself off as soon as it starts working is a remedy that only
/// ever oscillates.
/// </para>
/// <para>
/// <b>The second disjunct was already there.</b> <c>stranded</c> is issue
/// #3213's configuration-free backlog evidence, decided by the trim scan rather
/// than by an option; it already named this population through
/// <c>ClassifyPass</c> and already held its cadence through
/// <c>StrandedRelaxCeiling</c>. Across the very window in which the oscillator
/// froze it read 254 -> 368 and climbing while <c>over_ceiling</c> stayed frozen
/// at 39. It was true, it was measured, and it was ignored.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// The shape the oscillator settles into after a successful drain: the tree
    /// has a ceiling configured and is now <i>under</i> it, so the byte-pressure
    /// gate is shut, while the trim scan still meets WAL it may not reclaim.
    /// </summary>
    /// <remarks>
    /// This is deliberately not <c>StrandedReport()</c>, which carries no
    /// ceiling at all. The stock-silo case and the hysteresis case are two
    /// different defects with two different populations, and collapsing them
    /// into one fixture would leave the configured silo - the one the oscillator
    /// was measured on - untested.
    /// </remarks>
    private static LatticeWalGcReport UnderCeilingStrandedReport() =>
        Report(
            entriesTrimmed: 0,
            retainedBytesAfter: 512,
            byteCeiling: 1_024,
            bytePressureOverThreshold: false,
            retainedBacklog: true);

    /// <summary>
    /// The repairing scheduler of <c>SchedulerRepairing</c>, but reading its
    /// report from <paramref name="report"/> on every pass, so a test can change
    /// what the tree looks like part-way through a run.
    /// </summary>
    private static (LatticeWalGcScheduler Scheduler, LeafTouchBook Leaves) SchedulerRepairingFrom(
        FakePinStore pins,
        IGrainStorage? storage,
        VirtualTimeProvider time,
        Func<LatticeWalGcReport> report)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(report()));

        var leaves = new LeafTouchBook();
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        return (
            CreateScheduler(factory, gc, OrphanSweepOptions(), time, leafStateStorage: storage),
            leaves);
    }

    /// <summary>
    /// Seeds <paramref name="population"/> dormant floor holders that are all
    /// genuinely repairable, so a run can span more than one paying pass.
    /// </summary>
    private static (FakePinStore Pins, LeafStateBook Storage) RepairablePopulation(int population)
    {
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < population; i++)
        {
            storage.PutLive(RepairLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, RepairConsumerId(i), UnusablePin);
        }

        return (pins, storage);
    }

    /// <summary>
    /// Drives passes until the arm has touched at least one leaf, so a test can
    /// act on the boundary between "the first paying pass has happened" and
    /// "the rest of the population is still owed a touch".
    /// </summary>
    private static async Task DriveToFirstTouchAsync(
        VirtualTimeProvider time,
        LeafTouchBook leaves,
        int maxPasses = 500)
    {
        var guard = 0;
        while (leaves.Touched.Count == 0)
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(maxPasses),
                "the arm never drove anything at all, so the fixture's own premise failed before the "
                    + "property under test could be exercised.");
        }
    }

    // ------------------------------------------------------- the stock silo

    [Test]
    public async Task A_stranded_floor_holder_is_driven_on_a_silo_with_no_byte_ceiling_configured()
    {
        // The stock-silo case, and the more important of the two: it is the
        // state of every deployment that has not tuned WalMaxRetainedBytes, and
        // that option has no default. Before the fix the gate could not be
        // opened on such a silo by any amount of WAL growth, because the only
        // key it accepted was one nobody had turned.
        //
        // Nothing here is over any ceiling, because nothing here HAS a ceiling:
        // StrandedReport carries no byte accounting whatsoever. The only
        // evidence the tree offers is that its trim scan met WAL it could not
        // reclaim, which is exactly the evidence #3213 added for this purpose.
        var (pins, storage) = RepairablePopulation(1);

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairingFrom(pins, storage, time, () => StrandedReport());

        using var reactivations = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        var attempted = reactivations.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagOutcome) as string) == "attempted")
            .Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(pins.KeysRead, Is.Not.Empty,
                "the classifying sweep must have run. An empty read log means the gate was never opened, "
                    + "so the candidate set was never populated and the drive below it had nothing to "
                    + "spend its budget on - which is the defect, not a quiet tree.");
            Assert.That(leaves.Touched, Does.Contain(RepairLeafGrainId(0)),
                "and the repairable holder must actually be driven. The repair only runs inside an "
                    + "activation, so a holder that is never touched holds the floor forever.");
            Assert.That(attempted, Is.EqualTo(1),
                "the arm must report a real attempt, and this is the assertion that cannot be satisfied "
                    + "by a branch merely being entered. The other arms of this instrument can come into "
                    + "existence at zero without anything having been driven, so a series appearing is "
                    + "not evidence of a repair; a positive 'attempted' count is, because it is written "
                    + "only where a touch is actually issued.");
        });
    }

    // ------------------------------------------------------ the anti-hysteresis

    [Test]
    public async Task The_repair_keeps_driving_after_it_takes_the_tree_back_under_its_ceiling()
    {
        // The anti-hysteresis property, and the one an assertion of the form
        // "a repair happened" cannot make: a test that only ever observes a tree
        // over its ceiling passes on the defective build, because the defective
        // build repairs that tree perfectly well. The property is about what
        // happens at the moment the repair SUCCEEDS.
        //
        // The two phases are admitted by different disjuncts, which is what makes
        // this a handover test rather than a repetition of the byte-ceiling
        // fixtures. Phase one is over ceiling and NOT stranded, so only
        // `overCeiling` can admit it; phase two is under ceiling and stranded,
        // so only `stranded` can. A build that never learned the second disjunct
        // stops dead at the phase boundary with the population half-drained,
        // which is precisely the measured 25-minute freeze.
        //
        // The population deliberately exceeds MaxReactivationTouchesPerPass, so
        // the boundary can be placed between the pass that drives the first
        // budget and the passes that owe the rest. Nothing here depends on
        // re-driving a leaf that was already driven, so the retry cooldown and
        // the heal credit are not load-bearing for the assertion.
        const int Population = 8;

        var (pins, storage) = RepairablePopulation(Population);

        var time = new VirtualTimeProvider();
        var overCeiling = true;
        var (scheduler, leaves) = SchedulerRepairingFrom(
            pins,
            storage,
            time,
            () => overCeiling ? OverCeilingReport() : UnderCeilingStrandedReport());

        await StartAndRunFirstPassAsync(scheduler, time);
        await DriveToFirstTouchAsync(time, leaves);

        var touchedWhileBreaching = leaves.Touched.Count;

        // The drain worked. That is the whole point: the tree is under its
        // ceiling BECAUSE the remedy is running, and it is still holding a
        // backlog because the rest of the population still holds the floor.
        overCeiling = false;

        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(20));
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(touchedWhileBreaching, Is.EqualTo(TouchesPerPass),
                "the fixture's own premise: the boundary has to fall with the population part-drained, "
                    + "or there is nothing left for the second phase to finish and the test passes "
                    + "vacuously on any build.");
            Assert.That(leaves.Touched.Distinct().Count(), Is.EqualTo(Population),
                "every holder must still be drained after the tree falls under its ceiling. A count "
                    + "frozen at the first pass's budget is the hysteresis oscillator: the candidate set "
                    + "is discarded at the exact moment the repair starts working, the floor stops "
                    + "moving, and the WAL regrows to a higher peak than the one it started from.");
        });
    }

    // ------------------------------------------- the candidate set is still dropped

    [Test]
    public async Task A_tree_that_stops_holding_a_backlog_drops_its_candidate_set()
    {
        // The guard against reading the fix as "never forget candidates". This
        // is the same run as the anti-hysteresis fixture above with one field
        // changed in the second phase - the tree stops reporting a retained
        // backlog - and the required outcome is the exact opposite. Together the
        // two pin the gate to the evidence rather than to either answer.
        //
        // It has to stop. The candidate set is a SAMPLE, taken under a condition;
        // once neither condition holds, no sweep will refresh it, and a drive
        // that kept spending touches off it would be spending durable reads and
        // activations on a tree with no symptom to explain.
        const int Population = 8;

        var (pins, storage) = RepairablePopulation(Population);

        var time = new VirtualTimeProvider();
        var pressured = true;
        var (scheduler, leaves) = SchedulerRepairingFrom(
            pins,
            storage,
            time,
            () => pressured ? OverCeilingReport() : Report(0));

        await StartAndRunFirstPassAsync(scheduler, time);
        await DriveToFirstTouchAsync(time, leaves);

        var touchedWhilePressured = leaves.Touched.Count;

        // Neither breaching nor stranded: the trim scan ran, met no WAL it had
        // to retain, and the tree has no symptom left to explain.
        pressured = false;

        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(20));
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(touchedWhilePressured, Is.EqualTo(TouchesPerPass),
                "the fixture's own premise: the sample must have been taken and partly spent, or there "
                    + "is no candidate set for the healthy pass to drop.");
            Assert.That(leaves.Touched, Has.Count.EqualTo(touchedWhilePressured),
                "a tree that is neither breaching nor stranded must drive nothing further. A count that "
                    + "keeps climbing means the sample outlived every condition that licensed it, and the "
                    + "arm is now spending its budget off a classification no sweep will ever replace.");
        });
    }
}

using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Liveness tests for the silo-scoped instruments <see cref="LatticeWalGcScheduler"/>
/// publishes about its own loop (issue #3060).
/// <para>
/// The defect these exist for is not that the scheduler reported a wrong value.
/// It is that a silo whose sweep had stopped produced a scrape <i>byte-identical</i>
/// to one whose sweep was running and finding nothing to do: every per-tree
/// series is written inside the collection stage, so a loop that dies at or
/// before the registry enumeration writes nothing at all, and a frozen series is
/// equally consistent with "stopped" and with "alive, retrying, failing every
/// pass on a cadence that has relaxed past the observation window". These six
/// instruments exist to separate those two readings, so the properties worth
/// testing are almost all about what is emitted on the <i>failing</i> paths.
/// </para>
/// <para>
/// Every counter arm here is therefore proved to advance <b>in isolation</b> -
/// one arm up, every sibling arm still at zero - rather than merely to be
/// non-zero. An arm that advances whenever any other one does is not a
/// discriminator, and a taxonomy whose arms are not separately reachable is a
/// single boolean wearing six labels.
/// </para>
/// </summary>
/// <remarks>
/// <see cref="NonParallelizableAttribute"/> is declared on this partial and so
/// applies to the whole fixture. The phase census is process-global by design -
/// a silo has exactly one WAL GC loop - so two schedulers running concurrently
/// would overwrite each other's phase. The assembly declares no parallelism
/// today, which makes this redundant today and correct tomorrow.
/// </remarks>
[NonParallelizable]
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>Every arm of the enumeration taxonomy, as the tags name them.</summary>
    private static readonly string[] EveryEnumerationOutcome =
        ["succeeded", "faulted", "cancelled", "empty", "all_blank", "timed_out"];

    /// <summary>Every arm of the termination taxonomy, as the tags name them.</summary>
    private static readonly string[] EveryTermination = ["disabled", "cancelled", "faulted"];

    /// <summary>
    /// A registry whose answer the test controls after the fact, for the arms
    /// that need an enumeration which is in flight rather than finished.
    /// </summary>
    private static IGrainFactory FactoryAwaiting(Task<IReadOnlyList<string>> answer, TaskCompletionSource called)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(_ =>
        {
            called.TrySetResult();
            return answer;
        });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return factory;
    }

    /// <summary>A registry that throws rather than answering.</summary>
    private static IGrainFactory FactoryThatThrows()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync()
            .Returns<Task<IReadOnlyList<string>>>(_ => throw new InvalidOperationException("registry wedged"));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return factory;
    }

    /// <summary>A collector whose pass never returns, for the bounded-await arms.</summary>
    private static ILatticeWalGc GcParkedOn(Task<LatticeWalGcReport> parked, string treeId, TaskCompletionSource called)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                if (!string.Equals((string)call[0], treeId, StringComparison.Ordinal))
                {
                    return Task.FromResult(Report(entriesTrimmed: 0));
                }

                called.TrySetResult();
                return parked;
            });
        return gc;
    }

    /// <summary>A collector that reclaims, having first burned <paramref name="cost"/> of the clock.</summary>
    private static ILatticeWalGc GcCosting(VirtualTimeProvider time, TimeSpan cost)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                time.Advance(cost);
                return Task.FromResult(Report(entriesTrimmed: 9));
            });
        return gc;
    }

    /// <summary>A collector that reclaims nothing, quickly.</summary>
    private static ILatticeWalGc IdleGc()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0)));
        return gc;
    }

    private static string[] ArmsOn(SiloInstrumentRecorder recorder, string tagKey) =>
        recorder.Measurements
            .Select(m => m.Tag(tagKey) as string)
            .Where(a => a is not null)
            .Distinct()
            .ToArray()!;

    private static double ArmTotal(SiloInstrumentRecorder recorder, string tagKey, string arm) =>
        recorder.Measurements
            .Where(m => string.Equals(m.Tag(tagKey) as string, arm, StringComparison.Ordinal))
            .Sum(m => m.Value);

    // ------------------------------------------------- the arms exist at zero

    [Test]
    public async Task Every_enumeration_outcome_is_primed_so_an_absent_arm_is_never_a_healthy_reading()
    {
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();

        using var enumerations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerEnumerations);
        var scheduler = CreateScheduler(FactoryWithTrees("walgc-liveness-prime"), IdleGc(), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        // Priming is by a walk of the enum rather than a list of arms, so this
        // assertion is also the guard on that walk: an arm added to the enum and
        // not to the mapping cannot pass here, and an arm added to both is
        // covered without anyone remembering to extend a list.
        Assert.That(
            ArmsOn(enumerations, LatticeMetrics.TagOutcome),
            Is.EquivalentTo(EveryEnumerationOutcome),
            "every enumeration arm must exist from the first pass, or its zero reads as "
            + "'this build has no such instrument' rather than 'measured, never happened'.");
    }

    [Test]
    public async Task Every_termination_reason_is_primed_before_the_loop_can_reach_one()
    {
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();

        using var terminations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerTerminations);
        var scheduler = CreateScheduler(FactoryWithTrees("walgc-liveness-term-prime"), IdleGc(), Adaptive(), time);
        await StartArmedAsync(scheduler, time);

        // Asserted while the loop is still running, which is the whole point:
        // a termination counter primed at the moment of termination would be
        // absent for exactly as long as the interesting question - "has this
        // loop stopped?" - is open.
        Assert.That(
            ArmsOn(terminations, LatticeMetrics.TagReason),
            Is.EquivalentTo(EveryTermination),
            "every termination arm must exist while the loop is alive, so that a reader "
            + "asking whether it stopped gets a zero rather than a missing series.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task The_started_pass_counter_is_primed_above_the_startup_delay()
    {
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();

        using var started = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerPassesStarted);
        var scheduler = CreateScheduler(FactoryWithTrees("walgc-liveness-heartbeat"), IdleGc(), Adaptive(), time);

        // Armed, not ticked: the scheduler is parked on its startup stagger and
        // has not run a pass. The counter must already exist.
        await StartArmedAsync(scheduler, time);

        Assert.Multiple(() =>
        {
            Assert.That(started.Measurements, Is.Not.Empty,
                "the heartbeat must be published before the first pass, or a silo still inside "
                + "its startup window is indistinguishable from one whose loop never started.");
            Assert.That(started.Counted, Is.Empty,
                "and priming must not fabricate a pass that has not happened.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    // ------------------------------------- one enumeration arm at a time only

    [TestCase("succeeded")]
    [TestCase("empty")]
    [TestCase("all_blank")]
    [TestCase("faulted")]
    public async Task An_enumeration_advances_exactly_one_outcome_arm(string expected)
    {
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();
        var factory = expected switch
        {
            "succeeded" => FactoryWithTrees($"walgc-enum-{expected}"),
            "empty" => FactoryFor(() => []),

            // Empty rather than whitespace, because the classifier's predicate
            // is the same IsNullOrEmpty the collection loop skips on. A blank id
            // the loop would have collected is not a blank enumeration, and a
            // classifier disagreeing with the loop it describes would report an
            // outcome no pass actually had.
            "all_blank" => FactoryFor(() => ["", ""]),
            _ => FactoryThatThrows(),
        };

        using var enumerations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerEnumerations);
        var scheduler = CreateScheduler(factory, IdleGc(), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        AssertExactlyOneArm(enumerations, LatticeMetrics.TagOutcome, EveryEnumerationOutcome, expected);
    }

    [Test]
    public async Task An_enumeration_that_outlives_its_budget_is_recorded_as_timed_out_and_the_loop_goes_on()
    {
        // Rider on the sixth arm: a bound that fired is a failure of the
        // measurement, not of the registry, and folding it into `faulted` would
        // let a decision of ours present as a finding about the system. This
        // asserts the separation in the direction that matters - timed_out up,
        // faulted still at zero.
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();
        var never = new TaskCompletionSource<IReadOnlyList<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var called = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var enumerations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerEnumerations);
        var scheduler = CreateScheduler(FactoryAwaiting(never.Task, called), IdleGc(), Adaptive(), time);
        await StartArmedAsync(scheduler, time);

        var budgetArmed = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(called.Task);
        await Parked(budgetArmed);

        var cadenceArmed = time.NextTimerAsync();
        time.Advance(TimeSpan.FromMinutes(1));
        await Parked(cadenceArmed);

        AssertExactlyOneArm(enumerations, LatticeMetrics.TagOutcome, EveryEnumerationOutcome, "timed_out");

        // The repair, not merely the instrument: before the bound existed this
        // await was the end of collection for the whole silo, permanently.
        Assert.That(time.LastScheduledDelay, Is.GreaterThan(TimeSpan.Zero),
            "the loop must still be scheduling passes after abandoning a hung enumeration.");

        never.TrySetResult([]);
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task An_enumeration_interrupted_by_shutdown_is_recorded_as_cancelled_not_timed_out()
    {
        // The other half of the rider, and the reason the catch discriminates on
        // which token fired rather than on exception type: a cooperative
        // shutdown and an expired budget both arrive as
        // OperationCanceledException, and an arm that absorbed shutdowns would
        // report every clean stop as a timeout.
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();
        var never = new TaskCompletionSource<IReadOnlyList<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var called = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var enumerations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerEnumerations);
        var scheduler = CreateScheduler(FactoryAwaiting(never.Task, called), IdleGc(), Adaptive(), time);
        await StartArmedAsync(scheduler, time);

        var budgetArmed = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(called.Task);
        await Parked(budgetArmed);

        await scheduler.StopAsync(CancellationToken.None);
        never.TrySetResult([]);

        AssertExactlyOneArm(enumerations, LatticeMetrics.TagOutcome, EveryEnumerationOutcome, "cancelled");
    }

    // ------------------------------------------------ the loop says why it stopped

    [Test]
    public async Task A_disabled_scheduler_records_a_disabled_termination_and_parks_the_census()
    {
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();
        var options = Adaptive();
        options.WalGcInterval = TimeSpan.Zero;

        using var terminations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerTerminations);
        using var phase = new PhaseGaugeRecorder();
        var scheduler = CreateScheduler(FactoryWithTrees("walgc-disabled"), IdleGc(), options, time);

        // A disabled loop arms no timer, so the fixture's usual arm-event
        // synchronisation has nothing to wait on - and StartAsync returning does
        // not mean ExecuteAsync has run even its first statement. Synchronise on
        // the loop's own completion instead: this is the one exit that reaches
        // it. Asserting straight after StartAsync reads a zero produced by
        // machinery that has not executed yet, which is byte-identical to the
        // measured zero this test exists to distinguish from.
        await scheduler.StartAsync(CancellationToken.None);
        await Parked(scheduler.ExecuteTask!);

        AssertExactlyOneArm(terminations, LatticeMetrics.TagReason, EveryTermination, "disabled");
        Assert.That(phase.Scrape().Single().Tag(LatticeMetrics.TagPhase), Is.EqualTo("disabled"),
            "a configured-off scheduler and a stopped one are different facts, and a reader "
            + "who cannot tell them apart will chase a defect that is a setting.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task A_cancelled_scheduler_records_a_cancelled_termination()
    {
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();

        using var terminations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerTerminations);
        var scheduler = CreateScheduler(FactoryWithTrees("walgc-cancelled"), IdleGc(), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        AssertExactlyOneArm(terminations, LatticeMetrics.TagReason, EveryTermination, "cancelled");
    }

    [Test]
    public async Task A_faulted_loop_records_a_faulted_termination_and_still_rethrows()
    {
        // The arm most likely to ship unreachable, so it is driven by a fault on
        // the loop's own path rather than by anything the pass handles: every
        // in-pass failure mode is already caught and turned into a relaxed
        // cadence, which is exactly why an uncaught one leaves no trace at all.
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();
        var faulting = new FaultingTimerProvider(time);

        using var terminations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerTerminations);
        using var phase = new PhaseGaugeRecorder();
        var scheduler = CreateScheduler(FactoryWithTrees("walgc-faulted"), IdleGc(), Adaptive(), faulting);
        await StartArmedAsync(scheduler, time);

        faulting.FailNextArm();
        time.Advance(time.LastScheduledDelay);

        var executing = scheduler.ExecuteTask;
        Assert.That(executing, Is.Not.Null);
        Assert.ThrowsAsync<InvalidOperationException>(() => Parked(executing!),
            "a fault must reach the host that is configured to act on it; swallowing it here "
            + "would convert an actionable crash into the silent permanent stop this issue is about.");

        Assert.Multiple(() =>
        {
            Assert.That(ArmTotal(terminations, LatticeMetrics.TagReason, "faulted"), Is.EqualTo(1));
            Assert.That(ArmTotal(terminations, LatticeMetrics.TagReason, "cancelled"), Is.Zero,
                "a fault is not a shutdown, and the two arms exist to be read against each other.");
            Assert.That(phase.Scrape().Single().Tag(LatticeMetrics.TagPhase), Is.EqualTo("stopped"));
        });
    }

    // ---------------------------------------------------- the anchored pair

    [Test]
    public async Task Each_started_pass_contributes_exactly_one_wait_and_one_duration_observation()
    {
        // The anchor that stops both histograms going vacuous. Neither is
        // zero-primed - the empty state of a duration distribution is undefined
        // rather than zero - so their liveness cannot be read from their own
        // presence. It is read from this relation against a counter that IS
        // primed: observations frozen while the heartbeat still climbs is a
        // broken instrument, and both frozen together is a stopped loop.
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();

        using var started = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerPassesStarted);
        using var wait = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerWait);
        using var duration = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerPassDuration);

        var scheduler = CreateScheduler(FactoryWithTrees("walgc-anchor"), IdleGc(), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        var passes = started.Counted.Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(passes, Is.EqualTo(3), "three passes were driven.");
            Assert.That(wait.Measurements, Has.Count.EqualTo((int)passes),
                "the selected-wait histogram must carry exactly one observation per started pass, "
                + "or its count cannot be used to tell 'not reporting' from 'not running'.");
            Assert.That(duration.Measurements, Has.Count.EqualTo((int)passes),
                "and so must the pass-duration histogram, which is recorded in a finally for "
                + "exactly this reason.");
        });
    }

    [Test]
    public async Task A_pass_that_fails_at_the_enumeration_is_still_measured_for_duration()
    {
        // The population whose duration is the diagnosis. A pass that dies at a
        // response timeout has a duration which is itself the finding, so
        // recording only on the success path would discard the one sample worth
        // having - and would break the anchor above on precisely the passes
        // where the anchor is being consulted.
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();

        using var started = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerPassesStarted);
        using var duration = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerPassDuration);

        var scheduler = CreateScheduler(FactoryThatThrows(), IdleGc(), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(
            duration.Measurements,
            Has.Count.EqualTo((int)started.Counted.Sum(m => m.Value)),
            "a pass that never reached a tree is still a pass, and the anchor must hold on "
            + "the failing path or it holds only where it is not needed.");
    }

    [Test]
    public async Task A_pass_that_dies_after_its_enumeration_is_still_measured_for_duration()
    {
        // The test that actually proves the finally, and the boundary on the one
        // above it. A registry that throws is caught *inside* the pass body,
        // which then returns normally - so that scenario is served by the
        // success path and cannot distinguish a finally from a record placed
        // after the call. Only a pass that throws its way out of the body can,
        // and that is the population whose duration is the diagnosis: a pass
        // killed mid-flight by a response timeout leaves no report, no tree
        // series, and nothing else to read.
        //
        // Driven by a registry answer that faults when the scheduler inspects
        // it, which lands after the guarded enumeration and before any tree is
        // collected.
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();

        using var started = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerPassesStarted);
        using var duration = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerPassDuration);
        using var terminations = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerTerminations);

        var scheduler = CreateScheduler(
            FactoryFor(() => new HostileTreeList()), IdleGc(), Adaptive(), time);
        await StartArmedAsync(scheduler, time);

        var loop = scheduler.ExecuteTask!;
        time.Advance(time.LastScheduledDelay);
        Assert.That(async () => await Parked(loop), Throws.InstanceOf<InvalidOperationException>(),
            "the loop must still surface the fault to its host; measuring it is not swallowing it.");

        Assert.Multiple(() =>
        {
            Assert.That(started.Counted.Sum(m => m.Value), Is.EqualTo(1),
                "the pass started, which is what makes its missing duration a hole rather than "
                + "an absence.");
            Assert.That(duration.Measurements, Has.Count.EqualTo(1),
                "and a pass that threw its way out of the body must still have been measured, or "
                + "the anchor holds only on the passes nobody needs it for.");
            Assert.That(ArmTotal(terminations, LatticeMetrics.TagReason, "faulted"), Is.EqualTo(1),
                "and the loop must say why it stopped.");
        });
    }

    [Test]
    public async Task Pass_duration_reports_the_elapsed_pass_and_not_the_wait_the_scheduler_selected()
    {
        // The divergence that motivates the sixth instrument, reproduced in
        // miniature. A scheduler whose per-tree work runs long still selects its
        // adaptive floor, so the selected wait reads a flat floor value while the
        // real period between passes is floor-plus-work. The selected wait is not
        // wrong - the scheduler really did choose it - it is structurally blind
        // to what happened after the choice, and only this pair names the gap.
        WalGcSchedulerPhaseCensus.ResetForTests();
        var time = new VirtualTimeProvider();
        var cost = TimeSpan.FromSeconds(45);

        using var wait = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerWait);
        using var duration = new SiloInstrumentRecorder(LatticeMetrics.WalGcSchedulerPassDuration);

        var scheduler = CreateScheduler(
            FactoryWithTrees("walgc-divergence"), GcCosting(time, cost), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(wait.Measurements.Single().Value, Is.EqualTo(Floor.TotalSeconds),
                "the scheduler selected its floor, and the selected-wait histogram reports it.");
            Assert.That(duration.Measurements.Single().Value, Is.EqualTo(cost.TotalSeconds),
                "while the pass itself took the work's time - the quantity the selected wait "
                + "cannot see, and the one a stalled sweep shows up in.");
        });
    }

    // ------------------------------------------------------- the phase spine

    [Test]
    public void The_phase_census_reports_unstarted_before_any_scheduler_runs()
    {
        // The build witness for the whole set. An observable gauge emits from
        // its declaration site, so this series exists in a process that has
        // never constructed a scheduler - which is what makes its absence mean
        // "this build predates the instruments" rather than "the loop is idle",
        // and what lets a reader trust the zero on the four counters beside it.
        WalGcSchedulerPhaseCensus.ResetForTests();

        using var phase = new PhaseGaugeRecorder();
        var observed = phase.Scrape();

        Assert.Multiple(() =>
        {
            Assert.That(observed, Has.Count.EqualTo(1),
                "exactly one series: a loop is in one phase, and publishing a stale age for the "
                + "ten it is not in would leave a reader to guess which number is live.");
            Assert.That(observed[0].Tag(LatticeMetrics.TagPhase), Is.EqualTo("unstarted"));
            Assert.That(observed[0].Tag(LatticeMetrics.TagTree), Is.Null,
                "and no tree dimension outside the collecting stage.");
        });
    }

    [Test]
    public async Task The_phase_census_names_the_parked_await_and_its_age_climbs()
    {
        // The property the counters cannot deliver. A counter can only be
        // advanced by code that runs, so no counter can report a loop parked
        // inside an await that never returns; the gauge reports from the
        // collector's thread and therefore can. This is the instrument that
        // makes a stall a location rather than an inference.
        WalGcSchedulerPhaseCensus.ResetForTests();
        const string Tree = "walgc-parked";
        var time = new VirtualTimeProvider();
        var never = new TaskCompletionSource<LatticeWalGcReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var called = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var phase = new PhaseGaugeRecorder();
        var scheduler = CreateScheduler(
            FactoryWithTrees(Tree), GcParkedOn(never.Task, Tree, called), Adaptive(), time);
        await StartArmedAsync(scheduler, time);

        var budgetArmed = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(called.Task);
        await Parked(budgetArmed);

        var parked = phase.Scrape().Single();
        time.Advance(TimeSpan.FromMinutes(5));
        var aged = phase.Scrape().Single();

        Assert.Multiple(() =>
        {
            Assert.That(parked.Tag(LatticeMetrics.TagPhase), Is.EqualTo("collecting.gc_run"),
                "the four collecting phases are split precisely so a stall inside collection "
                + "names a statement; one 'collecting' phase would reproduce the defect a level down.");
            Assert.That(parked.Tag(LatticeMetrics.TagTree), Is.EqualTo(Tree),
                "and the collecting stage is the one place a stall is attributable to a tree.");
            Assert.That(aged.Value - parked.Value, Is.EqualTo(300).Within(1),
                "the age must climb while the loop is parked, or the gauge reports a location "
                + "without reporting that it is stuck there.");
        });

        never.TrySetResult(Report(entriesTrimmed: 0));
        await scheduler.StopAsync(CancellationToken.None);
    }

    // ------------------------------------------------------ the bounded awaits

    [Test]
    public async Task A_tree_whose_collect_outlives_its_budget_is_abandoned_and_its_siblings_still_collect()
    {
        // The second half of the repair. The enumeration bound protects the
        // silo from a registry that never answers; this one protects every other
        // tree from one tree that never finishes, which before this change ended
        // collection silo-wide for as long as the await lasted.
        WalGcSchedulerPhaseCensus.ResetForTests();
        const string Stuck = "walgc-budget-stuck";
        const string Sibling = "walgc-budget-sibling";
        var time = new VirtualTimeProvider();
        var never = new TaskCompletionSource<LatticeWalGcReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var called = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var stuckPasses = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Stuck);
        using var siblingPasses = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Sibling);
        var scheduler = CreateScheduler(
            FactoryWithTrees(Stuck, Sibling), GcParkedOn(never.Task, Stuck, called), Adaptive(), time);
        await StartArmedAsync(scheduler, time);

        var budgetArmed = time.NextTimerAsync();
        time.Advance(time.LastScheduledDelay);
        await Parked(called.Task);
        await Parked(budgetArmed);

        var cadenceArmed = time.NextTimerAsync();
        time.Advance(TimeSpan.FromMinutes(10));
        await Parked(cadenceArmed);

        Assert.Multiple(() =>
        {
            Assert.That(
                stuckPasses.Counted.Select(m => m.Tag(LatticeMetrics.TagOutcome)),
                Is.EqualTo(new[] { LatticeMetrics.OutcomeFailed.Value }),
                "an abandoned tree is a failed pass - the same treatment a throwing tree already "
                + "gets, because the remedy is the same and only the cause differs.");
            Assert.That(siblingPasses.Counted, Is.Not.Empty,
                "and the sibling must still have been collected on that same pass, which is the "
                + "entire property the bound buys.");
        });

        never.TrySetResult(Report(entriesTrimmed: 0));
        await scheduler.StopAsync(CancellationToken.None);
    }

    // ------------------------------------------------------------- assertions

    /// <summary>
    /// Asserts that <paramref name="expected"/> advanced by exactly one and that
    /// every sibling arm is still a published zero.
    /// <para>
    /// The second half is the half that matters. An arm which advances whenever
    /// any other one does is not a discriminator, and a taxonomy proved only
    /// arm-by-arm in the positive direction can be a single boolean wearing six
    /// labels without any test noticing.
    /// </para>
    /// </summary>
    private static void AssertExactlyOneArm(
        SiloInstrumentRecorder recorder,
        string tagKey,
        string[] population,
        string expected)
    {
        var observed = string.Join(
            ", ",
            recorder.Measurements.Select(m => $"{m.Tag(tagKey)}={m.Value}"));

        Assert.Multiple(() =>
        {
            Assert.That(ArmTotal(recorder, tagKey, expected), Is.EqualTo(1),
                $"the '{expected}' arm must advance by exactly one. Observed: [{observed}]");

            foreach (var arm in population)
            {
                if (string.Equals(arm, expected, StringComparison.Ordinal))
                {
                    continue;
                }

                Assert.That(ArmTotal(recorder, tagKey, arm), Is.Zero,
                    $"the '{arm}' arm must stay at a published zero, or it is not separable "
                    + $"from '{expected}' and the taxonomy discriminates nothing.");
            }
        });
    }

    // -------------------------------------------------------------- recorders

    /// <summary>
    /// Captures every measurement on one silo-scoped instrument, unfiltered.
    /// <para>
    /// <see cref="InstrumentRecorder"/> keeps only measurements carrying a
    /// test-unique tree tag, which is what makes it immune to a concurrently
    /// running fixture. These instruments are deliberately untagged by tree - a
    /// pass is a property of the loop, and a pass that failed before enumerating
    /// has no tree to attribute itself to - so that filter would discard
    /// everything. Isolation comes instead from the fixture being
    /// non-parallelizable and from every scheduler here being stopped before its
    /// measurements are read.
    /// </para>
    /// </summary>
    private sealed class SiloInstrumentRecorder : IDisposable
    {
        private readonly List<Captured> _measurements = [];
        private readonly object _gate = new();
        private readonly MeterListener _listener;

        public SiloInstrumentRecorder(Instrument instrument)
        {
            ArgumentNullException.ThrowIfNull(instrument);

            _listener = new MeterListener
            {
                InstrumentPublished = (published, listener) =>
                {
                    if (ReferenceEquals(published, instrument))
                    {
                        listener.EnableMeasurementEvents(published);
                    }
                },
            };

            _listener.SetMeasurementEventCallback<long>((_, value, tags, _) => Capture(value, tags));
            _listener.SetMeasurementEventCallback<double>((_, value, tags, _) => Capture(value, tags));
            _listener.Start();
        }

        public IReadOnlyList<Captured> Measurements
        {
            get { lock (_gate) { return _measurements.ToArray(); } }
        }

        /// <summary>The measurements recording a real event, excluding primed zeroes.</summary>
        public IReadOnlyList<Captured> Counted
        {
            get { lock (_gate) { return _measurements.Where(m => m.Value != 0).ToArray(); } }
        }

        public void Dispose() => _listener.Dispose();

        private void Capture(double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var captured = tags.ToArray();
            lock (_gate) { _measurements.Add(new Captured(value, captured)); }
        }
    }

    /// <summary>
    /// Scrapes the scheduler phase gauge on demand.
    /// <para>
    /// Matched by instrument <i>name</i> through
    /// <see cref="Orleans.Lattice.Testing.MeterListening"/> rather than by
    /// reference: touching the census type to obtain a field reference would run
    /// its static initialiser, and doing that from inside a listener callback is
    /// the re-entrancy hazard that helper exists to avoid.
    /// </para>
    /// </summary>
    private sealed class PhaseGaugeRecorder : IDisposable
    {
        private readonly List<Captured> _observations = [];
        private readonly object _gate = new();
        private readonly MeterListener _listener;

        public PhaseGaugeRecorder() =>
            _listener = Orleans.Lattice.Testing.MeterListening.StartForMeter(
                LatticeMetrics.Meter,
                [LatticeMetrics.WalGcSchedulerPhaseAgeGaugeName],
                listener => listener.SetMeasurementEventCallback<double>(
                    (_, value, tags, _) =>
                    {
                        var captured = tags.ToArray();
                        lock (_gate) { _observations.Add(new Captured(value, captured)); }
                    }));

        /// <summary>Forces one observation round and returns what the gauge reported.</summary>
        public IReadOnlyList<Captured> Scrape()
        {
            lock (_gate) { _observations.Clear(); }
            _listener.RecordObservableInstruments();
            lock (_gate) { return _observations.ToArray(); }
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// A time provider that arms one timer badly, on demand.
    /// <para>
    /// Models an unexpected fault on the scheduler loop's own path, outside any
    /// pass. Every failure mode the pass body anticipates is already caught and
    /// turned into a relaxed cadence, so the <c>faulted</c> termination arm
    /// exists precisely for what is <i>not</i> handled.
    /// </para>
    /// </summary>
    private sealed class FaultingTimerProvider(VirtualTimeProvider inner) : TimeProvider
    {
        private int _failNext;

        public void FailNextArm() => Interlocked.Exchange(ref _failNext, 1);

        public override DateTimeOffset GetUtcNow() => inner.GetUtcNow();

        public override long GetTimestamp() => inner.GetTimestamp();

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            Interlocked.Exchange(ref _failNext, 0) == 1
                ? throw new InvalidOperationException("timer arming failed")
                : inner.CreateTimer(callback, state, dueTime, period);
    }

    /// <summary>
    /// A registry answer that faults when the scheduler inspects it.
    /// <para>
    /// The registry call itself succeeds, so the pass clears its guarded
    /// enumeration and then dies on the first use of what it was handed. That
    /// places the fault after the three catches and before any tree is
    /// collected, which is the only shape that makes a pass throw its way out of
    /// the body - and therefore the only shape that can tell a duration recorded
    /// in a <c>finally</c> from one recorded after the call.
    /// </para>
    /// </summary>
    private sealed class HostileTreeList : IReadOnlyList<string>
    {
        public int Count => throw new InvalidOperationException("registry answer faulted on inspection");

        public string this[int index] => throw new InvalidOperationException("registry answer faulted on inspection");

        public IEnumerator<string> GetEnumerator() =>
            throw new InvalidOperationException("registry answer faulted on inspection");

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

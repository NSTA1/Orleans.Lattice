using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Scheduling and metering tests for the stranded-tree backoff cap (issue
/// #3213).
/// <para>
/// The adaptive cadence decided whether to relax on a single question - did
/// this pass trim anything - and answered "no" identically for two states that
/// could not be more different. A quiet tree has <i>nothing to do</i>; a tree
/// whose trim scan stops on WAL it may not reclaim <i>could not do anything</i>.
/// Both reported <c>idle</c>, so the tree with the largest backlog on the silo
/// doubled its own sweep interval toward <c>WalGcInterval</c> while that backlog
/// grew, and the more stranded it was the less often it was looked at. The live
/// container measured the extreme of it: the tree holding 1.44 GB was seen by
/// the scheduler 871 times and swept 6, while trees holding almost nothing were
/// swept at 7:1.
/// </para>
/// <para>
/// Issue #3119 had already corrected one instance of this, but only for a tree
/// breaching <c>WalMaxRetainedBytes</c> - and that option is <c>long?</c> with
/// <b>no default</b>. So on a stock deployment every floor condition was
/// permanently false on a stranded tree and there was no corrective signal at
/// all. <b>Every scheduling test in this file therefore runs with no byte
/// ceiling configured</b>, which is the case the fix exists for and the one no
/// existing fixture covered.
/// </para>
/// <para>
/// The fix is a <b>cap on the ladder, not a fourth floor arm</b>, and the tests
/// are written to hold that line from both sides. Pinning every non-reclaiming
/// tree to <c>WalGcMinInterval</c> would reintroduce the poll storm the backoff
/// was built for in issue #1030, and the stranded population is far larger than
/// the blocked or the breaching one - a tree retaining WAL behind a legitimate
/// causal frontier is an ordinary, healthy state. So a stranded tree must still
/// back off, and must still stop short of the terminal interval.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// The interval a stranded tree may relax to and no further: the fault-retry
    /// ceiling, which at the fixture's band is <c>ReactivationMinBlockAge</c>.
    /// Asserted as a literal rather than computed from the production helper, so
    /// that a change to that helper fails here and is read rather than absorbed.
    /// </summary>
    private static readonly TimeSpan StrandedCeiling = TimeSpan.FromMinutes(5);

    /// <summary>
    /// A pass that evaluated a usable cursor floor, trimmed nothing, and left
    /// WAL behind - with no byte ceiling configured anywhere, so
    /// <c>BytePressureOverThreshold</c> is false and the #3119 clause cannot
    /// fire. Byte for byte a quiet tree's report apart from the one field, which
    /// is exactly the point: before that field existed the scheduler had no way
    /// to tell these two apart.
    /// </summary>
    private static LatticeWalGcReport StrandedReport(
        long entriesTrimmed = 0,
        WalGcCursorFloorState cursorFloorState = WalGcCursorFloorState.Available) =>
        Report(
            entriesTrimmed,
            cursorFloorState: cursorFloorState,
            retainedBacklog: true);

    // ------------------------------------------------------------- scheduling

    [Test]
    public async Task ExecuteAsync_a_stranded_tree_stops_relaxing_at_the_bounded_interval_with_no_byte_ceiling_set()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(StrandedReport()));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("stranded"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The acceptance case. Every pass reclaims nothing and every pass leaves
        // a backlog, on a deployment that configured no ceiling - so under the
        // old rule this ladder ran 60s, 2m, 4m, 8m, 16m, 32m, 1h and stayed
        // there for the life of the process. It still backs off, because a
        // stranded tree is not an emergency and there are many of them; it
        // simply cannot reach the terminal interval while it is holding WAL.
        TimeSpan[] expected =
        [
            TimeSpan.FromMinutes(1),
            TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(4),
            StrandedCeiling,
            StrandedCeiling,
            StrandedCeiling,
            StrandedCeiling,
        ];

        Assert.That(time.LastScheduledDelay, Is.EqualTo(expected[0]));
        for (var pass = 1; pass < expected.Length; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(expected[pass]),
                "a tree retaining WAL it could not reclaim must relax no further than the bounded interval.");
        }

        Assert.That(time.LastScheduledDelay, Is.LessThan(Ceiling),
            "the terminal WalGcInterval must be unreachable while a backlog is outstanding.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_stranded_tree_still_backs_off_rather_than_being_pinned_to_the_floor()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(StrandedReport()));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("stranded"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The regression guard on the other side of the fix, and the reason this
        // is a cap rather than a fourth floor condition. Retaining WAL is the
        // ordinary condition of a healthy tree with a live consumer, so the
        // stranded population is most of the silo; holding all of it at the
        // cadence floor is the #1030 poll storm the adaptive backoff exists to
        // prevent. A fix that pinned the floor would pass every other assertion
        // in this file.
        Assert.That(time.LastScheduledDelay, Is.GreaterThan(Floor),
            "a stranded tree must keep backing off; pinning it to the floor reintroduces the #1030 storm.");

        for (var pass = 0; pass < 6; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.GreaterThan(Floor),
                "and must stay above the floor for as long as it remains merely stranded.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_quiet_tree_that_strands_snaps_back_to_the_bounded_interval()
    {
        var stranded = false;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stranded ? StrandedReport() : Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("drifting"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Relax it well past the cap while it is genuinely quiet: 1m, 2m, 4m,
        // 8m, 16m.
        for (var pass = 0; pass < 4; pass++)
        {
            await TickAsync(time);
        }

        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(16)));

        // Then WAL starts accumulating behind a frontier that has stopped
        // advancing. The cap has to be applied to the *inherited* ladder, not
        // only to a ladder built under it, otherwise a tree that strands after a
        // quiet period keeps the wide interval it earned while healthy - which
        // is precisely the shape the live container was in.
        stranded = true;
        await TickAsync(time);

        Assert.That(time.LastScheduledDelay, Is.EqualTo(StrandedCeiling),
            "a tree that strands after relaxing must come straight back under the cap, not keep its inherited interval.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_fully_trimmed_tree_still_relaxes_all_the_way_to_the_ceiling_interval()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0, retainedBacklog: false)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("empty"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The other half of the acceptance criteria, and the property the fix
        // must not cost. A tree whose scan ran to exhaustion or found no log at
        // all has nothing outstanding, so there is nothing for a shorter cadence
        // to recover and no reason to pay for one. It relaxes to WalGcInterval,
        // exactly as it did before the fix.
        TimeSpan[] expected =
        [
            TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(4),
            TimeSpan.FromMinutes(8),
            TimeSpan.FromMinutes(16),
            TimeSpan.FromMinutes(32),
            Ceiling,
            Ceiling,
        ];

        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(1)));
        foreach (var interval in expected)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(interval),
                "a genuinely empty or fully-trimmed tree must still relax to the ceiling interval.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_stranded_tree_and_a_quiet_tree_diverge_in_scheduling_not_merely_in_label()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync("stranded", Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(StrandedReport()));
        gc.RunOnceAsync("quiet", Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("stranded", "quiet"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        for (var pass = 0; pass < 10; pass++)
        {
            await TickAsync(time);
        }

        // The property the issue is actually about, asserted the way #3119's
        // sibling test asserts its own: both trees reclaimed nothing on every
        // pass, so under the old rule they relaxed in lockstep and were swept an
        // identical number of times. The distinction has to be observable in
        // what the scheduler *does*, not only in the label it writes - a new arm
        // that changed no cadence would satisfy a label assertion and leave the
        // starvation exactly where it was.
        var strandedSweeps = gc.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync)
                && (string)c.GetArguments()[0]! == "stranded");
        var quietSweeps = gc.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync)
                && (string)c.GetArguments()[0]! == "quiet");

        Assert.That(strandedSweeps, Is.GreaterThan(quietSweeps),
            "the tree holding WAL must be swept more often than the tree holding none, not identically.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_stranded_pass_that_reclaimed_keeps_the_cadence_floor()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(StrandedReport(entriesTrimmed: 900)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("draining"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Almost every productive pass leaves a backlog - the scan trims what it
        // may and stops at the frontier - so the cap must never be allowed to
        // *lengthen* the interval of a tree that is reclaiming. It can only ever
        // shorten a ladder that was already relaxing.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        for (var pass = 0; pass < 4; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "a reclaiming tree holds the floor whether or not it left a backlog behind.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_stranded_blocked_tree_still_holds_the_floor()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(
                StrandedReport(cursorFloorState: WalGcCursorFloorState.BlockedByUnusablePin)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("blocked"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Unchanged-behaviour guard. A blocked tree is stranded by definition,
        // so the cap must not be read as a weakening of the #2702 floor: the
        // stronger clause still wins and the tree stays at WalGcMinInterval.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        for (var pass = 0; pass < 4; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "the blocked floor outranks the stranded cap and is unchanged by it.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_stranded_tree_over_its_byte_ceiling_still_holds_the_floor()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(
                entriesTrimmed: 0,
                retainedBytesAfter: 4_096,
                byteCeiling: 1_024,
                bytePressureOverThreshold: true,
                retainedBacklog: true)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("bloated"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The other unchanged-behaviour guard. A breaching tree is also
        // stranded, and the operator who set a ceiling asked for the stronger
        // remedy; the cap must not quietly downgrade the #3119 floor to a
        // five-minute backoff on exactly the trees the ceiling exists to bound.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        for (var pass = 0; pass < 4; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "the over-ceiling floor outranks the stranded cap and is unchanged by it.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    // ---------------------------------------------------------------- metering

    [Test]
    public async Task ExecuteAsync_publishes_a_stranded_outcome_distinct_from_idle()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(StrandedReport()));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "stranded-outcome-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("stranded-outcome-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        // The reading half of the fix. `idle` is contracted to mean the
        // genuinely healthy quiet case, and it was the arm a starving tree
        // landed in on every deployment that set no ceiling - so an operator
        // reading the pass panel saw health on the one tree that had none.
        Assert.That(outcomes, Does.Contain("stranded"),
            "a pass that reclaimed nothing and left WAL behind must be labelled as such.");
        Assert.That(outcomes, Does.Not.Contain("idle"),
            "and must not be counted as idle, which the contract reserves for the genuinely healthy case.");
    }

    [Test]
    public async Task ExecuteAsync_publishes_idle_for_a_pass_that_left_no_backlog()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0, retainedBacklog: false)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "quiet-outcome-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("quiet-outcome-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        // The distinction asserted from the other side. Splitting an arm is only
        // worth anything if the residue still means what it always did: `idle`
        // has to keep naming the quiet tree, or the fix has merely renamed the
        // problem.
        Assert.That(outcomes, Does.Contain("idle"));
        Assert.That(outcomes, Does.Not.Contain("stranded"),
            "a tree with nothing outstanding is quiet, not stranded.");
    }

    [Test]
    public async Task ExecuteAsync_a_stranded_pass_that_reclaimed_is_still_reported_as_reclaimed()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(StrandedReport(entriesTrimmed: 12)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "reclaiming-stranded-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("reclaiming-stranded-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        // Arm precedence, and the reason `stranded` is guarded on having
        // reclaimed nothing. A productive pass almost always stops at a frontier
        // and so almost always leaves a backlog; labelling those `stranded`
        // would turn the affirmative arm off on the healthiest trees on the silo
        // and make the new arm the modal outcome, which would tell a reader
        // nothing at all.
        Assert.That(outcomes, Does.Contain("reclaimed"));
        Assert.That(outcomes, Does.Not.Contain("stranded"),
            "stranded names a pass that could not reclaim, not one that reclaimed and stopped at the frontier.");
    }

    [Test]
    public async Task ExecuteAsync_a_stranded_blocked_tree_is_still_reported_as_blocked()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(
                StrandedReport(cursorFloorState: WalGcCursorFloorState.BlockedByUnusablePin)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "blocked-stranded-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("blocked-stranded-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        // The new arm refines `idle` and nothing else, on the same reasoning
        // #3119 recorded: `blocked` already names a cause and does not claim
        // health, and it is the arm the blocked-leaf remedy reads. Relabelling
        // it would trade an actionable diagnosis for a general symptom.
        Assert.That(outcomes, Does.Contain("blocked"));
        Assert.That(outcomes, Does.Not.Contain("stranded"),
            "a cause-naming arm must not be downgraded to the symptom.");
    }

    [Test]
    public async Task ExecuteAsync_a_stranded_breaching_tree_is_still_reported_as_over_ceiling()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(
                entriesTrimmed: 0,
                retainedBytesAfter: 4_096,
                byteCeiling: 1_024,
                bytePressureOverThreshold: true,
                retainedBacklog: true)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "breaching-stranded-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("breaching-stranded-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        // Mutual exclusivity, which is a documented property and not an
        // incidental one: both doc rows state that a pass records a single arm
        // and that the sum across arms is the pass count rather than an
        // over-count. A breaching tree is necessarily also stranded, so if the
        // new arm co-occurred with `over_ceiling` those claims would become
        // false while still reading as true. `over_ceiling` is the more specific
        // diagnosis and outranks it.
        Assert.That(outcomes, Does.Contain("over_ceiling"));
        Assert.That(outcomes, Does.Not.Contain("stranded"),
            "the arms must stay mutually exclusive: the more specific diagnosis wins.");
        Assert.That(outcomes.Count(o => o is "over_ceiling" or "stranded" or "idle"), Is.EqualTo(1),
            "exactly one of the unreclaimed-available arms may be recorded for a pass.");
    }

    [Test]
    public async Task ExecuteAsync_primes_the_stranded_outcome_at_zero_for_a_tree_that_never_strands()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 7)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "never-stranded-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("never-stranded-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var stranded = recorder.Measurements
            .Where(m => string.Equals(m.Tag(LatticeMetrics.TagOutcome) as string, "stranded", StringComparison.Ordinal))
            .ToArray();

        // A Counter exports no series until its first Add, and this arm is
        // reachable on every deployment rather than only on one that configured
        // a ceiling - so an absent series would be consistent with "no tree has
        // ever stranded", with "this silo is not reporting", and with "the build
        // predates the arm" at once. The first of those is the reading the whole
        // defect made unavailable, so it is the one that has to be trustworthy.
        Assert.That(stranded, Is.Not.Empty,
            "the stranded series must exist for every collected tree, so 'never stranded' is distinguishable from 'not reporting'.");
        Assert.That(stranded.Sum(m => m.Value), Is.Zero,
            "and priming must not fabricate a stranded pass: the primed value is zero.");
    }
}

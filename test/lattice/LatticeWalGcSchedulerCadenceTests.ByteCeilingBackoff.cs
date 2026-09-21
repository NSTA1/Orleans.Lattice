using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Scheduling tests for the byte-ceiling backoff (issue #3119).
/// <para>
/// <c>WalMaxRetainedBytes</c> is the operator's bound on a tree's WAL footprint,
/// and the byte-pressure policy is deliberately subordinate to the safe trim
/// frontier: it may collect harder, but it may never trim an entry a consumer
/// still needs. So when that frontier is pinned, a breaching tree reclaims
/// nothing however hard it is asked - and <b>pass frequency is the only lever
/// the scheduler has left</b>. The adaptive cadence took that lever away. It read
/// whether a pass <i>trimmed</i>, never whether the tree was <i>breaching</i>, so
/// a tree parked over its ceiling reported a usable floor, trimmed nothing,
/// classified as <c>idle</c>, and relaxed geometrically toward the ceiling
/// interval. The policy was starved of passes on exactly the trees it exists to
/// bound, and the starvation was self-reinforcing in the same shape as issue
/// #2702: being unable to reclaim was itself the evidence used to look less
/// often.
/// </para>
/// <para>
/// These tests assert <b>scheduling behaviour</b>, not the existence of a label.
/// A seventh outcome arm that changed nothing about when a pass runs would pass
/// a label assertion and leave the defect in place, so the bar here is that a
/// breaching tree and a quiescent tree are distinguishable in what the scheduler
/// actually does. The two metering tests are the secondary arm of the fix: the
/// <c>WalGcPasses</c> contract states that <c>idle</c> is the genuinely quiet,
/// healthy case and <i>only</i> that, which was false for every breaching tree.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// A pass that confirmed a breach and reclaimed nothing: the exact report a
    /// tree parked over its ceiling behind a pinned frontier produces.
    /// </summary>
    private static LatticeWalGcReport OverCeilingReport(
        long entriesTrimmed = 0,
        WalGcCursorFloorState cursorFloorState = WalGcCursorFloorState.Available) =>
        Report(
            entriesTrimmed,
            retainedBytesAfter: 4_096,
            byteCeiling: 1_024,
            cursorFloorState: cursorFloorState,
            bytePressureOverThreshold: true);

    // ------------------------------------------------------------- scheduling

    [Test]
    public async Task ExecuteAsync_a_tree_over_its_byte_ceiling_holds_the_floor_instead_of_relaxing()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("bloated"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Every one of these passes confirms the breach and reclaims nothing.
        // Under the old predicate that is indistinguishable from a quiet tree,
        // so the interval would have doubled on each: 60s, 2m, 4m, 8m, 16m, 32m,
        // then the ceiling - which is what the live container showed, collecting
        // a breaching tree 15 times against 7,449 opportunities.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        for (var pass = 0; pass < 8; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "a tree over its configured byte ceiling must not be backed off as though it were healthy.");
        }

        await gc.Received(9).RunOnceAsync("bloated", Arg.Any<CancellationToken>());
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_tree_inside_its_byte_ceiling_still_relaxes()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(
                entriesTrimmed: 0,
                retainedBytesAfter: 512,
                byteCeiling: 1_024,
                bytePressureOverThreshold: false)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("comfortable"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The regression guard on the other side of the fix. A configured
        // ceiling must not by itself pin the cadence: the overwhelming majority
        // of trees on a silo with a ceiling set are comfortably inside it, and
        // holding all of them at the floor would neuter the backoff outright and
        // pay the byte probe on every one of them forever.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(1)));

        TimeSpan[] expected =
        [
            TimeSpan.FromMinutes(2),
            TimeSpan.FromMinutes(4),
            TimeSpan.FromMinutes(8),
            TimeSpan.FromMinutes(16),
        ];
        foreach (var interval in expected)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(interval),
                "a tree inside its ceiling with nothing to do must still relax toward the ceiling interval.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_tree_with_no_byte_ceiling_configured_is_untouched_by_the_fix()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("no-ceiling"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The fail-safe property, asserted rather than assumed. The byte verdict
        // is produced only when a ceiling is configured *and* the provider can
        // account for bytes; in every other case it is false, so a deployment
        // that never opted into the policy keeps its cadence exactly. Without
        // this, the fix would be a silent, unrequested load increase on every
        // silo that has no ceiling at all.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(1)));
        await TickAsync(time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(2)),
            "with no ceiling configured the cadence must be the pre-fix cadence, unchanged.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_breaching_tree_and_a_quiescent_tree_diverge_in_scheduling_not_merely_in_label()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync("bloated", Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));
        gc.RunOnceAsync("quiet", Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("bloated", "quiet"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        for (var pass = 0; pass < 6; pass++)
        {
            await TickAsync(time);
        }

        // Both trees reclaimed nothing on every pass, so under the old rule they
        // relaxed in lockstep and were collected an identical number of times.
        // This is the property the issue is actually about, and it is the one a
        // label-only change would not deliver: the two states have to be
        // separable by observing the scheduler, without reading a label, reading
        // source, or comparing byte history.
        var bloated = gc.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync)
                && (string)c.GetArguments()[0]! == "bloated");
        var quiet = gc.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync)
                && (string)c.GetArguments()[0]! == "quiet");

        Assert.That(bloated, Is.GreaterThan(quiet * 2),
            "a breaching tree must be collected far more often than a quiescent one, not identically.");
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
            "the breaching tree holds the silo's wake cadence at the floor while its quiet sibling relaxes.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_holds_the_floor_for_a_breaching_tree_that_never_reported_a_cursor()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(
                OverCeilingReport(cursorFloorState: WalGcCursorFloorState.NoCursorReported)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("orphaned"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Arm independence, and the reason the breach clause is not folded into
        // the blocked one. A breaching tree whose consumers never reported a
        // cursor reports NoCursorReported, not BlockedByUnusablePin, so a fix
        // conditioned on the blocked cause would hold the floor for one
        // breaching tree and relax the other for the identical reason. The
        // breach is an independent axis and has to be read as one.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        for (var pass = 0; pass < 5; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "the byte breach must hold the floor regardless of which cursor-floor state accompanies it.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_breaching_tree_relaxes_again_once_it_drops_under_its_ceiling()
    {
        var breaching = true;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(breaching ? OverCeilingReport() : Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("draining"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        await TickAsync(time);
        await TickAsync(time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));

        // Whatever unpinned the frontier lands and the footprint falls back
        // under the ceiling. The floor must be a consequence of the breach, not
        // a latch: a recovered tree with nothing to reclaim is an ordinary quiet
        // tree and has to relax like one, otherwise the fix silently converts
        // every tree that ever breached into a permanent floor-rate poller that
        // pays the byte probe forever.
        breaching = false;

        TimeSpan[] expected = [TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(4)];
        foreach (var interval in expected)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(interval),
                "the floor must be held only while the tree is actually over its ceiling.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_drainable_breach_is_reclaimed_within_the_floor_not_the_ceiling()
    {
        var pinned = true;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(pinned
                ? OverCeilingReport()
                : Report(entriesTrimmed: 4_000, retainedBytesAfter: 512, byteCeiling: 1_024)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("bloated"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Sit over the ceiling long enough that the old geometric backoff would
        // have run out to the ceiling interval: 60s, 2m, 4m, 8m, 16m, 32m, 1h.
        for (var pass = 0; pass < 7; pass++)
        {
            await TickAsync(time);
        }

        // The frontier advances. The excess bytes do not come back until a GC
        // pass runs and trims them, so this interval is the time-to-reclaim even
        // when something else supplies the time-to-unpin. That is what makes the
        // cadence part of the ceiling's guarantee rather than a presentation
        // concern: at the ceiling interval the operator's bound would have been
        // breached for a further hour after it became enforceable.
        pinned = false;
        var beforeDrainPass = time.GetUtcNow();
        await TickAsync(time);

        var elapsed = time.GetUtcNow() - beforeDrainPass;
        Assert.That(elapsed, Is.LessThanOrEqualTo(Floor),
            "a drainable tree must be re-collected within the floor; at the ceiling this pass would have been an hour later.");
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
            "and the reclaiming tree stays at the floor while it drains.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    // ---------------------------------------------------------------- metering

    [Test]
    public async Task ExecuteAsync_publishes_an_over_ceiling_outcome_distinct_from_idle()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "over-ceiling-outcome-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("over-ceiling-outcome-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        Assert.That(outcomes, Does.Contain("over_ceiling"),
            "a pass that confirmed a breach and reclaimed nothing must be labelled as such.");
        Assert.That(outcomes, Does.Not.Contain("idle"),
            "and must not be counted as idle, which the contract reserves for the genuinely healthy case.");
    }

    [Test]
    public async Task ExecuteAsync_a_breaching_pass_that_reclaimed_is_still_reported_as_reclaimed()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport(entriesTrimmed: 12)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "draining-breach-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("draining-breach-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        // Arm precedence. A tree can be draining hard and still be over its
        // ceiling on the pass that measured it, and that pass reclaimed - the
        // affirmative arm the epic's criterion reads must keep counting it,
        // otherwise the new arm would quietly eat the reclamation signal on
        // exactly the trees under the most pressure.
        Assert.That(outcomes, Does.Contain("reclaimed"));
        Assert.That(outcomes, Does.Not.Contain("over_ceiling"),
            "over_ceiling names a pass that could not reclaim, not merely a tree that is large.");
    }

    [Test]
    public async Task ExecuteAsync_a_breaching_blocked_tree_is_still_reported_as_blocked()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(
                OverCeilingReport(cursorFloorState: WalGcCursorFloorState.BlockedByUnusablePin)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "blocked-breach-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("blocked-breach-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        // The new arm refines `idle` and nothing else. `blocked` already names a
        // cause and does not claim health, and it is the arm the blocked-leaf
        // remedy reads; relabelling it on a breach would trade a specific,
        // actionable diagnosis for a general symptom. The cadence still holds
        // the floor either way, so nothing is lost by keeping the better label.
        Assert.That(outcomes, Does.Contain("blocked"));
        Assert.That(outcomes, Does.Not.Contain("over_ceiling"),
            "a cause-naming arm must not be downgraded to the symptom.");
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
    }

    [Test]
    public async Task ExecuteAsync_primes_the_over_ceiling_outcome_at_zero_for_a_tree_that_never_breaches()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 7)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "never-breaching-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("never-breaching-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var overCeiling = recorder.Measurements
            .Where(m => string.Equals(m.Tag(LatticeMetrics.TagOutcome) as string, "over_ceiling", StringComparison.Ordinal))
            .ToArray();

        // A Counter exports no series until its first Add, so an unprimed
        // over_ceiling counter would be absent on a healthy silo - conflating
        // "never breached", "no ceiling configured" and "not wired", which are
        // three very different answers to the operator's question. Worse, it
        // would *vanish* from a silo whose trees had breached and then drained,
        // at exactly the moment a reader needs to confirm the recovery held.
        Assert.That(overCeiling, Is.Not.Empty,
            "the over_ceiling series must exist for every collected tree, so 'inside the ceiling' is distinguishable from 'not reporting'.");
        Assert.That(overCeiling.Sum(m => m.Value), Is.Zero,
            "and priming must not fabricate a breaching pass: the primed value is zero.");
    }

    [Test]
    public async Task ExecuteAsync_primes_the_over_ceiling_outcome_per_tree_so_one_bloated_tree_is_not_averaged_away()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 1)));
        var time = new VirtualTimeProvider();

        using var first = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "ceiling-prime-a");
        using var second = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "ceiling-prime-b");
        var scheduler = CreateScheduler(FactoryWithTrees("ceiling-prime-a", "ceiling-prime-b"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        foreach (var recorder in new[] { first, second })
        {
            Assert.That(
                recorder.Measurements.Any(m =>
                    string.Equals(m.Tag(LatticeMetrics.TagOutcome) as string, "over_ceiling", StringComparison.Ordinal)),
                Is.True,
                "each tree carries its own over_ceiling series, so a single bloated tree stays visible.");
        }
    }

    // ------------------------------------------------------------- end to end

    [Test]
    public async Task A_tree_over_its_configured_ceiling_reports_over_ceiling_against_a_real_collector()
    {
        // Every test above drives the scheduler from a report a substitute
        // returned, which proves the scheduler reads the flag but not that
        // production ever sets it. This one produces the breach the way the live
        // container did: a real collector, a real WAL holding entries, a
        // consumer whose cursor sits below all of them so the safe frontier
        // trims nothing, and a ceiling of one byte that the footprint cannot
        // help but exceed.
        const string Tree = "walgc-outcome-over-ceiling";
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0,
            new[] { OutcomeEntry(Tree, 0, OutcomeHlc(100)), OutcomeEntry(Tree, 1, OutcomeHlc(200)) },
            CancellationToken.None);

        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", OutcomeHlc(10));

        var time = new VirtualTimeProvider();
        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Tree);
        var scheduler = CreateScheduler(
            FactoryWithTrees(Tree),
            RealGcWithCeiling(provider, registry, maxRetainedBytes: 1),
            Adaptive(),
            time);
        await StartAndRunFirstPassAsync(scheduler, time);

        var advanced = passes.Counted;
        Assert.That(advanced, Has.Count.EqualTo(1),
            "a completed pass must advance exactly one outcome arm.");
        Assert.That(advanced[0].Tag(LatticeMetrics.TagOutcome) as string, Is.EqualTo("over_ceiling"),
            "a real collector over a real breaching WAL must produce the breach arm, not idle.");
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
            "and the cadence must hold the floor on the strength of production's own verdict.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// A real collector over an in-memory WAL with the byte-pressure policy
    /// armed at <paramref name="maxRetainedBytes"/>.
    /// </summary>
    private static ILatticeWalGc RealGcWithCeiling(
        IWalStorageProvider provider,
        IWalCursorRegistry registry,
        long maxRetainedBytes)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalMaxRetainedBytes = maxRetainedBytes,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        return new LatticeWalGc(sc.BuildServiceProvider(), registry, monitor);
    }
}

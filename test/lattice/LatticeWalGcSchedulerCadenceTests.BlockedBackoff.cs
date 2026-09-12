using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Scheduling tests for the blocked-tree backoff (issue #2702).
/// <para>
/// A WAL GC pass that reclaimed nothing did so for one of two opposite reasons.
/// Either the tree was quiet, which is the healthy steady state and should be
/// collected less often; or the consumer-cursor branch was disabled outright by
/// an unusable durable materialiser pin, in which case the tree cannot reclaim
/// at all and its WAL grows without bound. Both produced
/// <c>EntriesTrimmed == 0</c>, and that single boolean drove <i>both</i> the
/// <c>outcome</c> label and the exponential backoff - so a starved tree was
/// scheduled least often precisely when it needed the most attention, and the
/// backoff was self-reinforcing: being unable to reclaim was itself the evidence
/// used to decide to look less often.
/// </para>
/// <para>
/// These tests assert <b>scheduling behaviour</b>, not the existence of a label.
/// A fourth enum value that changed nothing about when a pass runs would pass a
/// label assertion and leave the defect in place, so the bar here is that a
/// blocked tree and a quiescent tree are distinguishable in what the scheduler
/// actually does.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    // ------------------------------------------------------------- scheduling

    [Test]
    public async Task ExecuteAsync_a_blocked_tree_holds_the_floor_instead_of_relaxing()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReport()));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("stranded"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Every one of these passes reclaims nothing. Under the old predicate
        // that is indistinguishable from a quiet tree, so the interval would
        // have doubled on each: 60s, 2m, 4m, 8m, 16m, 32m, then the ceiling.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));
        for (var pass = 0; pass < 8; pass++)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "a tree that cannot reclaim must not be backed off as though it had nothing to reclaim.");
        }

        await gc.Received(9).RunOnceAsync("stranded", Arg.Any<CancellationToken>());
        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_quiescent_tree_still_relaxes_after_the_blocked_state_exists()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("quiet"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // The first pass already finds nothing, so the tree has relaxed off the
        // floor before the loop below starts.
        Assert.That(time.LastScheduledDelay, Is.EqualTo(TimeSpan.FromMinutes(1)));

        // The regression guard on the other side of the fix: holding blocked
        // trees at the floor must not neuter the backoff for genuinely quiet
        // ones, which are the overwhelming majority and the reason the backoff
        // exists at all.
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
                "an unblocked tree with nothing to do must still relax toward the ceiling.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_blocked_tree_and_a_quiescent_tree_diverge_in_scheduling_not_merely_in_label()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync("stranded", Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReport()));
        gc.RunOnceAsync("quiet", Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("stranded", "quiet"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        for (var pass = 0; pass < 6; pass++)
        {
            await TickAsync(time);
        }

        // Both trees reclaimed nothing on every pass, so under the old rule they
        // would have relaxed in lockstep and been collected an identical number
        // of times. This is the property the issue is actually about: the two
        // states have to be separable by observing the scheduler, without
        // reading a label, reading source, or comparing byte history.
        var stranded = gc.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync)
                && (string)c.GetArguments()[0]! == "stranded");
        var quiet = gc.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ILatticeWalGc.RunOnceAsync)
                && (string)c.GetArguments()[0]! == "quiet");

        Assert.That(stranded, Is.GreaterThan(quiet * 2),
            "a blocked tree must be collected far more often than a quiescent one, not identically.");
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
            "the blocked tree holds the silo's wake cadence at the floor while its quiet sibling relaxes.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_blocked_tree_relaxes_again_once_it_is_no_longer_blocked()
    {
        var blocked = true;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(blocked ? BlockedReport() : Report(entriesTrimmed: 0)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("healing"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        await TickAsync(time);
        await TickAsync(time);
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor));

        // Whatever repaired the pin lands. The floor must be a consequence of
        // being blocked, not a latch: a healed tree with nothing to reclaim is
        // an ordinary quiet tree and has to relax like one, otherwise the fix
        // silently converts every tree that was ever blocked into a permanent
        // floor-rate poller.
        blocked = false;

        TimeSpan[] expected = [TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(4)];
        foreach (var interval in expected)
        {
            await TickAsync(time);
            Assert.That(time.LastScheduledDelay, Is.EqualTo(interval),
                "the floor must be held only while the tree is actually blocked.");
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_a_repair_on_a_blocked_tree_is_reclaimed_within_the_floor_not_the_ceiling()
    {
        var blocked = true;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(blocked ? BlockedReport() : Report(entriesTrimmed: 4_000)));
        var time = new VirtualTimeProvider();

        var scheduler = CreateScheduler(FactoryWithTrees("stranded"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Sit blocked long enough that the old geometric backoff would have run
        // out to the ceiling: 60s, 2m, 4m, 8m, 16m, 32m, 1h.
        for (var pass = 0; pass < 7; pass++)
        {
            await TickAsync(time);
        }

        // A repair unblocks the pin. The stranded bytes do not come back until a
        // GC pass runs and trims them, so this interval is the time-to-reclaim
        // even when something else supplies the time-to-unblock. That is what
        // makes the backoff part of a self-healing guarantee rather than a
        // presentation concern.
        blocked = false;
        var beforeRepairPass = time.GetUtcNow();
        await TickAsync(time);

        var elapsed = time.GetUtcNow() - beforeRepairPass;
        Assert.That(elapsed, Is.LessThanOrEqualTo(Floor),
            "a repaired tree must be re-collected within the floor; at the ceiling this pass would have been an hour later.");
        Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
            "and the reclaiming tree stays at the floor while it drains.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    // ---------------------------------------------------------------- metering

    [Test]
    public async Task ExecuteAsync_publishes_a_blocked_outcome_distinct_from_idle()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReport()));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "blocked-outcome-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("blocked-outcome-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var outcomes = recorder.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .ToArray();

        Assert.That(outcomes, Does.Contain("blocked"),
            "a pass blocked by an unusable pin must be labelled as such.");
        Assert.That(outcomes, Does.Not.Contain("idle"),
            "and must not also be counted as idle, which is the conflation the issue is about.");
    }

    [Test]
    public async Task ExecuteAsync_primes_the_blocked_outcome_at_zero_for_a_tree_that_never_blocks()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 7)));
        var time = new VirtualTimeProvider();

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "never-blocked-tree");
        var scheduler = CreateScheduler(FactoryWithTrees("never-blocked-tree"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var blocked = recorder.Measurements
            .Where(m => string.Equals(m.Tag(LatticeMetrics.TagOutcome) as string, "blocked", StringComparison.Ordinal))
            .ToArray();

        // A Counter exports no series until its first Add, so an unprimed
        // blocked counter would be absent on a healthy silo - and, worse, would
        // *vanish* from a silo whose trees had been blocked and were then
        // repaired, at exactly the moment a reader needs to confirm the repair
        // held. Priming makes the zero a measurement rather than an absence.
        Assert.That(blocked, Is.Not.Empty,
            "the blocked series must exist for every collected tree, so 'healthy' is distinguishable from 'not reporting'.");
        Assert.That(blocked.Sum(m => m.Value), Is.Zero,
            "and priming must not fabricate a blocked pass: the primed value is zero.");
    }

    [Test]
    public async Task ExecuteAsync_primes_the_blocked_outcome_per_tree_so_one_stranded_tree_is_not_averaged_away()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(entriesTrimmed: 1)));
        var time = new VirtualTimeProvider();

        using var first = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "prime-tree-a");
        using var second = new InstrumentRecorder(LatticeMetrics.WalGcPasses, "prime-tree-b");
        var scheduler = CreateScheduler(FactoryWithTrees("prime-tree-a", "prime-tree-b"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        foreach (var recorder in new[] { first, second })
        {
            Assert.That(
                recorder.Measurements.Any(m =>
                    string.Equals(m.Tag(LatticeMetrics.TagOutcome) as string, "blocked", StringComparison.Ordinal)),
                Is.True,
                "each tree carries its own blocked series, so a single re-stranded tree stays visible.");
        }
    }

    // -------------------------------------------------------------- the report

    [Test]
    public void LatticeWalGcReport_defaults_the_cursor_floor_state_to_available()
    {
        var report = new LatticeWalGcReport("tree", null, null, null, null, 1, 0);

        // The benign default matters for compatibility: every existing
        // construction site keeps its previous scheduling behaviour, and the
        // fix can only ever make a tree poll more often than before, never less.
        Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.Available));
    }

    [Test]
    public void WalGcCursorFloorState_separates_no_cursor_from_blocked()
    {
        // The whole point of the enum: a null MinCursor has two causes and they
        // demand opposite responses, so they must not share a value.
        Assert.That(WalGcCursorFloorState.NoCursorReported,
            Is.Not.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
        Assert.That(Enum.GetValues<WalGcCursorFloorState>(), Has.Length.EqualTo(3));
    }
}

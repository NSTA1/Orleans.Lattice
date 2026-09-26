using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Emission, reset and priming tests for
/// <see cref="LatticeMetrics.WalGcTerminalBreach"/> (issue #3149).
/// <para>
/// A tree over its byte ceiling with a usable floor that reclaims nothing lands
/// on <c>over_ceiling</c>, and that arm is by design a transient: the scheduler
/// holds the tree at the floor interval and asks again. When asking again stops
/// helping, nothing said so - the arm read the same on the tenth fruitless pass
/// as on the first. These tests pin that the terminal signal fires only once a
/// run of <see cref="LatticeWalGcScheduler.TerminalBreachPasses"/> such passes
/// has accumulated, that any pass which breaks the condition resets the run,
/// and that a blocked floor - which <c>blocked</c> already names - never counts.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private static ILatticeWalGc GcReportingCurrent(Func<LatticeWalGcReport> current)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(current()));
        return gc;
    }

    /// <summary>Runs <paramref name="passes"/> passes (the first included) of whatever <paramref name="current"/> reports.</summary>
    private static async Task RunPassesAsync(LatticeWalGcScheduler scheduler, VirtualTimeProvider time, int passes, bool started)
    {
        var remaining = passes;
        if (!started)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            remaining--;
        }

        for (var i = 0; i < remaining; i++)
        {
            await TickAsync(time);
        }
    }

    // ------------------------------------------------------------- threshold

    [Test]
    public async Task Terminal_breach_fires_only_once_the_run_reaches_the_threshold_and_then_on_every_pass()
    {
        const string Tree = "walgc-terminal-breach-threshold";
        var time = new VirtualTimeProvider();
        using var signal = new InstrumentRecorder(LatticeMetrics.WalGcTerminalBreach, Tree);
        var scheduler = CreateScheduler(
            FactoryWithTrees(Tree), GcReporting(OverCeilingReport()), Adaptive(), time);

        await RunPassesAsync(scheduler, time, LatticeWalGcScheduler.TerminalBreachPasses - 1, started: false);
        var beforeThreshold = signal.Counted.Count;

        await TickAsync(time);
        var atThreshold = signal.Counted.Count;

        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(beforeThreshold, Is.Zero,
                "a run shorter than the threshold is the over_ceiling transient the scheduler is still "
                + "retrying, and must not be called terminal.");
            Assert.That(atThreshold, Is.EqualTo(1),
                "the pass that completes the run is the first terminal one.");
            Assert.That(signal.Counted, Has.Count.EqualTo(2),
                "and every further breaching pass is stated, so the rate is readable against wal.gc.passes.");
            Assert.That(signal.Counted.Select(m => m.Value), Is.All.EqualTo(1));
            Assert.That(signal.Counted.Select(m => m.Tag(LatticeMetrics.TagTree)), Is.All.EqualTo(Tree));
        });
    }

    // ----------------------------------------------------------------- reset

    [TestCase(true, TestName = "Terminal_breach_run_resets_on_a_pass_that_reclaimed_something")]
    [TestCase(false, TestName = "Terminal_breach_run_resets_on_a_pass_under_the_ceiling")]
    public async Task Terminal_breach_run_resets_on_a_pass_that_breaks_the_condition(bool reclaiming)
    {
        var tree = reclaiming ? "walgc-terminal-breach-reset-reclaim" : "walgc-terminal-breach-reset-under";
        var time = new VirtualTimeProvider();
        using var signal = new InstrumentRecorder(LatticeMetrics.WalGcTerminalBreach, tree);
        var breaking = reclaiming
            ? OverCeilingReport(entriesTrimmed: 5)
            : Report(entriesTrimmed: 0, retainedBytesAfter: 512, byteCeiling: 1_024);
        var current = OverCeilingReport();
        var scheduler = CreateScheduler(FactoryWithTrees(tree), GcReportingCurrent(() => current), Adaptive(), time);

        await RunPassesAsync(scheduler, time, LatticeWalGcScheduler.TerminalBreachPasses - 1, started: false);
        current = breaking;
        await TickAsync(time);
        current = OverCeilingReport();
        await RunPassesAsync(scheduler, time, LatticeWalGcScheduler.TerminalBreachPasses - 1, started: true);
        var afterRestartedRun = signal.Counted.Count;

        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(afterRestartedRun, Is.Zero,
                "two sub-threshold runs separated by a pass that broke the condition are two transients, "
                + "not one terminal breach - the run must restart, not merely pause.");
            Assert.That(signal.Counted, Has.Count.EqualTo(1),
                "a fresh run that reaches the threshold is terminal again.");
        });
    }

    // ------------------------------------------------------------ exclusions

    [Test]
    public async Task Terminal_breach_never_fires_for_a_tree_whose_floor_is_blocked()
    {
        const string Tree = "walgc-terminal-breach-blocked";
        var time = new VirtualTimeProvider();
        using var signal = new InstrumentRecorder(LatticeMetrics.WalGcTerminalBreach, Tree);
        var scheduler = CreateScheduler(
            FactoryWithTrees(Tree),
            GcReporting(OverCeilingReport(cursorFloorState: WalGcCursorFloorState.BlockedByUnusablePin)),
            Adaptive(),
            time);

        await RunPassesAsync(scheduler, time, LatticeWalGcScheduler.TerminalBreachPasses * 2, started: false);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(signal.Counted, Is.Empty,
            "the signal is defined over an AVAILABLE floor: a blocked tree is already named by the "
            + "blocked arm, whose remedy differs, and must not be double-reported as terminal.");
    }

    // --------------------------------------------------------------- priming

    [Test]
    public async Task Terminal_breach_is_primed_at_zero_with_the_same_tag_set_as_a_real_emission()
    {
        const string Tree = "walgc-terminal-breach-primed";
        var time = new VirtualTimeProvider();
        using var signal = new InstrumentRecorder(LatticeMetrics.WalGcTerminalBreach, Tree);
        var scheduler = CreateScheduler(
            FactoryWithTrees(Tree), GcReporting(OverCeilingReport()), Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        var primedOnly = signal.Measurements.ToArray();

        await RunPassesAsync(scheduler, time, LatticeWalGcScheduler.TerminalBreachPasses - 1, started: true);
        await scheduler.StopAsync(CancellationToken.None);

        var real = signal.Counted.Single();
        var primed = signal.Measurements.Single(m => m.Value == 0);

        Assert.Multiple(() =>
        {
            Assert.That(primedOnly, Has.Length.EqualTo(1),
                "the series must exist from the first pass, so 'never terminal' is a reading rather than "
                + "an absence.");
            Assert.That(primedOnly[0].Value, Is.Zero, "and the prime must not fabricate a finding.");
            Assert.That(primed.Tags.Select(t => t.Key), Is.EquivalentTo(real.Tags.Select(t => t.Key)),
                "a prime with a different tag set mints a decoy series while the queried one stays absent.");
            Assert.That(
                primed.Tag(LatticeTenantLabel.TagTenant),
                Is.EqualTo(real.Tag(LatticeTenantLabel.TagTenant)));
        });
    }
}

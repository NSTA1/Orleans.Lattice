using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Priming and emission-shape tests for the WAL GC pass counter (issue #2774).
/// <para>
/// A Counter publishes no series until its first <c>Add</c>, so an outcome arm
/// that has never fired is absent rather than zero. On the <c>reclaimed</c> arm
/// that absence is not merely untidy: it backs a release acceptance criterion,
/// and unprimed it makes "reclamation never happened" and "the instrument is
/// unwired" the same reading - so a system that reclaimed perfectly is
/// indistinguishable from one that never ran. That is a success misread as a
/// failure, which is the most expensive wrong answer a predicate can give.
/// </para>
/// <para>
/// A second, subtler property is being replaced here rather than merely added
/// to. The three non-<c>failed</c> arms are selected by a ternary inside a
/// single <c>Add</c> call, so the presence of <i>any</i> one of them proves the
/// site executed for that tree - which is why a scrape carrying only
/// <c>blocked</c> and <c>idle</c> was still readable as evidence that the
/// instrument was wired. That inference is incidental to how the expression
/// happens to be written today: splitting the ternary into three calls would
/// destroy it silently, with no test failing. These fixtures make the guarantee
/// structural, so a reader never has to know the shape of the emission in order
/// to interpret an absence.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private static readonly string[] EveryOutcome = ["reclaimed", "idle", "blocked", "failed"];

    /// <summary>
    /// Configures the collaborator so a single pass lands on <paramref name="outcome"/>.
    /// </summary>
    private static ILatticeWalGc GcReaching(string outcome)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        switch (outcome)
        {
            case "reclaimed":
                gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                    .Returns(_ => Task.FromResult(Report(entriesTrimmed: 9)));
                break;
            case "idle":
                gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                    .Returns(_ => Task.FromResult(Report(entriesTrimmed: 0)));
                break;
            case "blocked":
                gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                    .Returns(_ => Task.FromResult(BlockedReport()));
                break;
            default:
                gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                    .Returns<Task<LatticeWalGcReport>>(_ => throw new InvalidOperationException("wal wedged"));
                break;
        }

        return gc;
    }

    // ------------------------------------------------- the arms exist at zero

    [TestCase("reclaimed")]
    [TestCase("idle")]
    [TestCase("blocked")]
    [TestCase("failed")]
    public async Task Every_pass_outcome_is_primed_so_an_absent_arm_is_never_a_healthy_reading(string reached)
    {
        var tree = $"walgc-prime-all-{reached}";
        var time = new VirtualTimeProvider();

        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, tree);
        var scheduler = CreateScheduler(FactoryWithTrees(tree), GcReaching(reached), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var arms = passes.Measurements
            .Select(m => m.Tag(LatticeMetrics.TagOutcome) as string)
            .Distinct()
            .ToArray();

        // Whichever single outcome this pass reached, the other three must
        // still be exported at zero. Asserting it on all four cases is what
        // makes the claim "primed unconditionally" rather than "primed on the
        // paths we happened to exercise" - in particular the failed case,
        // whose priming happens before the try and so must survive a throw.
        Assert.That(arms, Is.EquivalentTo(EveryOutcome),
            "every outcome arm must exist for a collected tree, so an absent arm means "
            + "'this silo is not reporting' rather than 'measured, never happened'.");
    }

    [Test]
    public async Task A_tree_that_has_never_reclaimed_still_exports_the_reclaimed_arm_at_zero()
    {
        // The issue's own defect, named directly. This is the arm a release
        // predicate reads, and the one whose absence was being interpreted as
        // a measurement of failure.
        const string Tree = "walgc-never-reclaimed";
        var time = new VirtualTimeProvider();

        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Tree);
        var scheduler = CreateScheduler(FactoryWithTrees(Tree), GcReaching("idle"), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        var reclaimed = passes.Measurements
            .Where(m => string.Equals(m.Tag(LatticeMetrics.TagOutcome) as string, "reclaimed", StringComparison.Ordinal))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.Not.Empty,
                "the reclaimed series must exist before the first reclaiming pass, or a system that "
                + "reclaimed perfectly reads identically to one that never ran.");
            Assert.That(reclaimed.Sum(m => m.Value), Is.Zero,
                "and priming must not fabricate a reclaimed pass: the primed value is zero.");
        });
    }

    // --------------------------------------------- the arms are shaped alike

    [Test]
    public async Task A_primed_outcome_carries_the_same_tag_set_as_a_real_emission()
    {
        // The guard on the shared recorder. A prime whose tags differ from the
        // emission it anticipates is worse than no prime at all: it mints a
        // second series that is permanently zero while the series a reader
        // actually queries stays absent, so the absence survives behind a decoy
        // that looks like the fix landed.
        const string Tree = "walgc-prime-tag-parity";
        var time = new VirtualTimeProvider();

        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, Tree);
        var scheduler = CreateScheduler(FactoryWithTrees(Tree), GcReaching("reclaimed"), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var real = passes.Counted.Single();
        var primed = passes.Measurements.Single(m =>
            m.Value == 0 && string.Equals(m.Tag(LatticeMetrics.TagOutcome) as string, "idle", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(
                primed.Tags.Select(t => t.Key),
                Is.EquivalentTo(real.Tags.Select(t => t.Key)),
                "a primed arm and a real emission must carry the same tag keys, or they are two "
                + "different series and the prime does not anticipate the emission at all.");
            Assert.That(primed.Tag(LatticeMetrics.TagTree), Is.EqualTo(real.Tag(LatticeMetrics.TagTree)));
            Assert.That(
                primed.Tag(LatticeTenantLabel.TagTenant),
                Is.EqualTo(real.Tag(LatticeTenantLabel.TagTenant)),
                "including the derived tenant dimension, which a hand-repeated tag list is most "
                + "likely to drop.");
        });
    }

    // --------------------------------------- exactly one arm advances, always

    [TestCase("reclaimed")]
    [TestCase("idle")]
    [TestCase("blocked")]
    [TestCase("failed")]
    public async Task A_completed_pass_advances_exactly_one_outcome_arm_by_one(string expected)
    {
        // This is the invariant the single shared emission site delivers,
        // expressed behaviourally rather than structurally. Pinning the ternary
        // itself would forbid a correct refactor as readily as an incorrect
        // one, and a guard that fires on correct changes gets deleted; this
        // fires on exactly the harmful subset - a pass that advances no arm
        // (the #2774 defect class recurring) or more than one.
        var tree = $"walgc-exactly-one-{expected}";
        var time = new VirtualTimeProvider();

        using var passes = new InstrumentRecorder(LatticeMetrics.WalGcPasses, tree);
        var scheduler = CreateScheduler(FactoryWithTrees(tree), GcReaching(expected), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var advanced = passes.Counted;

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Has.Count.EqualTo(1),
                "a completed pass must advance exactly one outcome arm - neither none, which is the "
                + "defect that makes an arm unmintable, nor several, which double-counts the pass.");
            Assert.That(advanced[0].Value, Is.EqualTo(1),
                "and it advances that arm by exactly one, once per pass.");
            Assert.That(advanced[0].Tag(LatticeMetrics.TagOutcome), Is.EqualTo(expected),
                "and it must be the arm the pass actually reached.");
        });
    }
}

using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Emission and priming tests for
/// <see cref="LatticeMetrics.WalGcCeilingUnsatisfiable"/> (issue #3242).
/// <para>
/// The instrument exists because a tree whose configured
/// <c>WalMaxRetainedBytes</c> is arithmetically unreachable against its own
/// working set is indistinguishable, on every series this repository exports,
/// from a tree whose consumers are lagging. The two demand opposite responses -
/// raise the ceiling versus unblock the consumer - so the condition can persist
/// indefinitely while presenting as a transient, and an operator can spend the
/// whole diagnosis on the wrong one of the two. That is a property of what the
/// series can express, not of any deployment currently exhibiting it.
/// </para>
/// <para>
/// It is a separate counter rather than an eighth arm of
/// <see cref="LatticeMetrics.WalGcPasses"/>, and the co-occurrence fixture
/// below is what makes that decision falsifiable rather than a matter of
/// taste: the outcome arms partition invocations, and this condition holds
/// simultaneously with <c>reclaimed</c>, <c>over_ceiling</c> and
/// <c>stranded</c> alike. An arm would have had to take those invocations
/// from whichever arm names them today, so an operator alerting on
/// <c>stranded</c> would have watched it fall silent at exactly the moment
/// the condition worsened.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private static ILatticeWalGc GcReporting(LatticeWalGcReport report)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(report));
        return gc;
    }

    private static async Task<InstrumentRecorder> CollectCeilingSignalAsync(
        string tree,
        LatticeWalGcReport report)
    {
        var time = new VirtualTimeProvider();
        var recorder = new InstrumentRecorder(LatticeMetrics.WalGcCeilingUnsatisfiable, tree);
        var scheduler = CreateScheduler(FactoryWithTrees(tree), GcReporting(report), Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);
        return recorder;
    }

    // ------------------------------------------------------------- it fires

    [Test]
    public async Task A_pass_that_found_the_ceiling_unsatisfiable_advances_the_counter_by_one()
    {
        const string Tree = "walgc-ceiling-unsatisfiable-fires";

        using var signal = await CollectCeilingSignalAsync(
            Tree,
            Report(entriesTrimmed: 0, byteCeiling: 4096, logicalRetainedBytes: 2226, ceilingUnsatisfiable: true));

        var advanced = signal.Counted;

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Has.Count.EqualTo(1),
                "an unsatisfiable ceiling must be stated once per pass, so its rate is readable "
                + "against the pass rate.");
            Assert.That(advanced[0].Value, Is.EqualTo(1));
            Assert.That(advanced[0].Tag(LatticeMetrics.TagTree), Is.EqualTo(Tree),
                "and it must name the tree whose ceiling is unreachable - the whole remedy is "
                + "per-tree.");
        });
    }

    // ------------------------------------------------------------ it is quiet

    [Test]
    public async Task A_pass_that_found_the_ceiling_satisfiable_does_not_advance_the_counter()
    {
        const string Tree = "walgc-ceiling-unsatisfiable-quiet";

        using var signal = await CollectCeilingSignalAsync(
            Tree,
            Report(entriesTrimmed: 0, byteCeiling: 8192, logicalRetainedBytes: 2226));

        Assert.That(signal.Counted, Is.Empty,
            "a correctly sized tree must never tick this series, or the signal cannot be alerted on.");
    }

    // ----------------------------------------------------------- it is primed

    [Test]
    public async Task The_series_exists_at_zero_for_a_tree_whose_ceiling_is_satisfiable()
    {
        // The house standard, and the reason this is not left to fire only on
        // the pathological path: a Counter publishes no series until its first
        // Add, so an unprimed instrument makes "this tree is correctly sized",
        // "this silo is not collecting", and "this build predates the signal"
        // one indistinguishable absence - and the first of those three is the
        // reading an operator most needs to be able to trust.
        const string Tree = "walgc-ceiling-unsatisfiable-primed";

        using var signal = await CollectCeilingSignalAsync(
            Tree,
            Report(entriesTrimmed: 4, byteCeiling: 8192, logicalRetainedBytes: 2226));

        Assert.Multiple(() =>
        {
            Assert.That(signal.Measurements, Is.Not.Empty,
                "the series must exist before the first unsatisfiable pass.");
            Assert.That(signal.Measurements.Sum(m => m.Value), Is.Zero,
                "and priming must not fabricate a finding: the primed value is zero.");
        });
    }

    [Test]
    public async Task The_primed_series_carries_the_same_tag_set_as_a_real_emission()
    {
        // A prime whose tags differ from the emission it anticipates mints a
        // second, permanently-zero series while the one a reader queries stays
        // absent - so the absence survives behind a decoy that looks like the
        // fix landed. The derived tenant dimension is the tag a hand-repeated
        // list is most likely to drop.
        const string Tree = "walgc-ceiling-unsatisfiable-tag-parity";

        using var signal = await CollectCeilingSignalAsync(
            Tree,
            Report(entriesTrimmed: 0, byteCeiling: 4096, logicalRetainedBytes: 2226, ceilingUnsatisfiable: true));

        var real = signal.Counted.Single();
        var primed = signal.Measurements.Single(m => m.Value == 0);

        Assert.Multiple(() =>
        {
            Assert.That(
                primed.Tags.Select(t => t.Key),
                Is.EquivalentTo(real.Tags.Select(t => t.Key)),
                "a primed series and a real emission must carry the same tag keys, or the prime "
                + "does not anticipate the emission at all.");
            Assert.That(primed.Tag(LatticeMetrics.TagTree), Is.EqualTo(real.Tag(LatticeMetrics.TagTree)));
            Assert.That(
                primed.Tag(LatticeTenantLabel.TagTenant),
                Is.EqualTo(real.Tag(LatticeTenantLabel.TagTenant)));
        });
    }

    // --------------------------------- it is independent of the outcome arms

    [TestCase(9, WalGcCursorFloorState.Available, false, "reclaimed")]
    [TestCase(0, WalGcCursorFloorState.Available, true, "over_ceiling")]
    [TestCase(0, WalGcCursorFloorState.BlockedByUnusablePin, false, "blocked")]
    public async Task The_signal_is_stated_whatever_outcome_arm_the_pass_lands_on(
        long entriesTrimmed,
        WalGcCursorFloorState floorState,
        bool overThreshold,
        string arm)
    {
        // This is the falsifiable form of the design argument. Each case is a
        // pass that reaches a different outcome arm while the ceiling is
        // equally unreachable, and all three must state the condition. Folded
        // into the outcome ternary the signal could only ever have been
        // reported on one of them - reclaimed wins that ternary outright, and
        // a blocked floor short-circuits above the byte verdicts entirely - so
        // a regression that moves this emission under the classifier fails
        // here rather than in production.
        var tree = $"walgc-ceiling-cooccurs-{arm}";

        using var signal = await CollectCeilingSignalAsync(
            tree,
            Report(
                entriesTrimmed: entriesTrimmed,
                byteCeiling: 4096,
                cursorFloorState: floorState,
                bytePressureOverThreshold: overThreshold,
                logicalRetainedBytes: 2226,
                ceilingUnsatisfiable: true));

        Assert.That(signal.Counted.Sum(m => m.Value), Is.EqualTo(1),
            $"a pass landing on '{arm}' must still name an unreachable ceiling: the condition is a "
            + "standing property of the configuration, not one of the outcomes that partition a pass.");
    }
}

using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Pins the <b>refund class</b> of every reactivation outcome (issue #2938).
/// <para>
/// Two of the four outcomes are refunded against the attempt budget and two are
/// not, and that split is not merely behaviour - it is <b>load-bearing
/// evidence</b>. A refunded outcome costs
/// <c>MaxReactivationAttempts + MaxReactivationRefunds</c> = 6 touches per
/// abandonment; an unrefunded one costs <c>MaxReactivationAttempts</c> = 3. That
/// arithmetic is what let the epic diagnose two production trees as taking the
/// unrefunded path, from their <c>attempted/abandoned</c> ratio alone, at a time
/// when three of the four outcomes had no counter to read.
/// </para>
/// <para>
/// These fixtures exist because #2938 refactors the recording of outcomes out of
/// the loop that also decides refunds - the two concerns currently share one
/// piece of control flow. Separating them must leave the refund classes
/// bit-identical, because a silent shift would not just change production
/// behaviour, it would retrospectively invalidate a published finding that rests
/// on the ratio. They were therefore written and proven green <b>against the
/// unmodified scheduler, before that refactor</b>: a characterisation test
/// written afterwards pins whatever the refactor produced, which is precisely
/// the drift it was meant to detect.
/// </para>
/// <para>
/// The quantity asserted is deliberately <i>attempts before the first
/// abandonment</i> rather than a whole-window ratio. Abandonment is a pause
/// rather than a terminal state (issue #2783), so a long window ends
/// mid-cycle and its ratio carries a partial cycle's remainder - which makes
/// the assertion fragile without making it stronger. The first cycle is fully
/// determined and is exactly one budget.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Attempts charged by the time the first abandonment is published, which is
    /// the direct expression of a single outcome's refund class: one budget for
    /// an unrefunded outcome, one budget plus its permitted refunds for a
    /// refunded one.
    /// </summary>
    /// <remarks>
    /// Driven by observing the abandonment rather than by advancing a fixed
    /// span, so the fixture does not encode the cadence arithmetic a second
    /// time. A window sized by hand would have to be re-derived whenever the
    /// cooldown or the floor moved, and would fail as a timing puzzle rather
    /// than as the refund regression it is meant to report.
    /// </remarks>
    private static async Task<int> AttemptsBeforeFirstAbandonmentAsync(
        VirtualTimeProvider time,
        InstrumentRecorder recorder,
        int maxPasses = 400)
    {
        var guard = 0;
        while (Outcomes(recorder, "abandoned") == 0)
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(maxPasses),
                "the leaf never reached abandonment, so no refund class can be read from this run.");
        }

        return Outcomes(recorder, "attempted");
    }

    /// <summary>
    /// A scheduler whose only tree is permanently blocked by one leaf, with the
    /// leaf's probe behaviour supplied by the caller. Every refund-class fixture
    /// differs only in that behaviour, so sharing the rest keeps the outcome the
    /// single independent variable.
    /// </summary>
    /// <remarks>
    /// <paramref name="treeId"/> is a parameter rather than a constant because
    /// <see cref="InstrumentRecorder"/> filters by tree, so two schedulers built
    /// on the same tree name share a recording channel: the second recorder sees
    /// the first scheduler's abandonment, and any fixture that waits for an
    /// abandonment then reads a count belonging to the other arm. That is not
    /// hypothetical - it is what the comparison fixture below caught on its
    /// first run, and it produced a plausible wrong number rather than an error.
    /// Any fixture driving two schedulers at once must give them distinct trees.
    /// </remarks>
    private static (LatticeWalGcScheduler Scheduler, InstrumentRecorder Recorder) BlockedTreeProbing(
        VirtualTimeProvider time,
        Func<Task<string?>> probe,
        string? consumerId = null,
        string treeId = StrandedTree)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(consumerId ?? BlockedConsumerId(treeId))));

        var (factory, leaf) = FactoryWithBlockedLeaf(treeId);
        leaf.GetTreeIdAsync().Returns(_ => probe());

        var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, treeId);
        return (CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time), recorder);
    }

    [Test]
    public async Task ExecuteAsync_spends_one_budget_before_abandoning_a_leaf_whose_touch_completes()
    {
        // Completed is NOT refunded. The touch reached the leaf and returned, so
        // it is a real measurement of whether activation heals the pin - and a
        // measurement that says "no" is exactly what the budget is for. Refunding
        // it would mean a leaf that answers promptly and stays blocked is never
        // abandoned, which is the permanent-retry failure the cap exists to stop.
        var time = new VirtualTimeProvider();
        var (scheduler, recorder) = BlockedTreeProbing(
            time, () => Task.FromResult<string?>(StrandedTree));

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);

            Assert.That(await AttemptsBeforeFirstAbandonmentAsync(time, recorder), Is.EqualTo(3),
                "a completed touch is a real measurement, so it must be charged and cost exactly one budget.");

            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task ExecuteAsync_spends_one_budget_before_abandoning_a_consumer_it_cannot_resolve()
    {
        // Unresolvable is NOT refunded, and for a different reason from
        // Completed: nothing was measured at all, but the failure is a permanent
        // property of the id rather than of the silo, so retrying cannot change
        // it. Refunding would produce an unbounded 'attempted' series with no
        // touches behind it - cost with no evidence, which is the worst of both.
        var time = new VirtualTimeProvider();
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Task.FromResult<string?>(StrandedTree),
            consumerId: "not-a-materialiser-consumer-id");

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);

            Assert.That(await AttemptsBeforeFirstAbandonmentAsync(time, recorder), Is.EqualTo(3),
                "an id that can never resolve must still reach abandonment on one budget, not be refunded forever.");

            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task ExecuteAsync_excuses_a_capped_number_of_faulted_touches_before_abandoning()
    {
        // Faulted IS refunded, up to the cap. A throwing probe says the silo was
        // too busy to find out whether activation would heal the leaf, which is
        // evidence about the silo and not about the leaf - and abandonment is
        // only entitled to conclude the latter. The cap is what keeps that safe:
        // three excused touches, then three charged, then the verdict.
        var time = new VirtualTimeProvider();
        var (scheduler, recorder) = BlockedTreeProbing(
            time, () => throw new InvalidOperationException("silo refused the call"));

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);

            Assert.That(await AttemptsBeforeFirstAbandonmentAsync(time, recorder), Is.EqualTo(6),
                "a fault is refundable to the cap, so abandonment costs one budget plus its three refunds.");

            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task ExecuteAsync_excuses_a_capped_number_of_undelivered_touches_before_abandoning()
    {
        // Undelivered IS refunded, on the same reasoning as Faulted and with the
        // same cap. A caller-side timeout does not cancel the callee, so the
        // activation the probe requested is very probably still running; the
        // sweep stopped waiting, which is not evidence the leaf cannot heal.
        //
        // This shares a refund class with Faulted but is a distinct outcome, and
        // the distinction is the one the sweep's own effectiveness is judged on.
        // Asserting them separately is what stops a future change collapsing the
        // two into one arm on the grounds that they "behave the same".
        var time = new VirtualTimeProvider();
        var (scheduler, recorder) = BlockedTreeProbing(
            time, () => throw new TimeoutException("silo busy"));

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);

            Assert.That(await AttemptsBeforeFirstAbandonmentAsync(time, recorder), Is.EqualTo(6),
                "an undelivered touch is refundable to the cap, exactly as a fault is.");

            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task ExecuteAsync_separates_the_refunded_outcomes_from_the_unrefunded_by_a_factor_of_two()
    {
        // The discriminator itself, asserted as a relation rather than as two
        // numbers. This is the form the epic actually used in production: the
        // per-tree ratio was readable when three of the four outcome counters
        // did not exist, so it diagnosed the unrefunded path without needing a
        // counter for it. Pinning the relation keeps that inference valid even
        // if MaxReactivationAttempts or MaxReactivationRefunds is retuned - both
        // sides move together and the factor of two survives, which a pair of
        // hard-coded 3s and 6s would not.
        //
        // The two arms MUST run on distinct trees. On its first run this fixture
        // put both on StrandedTree, and because InstrumentRecorder filters by
        // tree the refunded arm's recorder saw the unrefunded arm's abandonment,
        // stopped waiting immediately, and reported that arm's 3 as its own. It
        // failed - but it failed with a credible wrong number, and had the two
        // costs happened to coincide it would have passed while comparing an arm
        // against itself.
        const string UnrefundedTree = "stranded-unrefunded";
        const string RefundedTree = "stranded-refunded";

        var unrefunded = new VirtualTimeProvider();
        var (completes, completesRecorder) = BlockedTreeProbing(
            unrefunded, () => Task.FromResult<string?>(UnrefundedTree), treeId: UnrefundedTree);

        var refunded = new VirtualTimeProvider();
        var (faults, faultsRecorder) = BlockedTreeProbing(
            refunded, () => throw new InvalidOperationException("silo refused the call"),
            treeId: RefundedTree);

        using (completesRecorder)
        using (faultsRecorder)
        {
            await StartAndRunFirstPassAsync(completes, unrefunded);
            await StartAndRunFirstPassAsync(faults, refunded);

            var unrefundedCost = await AttemptsBeforeFirstAbandonmentAsync(unrefunded, completesRecorder);
            var refundedCost = await AttemptsBeforeFirstAbandonmentAsync(refunded, faultsRecorder);

            Assert.Multiple(() =>
            {
                Assert.That(unrefundedCost, Is.GreaterThan(0),
                    "the unrefunded arm must actually abandon, or the ratio below is vacuous.");
                Assert.That(refundedCost, Is.EqualTo(unrefundedCost * 2),
                    "a refunded outcome must cost exactly twice an unrefunded one per abandonment, which is the separation the epic's production diagnosis rests on.");
            });

            await completes.StopAsync(CancellationToken.None);
            await faults.StopAsync(CancellationToken.None);
        }
    }
}

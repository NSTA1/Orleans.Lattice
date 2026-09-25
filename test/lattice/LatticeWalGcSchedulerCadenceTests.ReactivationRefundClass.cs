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
        string treeId = StrandedTree,
        Orleans.Lattice.BPlusTree.Grains.ILeafCursorReporter? cursorReporter = null)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(consumerId ?? BlockedConsumerId(treeId))));

        var (factory, leaf) = FactoryWithBlockedLeaf(treeId);

        // Issue #2692 Half B moved the seam this fixture injects on. The
        // scheduler used to touch the leaf with GetTreeIdAsync and now drives it
        // with DriveStarvedCheckpointAsync, so the probe has to arrive on the
        // call the scheduler actually makes. Left on GetTreeIdAsync it still
        // compiled, still read as fault injection, and injected nothing: every
        // touch fell through to FactoryWithBlockedLeaf's default Lifted stub and
        // was classified 'completed', which halved the refunded budgets from six
        // to three and moved three terminal counts onto the wrong arm.
        //
        // Worth naming because the failure was not the fix being wrong - it was a
        // test double still wired to the old seam, which is the one failure shape
        // a diff of the production change cannot show you.
        leaf.DriveStarvedCheckpointAsync().Returns(_ => DriveFromProbe(probe));

        // Inert, and deliberately not the probe. Wiring the probe to both seams
        // would double-consume any probe that counts its invocations, which is
        // most of them in this file.
        leaf.GetTreeIdAsync().Returns(_ => Task.FromResult<string?>(treeId));

        var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, treeId);
        return (CreateScheduler(factory, gc, Adaptive(floor: SweepPass), time, cursorReporter: cursorReporter), recorder);
    }

    /// <summary>
    /// Adapts a tree-id probe to a drive verdict, preserving exactly how it
    /// fails: a throwing probe still throws out of the drive, so the scheduler's
    /// timeout and fault arms classify it as they always did.
    /// </summary>
    private static async Task<LeafStarvationDriveOutcome> DriveFromProbe(Func<Task<string?>> probe)
    {
        var treeId = await probe().ConfigureAwait(false);
        return treeId is null
            ? LeafStarvationDriveOutcome.NotDriven
            : LeafStarvationDriveOutcome.Lifted;
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

    [Test]
    public async Task ExecuteAsync_never_charges_or_abandons_a_consumer_whose_drives_are_refused_admission()
    {
        // Issue #3575. A refused admission is raised by the leaf's silo before
        // its drive starts, so it tests nothing about the leaf, and it is the
        // one outcome excused outright rather than refunded within the cap.
        // Filed as 'faulted' it spent the budget of consumers the sweep never
        // managed to drive and ended in a give-up that blamed their snapshot
        // capture: 89% of one deployment's touches went that way.
        //
        // Six hours at the five-minute floor is ample for either wrong class to
        // show itself: charged, the consumer is abandoned after three touches,
        // and refunded to the cap after six.
        const string RefusedTree = "stranded-admission-refused";
        var time = new VirtualTimeProvider();
        var (scheduler, recorder) = BlockedTreeProbing(time, AdmissionRefusedProbe, treeId: RefusedTree);

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);
            await AdvanceAtLeastAsync(time, TimeSpan.FromHours(6));

            var attempted = Outcomes(recorder, "attempted");
            Assert.Multiple(() =>
            {
                Assert.That(attempted, Is.GreaterThan(6),
                    "more touches than a refunded budget could buy, or refusals are still being charged.");
                Assert.That(Outcomes(recorder, "admission_refused"), Is.EqualTo(attempted),
                    "every refused touch must land on its own arm.");
                Assert.That(Outcomes(recorder, "faulted"), Is.Zero,
                    "a refusal is back-pressure, not a fault, and must not be counted as one.");
                Assert.That(Outcomes(recorder, "abandoned"), Is.Zero,
                    "a consumer that was never driven must never be given up on: the give-up claims the block "
                    + "is not clearable by activation, which a refusal never tested.");
                Assert.That(attempted, Is.LessThan(40),
                    "but the retry must escalate: touched at every pass of the six hours the consumer would "
                    + "take about seventy touches, whereas a delay that doubles to the cooldown takes under thirty.");
            });

            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task ExecuteAsync_retries_a_consumer_refused_admission_sooner_than_the_retry_cooldown()
    {
        // The cooldown's premise is that a touch which ran and did not heal will
        // not heal if repeated at once. A refused touch never ran, so the
        // premise does not hold for it (issue #3575): it is retried after a
        // short jittered delay, which at a five-minute floor is the next pass.
        const string RetryTree = "stranded-admission-retry";
        var calls = 0;
        var time = new VirtualTimeProvider();
        var (scheduler, recorder) = BlockedTreeProbing(
            time,
            () => Interlocked.Increment(ref calls) == 1
                ? AdmissionRefusedProbe()
                : Task.FromResult<string?>(RetryTree),
            treeId: RetryTree);

        using (recorder)
        {
            await StartAndRunFirstPassAsync(scheduler, time);

            var guard = 0;
            while (Outcomes(recorder, "admission_refused") == 0)
            {
                await TickAsync(time);
                Assert.That(++guard, Is.LessThan(100), "the first touch was never made.");
            }

            var refusedAt = time.GetUtcNow();
            while (Outcomes(recorder, "attempted") < 2)
            {
                await TickAsync(time);
                Assert.That(++guard, Is.LessThan(100), "the refused consumer was never touched again.");
            }

            var retriedAfter = time.GetUtcNow() - refusedAt;
            Assert.Multiple(() =>
            {
                Assert.That(retriedAfter, Is.LessThan(TimeSpan.FromMinutes(15)),
                    "a refused consumer must not wait out the fifteen-minute retry cooldown a touch that ran "
                    + "has to serve.");
                Assert.That(Outcomes(recorder, "completed"), Is.EqualTo(1),
                    "and the early retry must reach the leaf once the refusal clears.");
            });

            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public void AdmissionRefusedRetryDelay_doubles_per_refusal_is_jittered_upward_and_never_exceeds_the_cooldown()
    {
        // The retry cooldown is private to the scheduler; fifteen minutes is its
        // value, pinned here so a retune of either constant is a visible change.
        var cooldown = TimeSpan.FromMinutes(15);

        Assert.Multiple(() =>
        {
            Assert.That(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(1, 0.0),
                Is.EqualTo(LatticeWalGcScheduler.AdmissionRefusedRetryBaseDelay));
            Assert.That(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(1, 0.999),
                Is.GreaterThan(TimeSpan.FromMinutes(1)).And.LessThan(TimeSpan.FromMinutes(1.5)),
                "jitter stretches the delay by up to half again, so refusals from one moment spread out.");
            Assert.That(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(2, 0.0), Is.EqualTo(TimeSpan.FromMinutes(2)));
            Assert.That(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(3, 0.0), Is.EqualTo(TimeSpan.FromMinutes(4)));
            Assert.That(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(4, 0.0), Is.EqualTo(TimeSpan.FromMinutes(8)));
            Assert.That(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(4, 0.999), Is.LessThanOrEqualTo(cooldown));
            Assert.That(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(5, 0.0), Is.EqualTo(cooldown),
                "the doubling stops at the cooldown, so a consumer refused on every touch is touched no closer "
                + "together than a charged one.");
            Assert.That(LatticeWalGcScheduler.AdmissionRefusedRetryDelay(int.MaxValue, 0.999), Is.EqualTo(cooldown),
                "however long the run of refusals and whatever the jitter.");
        });
    }
}

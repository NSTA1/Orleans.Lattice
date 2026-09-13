using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for the reactivation give-up budget on a tree whose reported blocker
/// does not hold still (issue #2772).
/// <para>
/// The heal action destroys the evidence that the heal is not working. The
/// durable floor skips any consumer present in the live cursor registry before
/// it evaluates that consumer's pin, and a leaf reports a cursor as soon as it
/// activates - so touching the leaf currently reported as blocking is precisely
/// what moves the report onto a different one. A budget keyed on the reported
/// consumer was therefore reset by the sweep's own remedy, which made the
/// give-up branch unreachable rather than merely slow: two blocked leaves are
/// enough to rotate indefinitely.
/// </para>
/// <para>
/// These fixtures drive the rotation off the reactivation call itself rather
/// than off a timer, because that is the causal shape of the real defect. A
/// test that rotated on a schedule would still redden, but it would be
/// asserting a coincidence rather than the mechanism.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// The distinguishing fragment of the unreachable-block escalation. It is
    /// matched on rather than the per-blocker "cannot reclaim" warning because
    /// the two are different conditions - being blocked at all, versus having
    /// been unable to attempt a remedy for long enough to give up - and a
    /// fragment common to both would conflate them.
    /// <para>
    /// This comment used to justify the choice by saying the cannot-reclaim
    /// warning "fires on every change of reported blocker and is therefore
    /// abundant in exactly the churn fixtures". That was true and is no longer:
    /// issue #2815 throttled it to once per episode, precisely because these
    /// churn fixtures describe the population it was abundant on.
    /// </para>
    /// </summary>
    private const string UnreachableBlockWarning = "has not been able to attempt a reactivation";

    /// <summary>A materialiser consumer id naming a specific leaf on the stranded tree.</summary>
    private static string ConsumerIdFor(string leafKey) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{StrandedTree}_{GrainId.Create("bplusleaf", leafKey)}";

    /// <summary>
    /// A blocked report that also trimmed entries, which the real collector
    /// produces whenever a configured retention ages entries out on a tree whose
    /// cursor branch is disabled: the TTL half of the trim predicate does not
    /// consult the cursor floor.
    /// </summary>
    private static LatticeWalGcReport BlockedReportNamingWithTrim(string consumerId, long entriesTrimmed) =>
        new("tree", null, null, null, null, 1, entriesTrimmed, null, null, null, false, false,
            WalGcCursorFloorState.BlockedByUnusablePin, consumerId);

    private static (LatticeWalGcScheduler Scheduler, RecordingLoggerFactory Logs) SchedulerWithLogs(
        IGrainFactory factory,
        ILatticeWalGc gc,
        VirtualTimeProvider time)
    {
        var logs = new RecordingLoggerFactory();
        var scheduler = CreateScheduler(
            factory, gc, Adaptive(), time, snapshotPins: null,
            logger: new Logger<LatticeWalGcScheduler>(logs));
        return (scheduler, logs);
    }

    // Reads Counted, not Measurements. Every reactivation outcome is zero-primed
    // once per tree (issue #2783), so the unfiltered list carries four zeros that
    // are not events. Counting them would inflate every assertion below - and the
    // inflation is uniform, so it would read as a behavioural regression rather
    // than as a harness artefact. Counted filters zero-valued measurements, which
    // is exactly the "real events only" set these fixtures mean. The priming
    // itself is asserted by ExecuteAsync_primes_every_reactivation_outcome_at_zero,
    // which is deliberately the only fixture reading Measurements unfiltered.
    private static string?[] Outcomes(InstrumentRecorder recorder) =>
        recorder.Counted.Select(m => m.Tag(LatticeMetrics.TagOutcome) as string).ToArray();

    private static int Count(string?[] outcomes, string outcome) =>
        outcomes.Count(o => string.Equals(o, outcome, StringComparison.Ordinal));

    [Test]
    public async Task ExecuteAsync_gives_up_on_a_blocker_that_rotates_away_and_returns()
    {
        // The defect of #2772, reproduced through its own mechanism: each touch
        // makes the touched leaf present in the live registry, so the floor
        // reports the other blocked leaf on the following pass, and the touched
        // one returns when it deactivates still blocked. Keyed on the reported
        // consumer the budget was reset by every rotation and never reached its
        // limit, so a tree that can never heal reported as still healing for as
        // long as the silo ran. Keyed on the blocking leaf, each leaf resumes
        // its own count when it returns and the give-up is reached.
        var first = ConsumerIdFor("leaf-a");
        var second = ConsumerIdFor("leaf-b");
        var reported = first;

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(reported)));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        leaf.GetTreeIdAsync().Returns(_ =>
        {
            reported = string.Equals(reported, first, StringComparison.Ordinal) ? second : first;
            return Task.FromResult<string?>(StrandedTree);
        });

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Long enough that a budget which resets on rotation would still be
        // issuing attempts, so the bound is what stops this and not the clock.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(90));

        var outcomes = Outcomes(recorder);
        Assert.Multiple(() =>
        {
            // Abandonment is a pause rather than a terminal state (issue #2783),
            // so the window covers two of them. The schedule is fully determined:
            // the two leaves alternate at the 5 min cadence floor, each spending
            // exactly its own budget of 3 (6 attempts, the last at t55); leaf-a
            // is reported spent at t60 and abandoned; it serves the full 30 min
            // backoff and re-arms at t90, where the elapsed cooldown lets it take
            // the first touch of its second cycle (the 7th attempt); leaf-b is
            // then reported spent at t95 and abandoned.
            //
            // These remain the assertions that catch #2772. Were the budget reset
            // by rotation, no leaf would ever reach its limit: abandoned would be
            // 0 rather than 2, and attempts would be issued every cadence tick for
            // the whole window - around 18 rather than 7. Pinning the exact values
            // keeps that discrimination while describing the re-armed behaviour
            // truthfully, which a bound of "at most 6" no longer does.
            Assert.That(Count(outcomes, "abandoned"), Is.EqualTo(2),
                "a tree that can never heal must be reported as given up on, however the reported blocker rotates.");
            Assert.That(Count(outcomes, "attempted"), Is.EqualTo(7),
                "each blocking leaf gets its own budget per cycle and no more, so two leaves cost two budgets plus the first touch of the one cycle that re-arms inside the window.");
            Assert.That(Count(outcomes, "rearmed"), Is.EqualTo(1),
                "exactly one leaf's backoff elapses inside the window, so the retry is periodic and bounded rather than permanent or hot.");
            Assert.That(outcomes, Does.Not.Contain("healed"),
                "a leaf that rotates off the head of the queue because the sweep activated it has not healed.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_keeps_the_attempt_budget_across_a_pass_that_trims_while_still_blocked()
    {
        // The second reset path. The episode used to end on the pass outcome
        // rather than on the floor state, and the TTL branch of the trim
        // predicate is independent of the cursor branch - so a single aged-out
        // entry on a tree whose floor was still blocked erased the attempt
        // budget and the abandoned flag, and credited a heal to a tree that had
        // never unblocked. This needs no second leaf and no rotation.
        var consumerId = ConsumerIdFor("leaf-a");
        var passes = 0;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(++passes % 12 == 0
                ? BlockedReportNamingWithTrim(consumerId, entriesTrimmed: 4)
                : BlockedReportNaming(consumerId)));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(60));

        var outcomes = Outcomes(recorder);
        Assert.Multiple(() =>
        {
            Assert.That(Count(outcomes, "abandoned"), Is.EqualTo(1),
                "an incidental age-based trim is not evidence that the cursor floor unblocked, so it must not reset the budget.");
            Assert.That(Count(outcomes, "healed"), Is.Zero,
                "a tree whose floor never unblocked must never be credited with a heal.");
            Assert.That(Count(outcomes, "attempted"), Is.EqualTo(3),
                "the blocker held still throughout, so it must spend exactly one budget.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_escalates_a_block_it_can_never_get_purchase_on()
    {
        // The hole no attempt-derived budget can cover, however monotonic. An
        // attempt is gated on the reported blocker holding still for the minimum
        // block age, so a tree whose blocker changes faster than that is never
        // touched at all and has no budget to spend. Silence here would be
        // indistinguishable from a healthy tree, which is the failure #2772
        // reports.
        var pass = 0;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(ConsumerIdFor($"leaf-{pass++}"))));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);

        var (scheduler, logs) = SchedulerWithLogs(factory, gc, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(70));

        await leaf.DidNotReceive().GetTreeIdAsync();

        var escalations = logs.Warnings
            .Where(e => e.Message.Contains(UnreachableBlockWarning, StringComparison.Ordinal))
            .ToArray();

        Assert.That(escalations, Has.Length.EqualTo(1),
            "a tree the sweep cannot get purchase on must be reported once, not once per pass and not never.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_does_not_escalate_a_tree_that_is_draining()
    {
        // The counterweight, and the reason the escalation is keyed on the last
        // attempt rather than on the age of the episode. A tree with many
        // blocked leaves reveals them one at a time and can stay blocked for
        // hours while healing steadily; it is working, not stuck. Keyed on the
        // episode alone this would alarm on every large recovery, which would
        // teach operators to ignore the alarm that matters.
        var healed = 0;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(ConsumerIdFor($"leaf-{healed}"))));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        leaf.GetTreeIdAsync().Returns(_ =>
        {
            // Each touched leaf captures its snapshot and stops blocking, and
            // the next blocked leaf takes its place at the head of the queue.
            healed++;
            return Task.FromResult<string?>(StrandedTree);
        });

        var (scheduler, logs) = SchedulerWithLogs(factory, gc, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromHours(3));

        Assert.Multiple(() =>
        {
            Assert.That(healed, Is.GreaterThan(10),
                "the fixture must actually be draining, or it would prove nothing about a draining tree.");
            Assert.That(
                logs.Warnings.Count(e => e.Message.Contains(UnreachableBlockWarning, StringComparison.Ordinal)),
                Is.Zero,
                "a tree that keeps healing leaves must never be reported as one the sweep cannot get purchase on.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_credits_one_heal_per_swept_consumer_when_the_block_clears()
    {
        // The heal credit is documented as the measure of whether the sweep is
        // working, so an inflated numerator is worse than a missing one. It used
        // to be credited every time the reported blocker changed, which counted
        // a rotating leaf once per rotation and counted leaves that had not
        // healed at all. Deferred to the end of the episode the claim is sound:
        // the floor is no longer blocked by anything, so everything swept during
        // the episode has in fact stopped blocking.
        var first = ConsumerIdFor("leaf-a");
        var second = ConsumerIdFor("leaf-b");
        var reported = first;
        var blocked = true;

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(blocked
                ? BlockedReportNaming(reported)
                : Report(entriesTrimmed: 9)));
        var time = new VirtualTimeProvider();
        var (factory, leaf) = FactoryWithBlockedLeaf(StrandedTree);
        leaf.GetTreeIdAsync().Returns(_ =>
        {
            reported = string.Equals(reported, first, StringComparison.Ordinal) ? second : first;
            return Task.FromResult<string?>(StrandedTree);
        });

        using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, StrandedTree);
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Three touches across two leaves: a, then b, then a again.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(20));
        await leaf.Received(3).GetTreeIdAsync();
        Assert.That(Count(Outcomes(recorder), "healed"), Is.Zero,
            "a rotation is not a heal, so nothing may be credited while the floor is still blocked.");

        // The captures complete, the pins lift, and the tree reclaims again.
        blocked = false;
        await TickAsync(time);

        Assert.That(Count(Outcomes(recorder), "healed"), Is.EqualTo(2),
            "one credit per distinct leaf the sweep touched during the episode, not one per rotation.");

        await scheduler.StopAsync(CancellationToken.None);
    }
}

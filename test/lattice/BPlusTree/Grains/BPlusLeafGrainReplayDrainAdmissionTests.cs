using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the <b>demand-side</b> half of WAL replay permit admission
/// (issue #3290): the queue must also be failing to drain before a deep queue
/// is refused.
/// <para>
/// <b>Why a second predicate rather than a bigger number.</b> The bound of issue
/// #3284 is <c>ceiling * depthPerPermit</c>, where the ceiling is
/// <c>min(ProcessorCount, ContainerCpuGrant)</c> - a supply-side quantity, a
/// drain rate. The queue it guards is filled by cluster-wide activation fan-out,
/// a demand-side quantity with no CPU term in it at all. Multiplying one by a
/// dimensionless constant cannot produce a bound on the other, and the
/// measurement confirmed the consequence: the same healthy fan-out offered 31
/// waiters against a bound of 64 on a 16-processor host and 40 against a bound
/// of 16 on a 4-processor one. Offered depth was invariant across a fourfold
/// change in the term the bound is derived from - and slightly <i>higher</i> at
/// the low ceiling, because slower draining increases overlap, so the residual
/// ran opposite to the model. The bound was therefore anti-correlated with need,
/// and raising the constant would only move which host size it is wrong on.
/// </para>
/// <para>
/// So these tests assert the discrimination, not a threshold: a deep queue that
/// is draining is healthy fan-out and must be admitted, and a deep queue that is
/// not draining is the backlog of issue #3284 and must still be refused.
/// </para>
/// <para>
/// Every test here perturbs the process-wide gate statics, so the fixture is
/// <see cref="NonParallelizableAttribute"/> and resets them in teardown.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class BPlusLeafGrainReplayDrainAdmissionTests
{
    private static readonly TimeSpan MaxQueueWait = TimeSpan.FromSeconds(5);

    [TearDown]
    public void ResetGate() => BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

    /// <summary>
    /// A queue draining well inside the configured wait is not stalled, however
    /// deep it is. This is the arm that unblocks the healthy fan-out.
    /// </summary>
    [Test]
    public void A_queue_draining_inside_the_configured_wait_is_not_refused()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            TimeSpan.FromMilliseconds(3), sinceLastProgress: TimeSpan.FromMilliseconds(12));

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "a queue whose waits are milliseconds is draining and must not be treated as stalled.");
    }

    /// <summary>
    /// The smoothed wait reaching the configured maximum is a stall, even while
    /// acquisitions are still completing. This is the regime of issue #3284:
    /// permits keep turning over, but every arrival's share of the queue exceeds
    /// what a request deadline can absorb.
    /// </summary>
    [Test]
    public void A_queue_whose_mean_wait_reaches_the_maximum_is_refused()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            MaxQueueWait, sinceLastProgress: TimeSpan.Zero);

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "a mean wait at the maximum is the backlog regime and must be refused.");
    }

    /// <summary>
    /// A gate that has not completed a wait for longer than the configured
    /// maximum is stalled, even though its last recorded mean was healthy.
    /// </summary>
    /// <remarks>
    /// This arm is not redundant with the mean, and it is the arm a total stall
    /// needs. The mean is folded only as waits <b>terminate</b>, so a gate whose
    /// permits are all held by wedged replays produces no new samples at all and
    /// would keep reporting the healthy mean it last observed, indefinitely -
    /// the worst case reading as the best one. Time since the last acquisition is
    /// the only signal that survives when nothing is terminating.
    /// </remarks>
    [Test]
    public void A_gate_that_has_not_acquired_for_longer_than_the_maximum_is_refused()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            TimeSpan.FromMilliseconds(1), sinceLastProgress: MaxQueueWait + TimeSpan.FromSeconds(1));

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "a healthy historical mean must not mask a gate that has stopped acquiring entirely.");
    }

    /// <summary>
    /// A gate with no evidence at all admits.
    /// </summary>
    /// <remarks>
    /// Every unknown at this seam resolves toward admitting, deliberately.
    /// Nothing here makes a caller back off and the public API has no retry, so a
    /// refusal does not shed load, it <b>fails</b> it, turning a slow success
    /// into a hard error. A cold process is exactly when the queue is deepest -
    /// mass reactivation - and exactly when there is least evidence, so failing
    /// toward refusal would refuse hardest where it knows least.
    /// </remarks>
    [Test]
    public void A_cold_gate_with_no_completed_waits_is_admitted()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: null);

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "a gate that has never completed a wait has no evidence of harm and must admit.");
    }

    /// <summary>
    /// A non-positive maximum disables this half, restoring the pure depth bound
    /// of issue #3284 for an operator who wants the historical shape back.
    /// </summary>
    [Test]
    public void A_non_positive_maximum_disables_the_drain_half()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            TimeSpan.FromHours(1), sinceLastProgress: TimeSpan.FromHours(1));

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(TimeSpan.Zero),
            Is.False,
            "zero must disable the drain half rather than refuse unconditionally.");
        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(TimeSpan.FromSeconds(-1)),
            Is.False,
            "a negative maximum must be treated as disabled, not as an always-stalled gate.");
    }

    /// <summary>
    /// The smoothed mean moves toward observed waits rather than tracking the
    /// last sample, so one slow replay cannot trip a refusal on its own.
    /// </summary>
    [Test]
    public void The_mean_smooths_rather_than_tracking_the_last_sample()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: TimeSpan.Zero);
        BPlusLeafGrain.NoteReplayPermitQueueWaitForTest(TimeSpan.FromSeconds(80), acquired: true);

        var afterOneOutlier = BPlusLeafGrain.ReplayPermitWaitMeanForTest;
        Assert.That(
            afterOneOutlier,
            Is.GreaterThan(TimeSpan.Zero),
            "the mean must respond to an observation at all.");
        Assert.That(
            afterOneOutlier,
            Is.LessThan(TimeSpan.FromSeconds(80)),
            "a single outlier must not become the mean.");

        for (var i = 0; i < 40; i++)
            BPlusLeafGrain.NoteReplayPermitQueueWaitForTest(TimeSpan.FromSeconds(80), acquired: true);

        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitMeanForTest,
            Is.GreaterThan(MaxQueueWait),
            "a sustained slow regime must carry the mean past the maximum.");
    }

    /// <summary>
    /// A cancelled wait folds into the mean, and does not count as an
    /// acquisition.
    /// </summary>
    /// <remarks>
    /// A cancellation is the strongest available evidence of the harm issue
    /// #3284 describes - an activation that waited until its request budget was
    /// gone - so excluding it would blind the mean to exactly the population it
    /// exists to detect. It must not refresh the acquisition timestamp though:
    /// nothing left the gate, so the stall arm must keep counting.
    /// </remarks>
    [Test]
    public void A_cancelled_wait_informs_the_mean_but_is_not_an_acquisition()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: null);
        BPlusLeafGrain.NoteReplayPermitQueueWaitForTest(TimeSpan.FromSeconds(30), acquired: false);

        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitMeanForTest,
            Is.GreaterThan(TimeSpan.Zero),
            "a cancellation is evidence of queue cost and must inform the mean.");
        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "one cancellation must not by itself trip the refusal; the gate has still acquired nothing to time.");
    }

    /// <summary>
    /// The depth predicate of issue #3284 is unchanged by this work: a deep
    /// queue still fails the depth test. The change is only that failing it is
    /// no longer sufficient on its own.
    /// </summary>
    [Test]
    public void The_depth_bound_is_unchanged_and_still_fails_a_deep_queue()
    {
        const int DepthPerPermit = 4;
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 4, queued: 16);

        Assert.That(
            BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                DepthPerPermit, LatticeReplayAdmissionClass.Interactive, out var queued, out var bound),
            Is.False,
            "the depth half must still refuse at the bound.");
        Assert.That(bound, Is.EqualTo(16));
        Assert.That(queued, Is.EqualTo(16));

        // ... and the measured healthy fan-out at that exact depth is admitted,
        // because the queue is draining. This pair is the whole fix.
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            TimeSpan.FromMilliseconds(2), sinceLastProgress: TimeSpan.FromMilliseconds(5));
        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "the same depth that trips #3284's bound must be admitted when the queue is draining.");
    }

    /// <summary>
    /// The default wait separates the two measured regimes by a wide margin in
    /// both directions, rather than sitting between them.
    /// </summary>
    /// <remarks>
    /// The healthy regime drained 40 queued waiters inside a test that completed
    /// in 111 ms; the pathological regime of issue #3284 was 87 waiters against a
    /// ceiling of 6 exhausting an Orleans request deadline in the queue. A
    /// threshold that only just cleared the healthy side would turn a slow test
    /// machine into a refusal, and one that only just undercut the pathological
    /// side would be sensitive to its exact service time. Neither is a number
    /// worth defending, so the default is asserted to be clear of both.
    /// </remarks>
    [Test]
    public void The_default_wait_is_clear_of_both_measured_regimes()
    {
        Assert.That(
            LatticeOptions.DefaultWalReplayPermitMaxQueueWait,
            Is.GreaterThan(TimeSpan.FromSeconds(1)),
            "must be well above the sub-second waits of the healthy regime.");
        Assert.That(
            LatticeOptions.DefaultWalReplayPermitMaxQueueWait,
            Is.LessThan(TimeSpan.FromSeconds(15)),
            "must be well below the Orleans 30s response timeout the backlog regime exhausts.");
        Assert.That(
            new LatticeOptions().WalReplayPermitMaxQueueWait,
            Is.EqualTo(LatticeOptions.DefaultWalReplayPermitMaxQueueWait),
            "the option must default to the documented value.");
    }

    /// <summary>
    /// <b>The idle-versus-wedged discrimination (issue #3290, review finding).</b>
    /// A gate that has simply been quiet for longer than the maximum, with every
    /// permit free, must admit the burst that ends the quiet period.
    /// <para>
    /// Time since the last <i>acquisition</i> is long in two states: a gate
    /// wedged with every permit held, and a gate idle with every permit free.
    /// The first is the harm; the second is the healthiest state the system has,
    /// and a quiet period longer than the maximum is entirely ordinary. Stamping
    /// the start of the queueing epoch is what separates them, because an idle
    /// gate's queue reached zero and a wedged gate's never does.
    /// </para>
    /// </summary>
    [Test]
    public void A_burst_arriving_after_an_idle_period_is_admitted()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 4, queued: 0);

        // The gate has been quiet far longer than the maximum, so the progress
        // reading is stale for the healthiest possible reason. The mean is the
        // sub-second one the last healthy drain left behind.
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            TimeSpan.FromMilliseconds(4), sinceLastProgress: MaxQueueWait + TimeSpan.FromSeconds(30));

        // The first arrival of the new burst makes the queue non-empty.
        BPlusLeafGrain.NoteReplayPermitArrivalForTest();

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "a burst that merely follows a quiet period must not be judged against a timestamp "
                + "that the quietness itself made stale - nothing was holding the gate up.");
    }

    /// <summary>
    /// The counterpart, and the arm that stops the fix above from becoming a
    /// hole: a queue that has been occupied for longer than the maximum without
    /// any acquisition is wedged, and must still be refused.
    /// <para>
    /// This is what rules out the obvious alternative repair of clearing the
    /// progress reading when the queue drains to zero. A wedged queue never
    /// returns to zero, so that variant would leave the field reading <i>cold</i>
    /// - which admits - for the whole duration of a wedge that began after an
    /// idle period, while no terminating wait moved the mean either. Both arms
    /// would fall silent together in exactly the regime they exist for.
    /// </para>
    /// </summary>
    [Test]
    public void A_queue_occupied_past_the_maximum_without_progress_is_refused()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 4, queued: 0);

        // A queueing epoch that began longer ago than the maximum, and no
        // acquisition since: the queue never drained, so this is a wedge.
        BPlusLeafGrain.NoteReplayPermitArrivalForTest(
            epochAge: MaxQueueWait + TimeSpan.FromSeconds(1));

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "a queue that has been occupied past the maximum with nothing acquiring is wedged.");
    }

    /// <summary>
    /// Only the empty-to-non-empty transition stamps the epoch. A later arrival
    /// joining an already-occupied queue must not refresh it, or a wedge fed by
    /// continuous arrivals would renew its own timestamp forever.
    /// </summary>
    [Test]
    public void A_later_arrival_joining_an_occupied_queue_does_not_refresh_the_epoch()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 4, queued: 0);
        BPlusLeafGrain.NoteReplayPermitArrivalForTest(
            epochAge: MaxQueueWait + TimeSpan.FromSeconds(1));

        // Arrivals keep landing on the wedged queue, as they do in a real storm.
        BPlusLeafGrain.NoteReplayPermitArrivalForTest();
        BPlusLeafGrain.NoteReplayPermitArrivalForTest();

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "arrivals joining an occupied queue must not renew the epoch, or a wedge fed by "
                + "continuous arrivals would never be detected.");
    }

    /// <summary>
    /// A queue that genuinely drains and then refills starts a fresh epoch, so
    /// the second burst is judged on its own waiting rather than on how long ago
    /// the first one happened to be.
    /// </summary>
    [Test]
    public void A_queue_that_drains_and_refills_starts_a_fresh_epoch()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 4, queued: 0);
        BPlusLeafGrain.NoteReplayPermitArrivalForTest(
            epochAge: MaxQueueWait + TimeSpan.FromSeconds(1));
        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "precondition: the first epoch reads as wedged.");

        // The queue drains completely, then a new burst arrives.
        BPlusLeafGrain.NoteReplayPermitDepartureForTest();
        BPlusLeafGrain.NoteReplayPermitArrivalForTest();

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "a queue that reached zero has demonstrably drained, so the next burst must be "
                + "judged on its own epoch.");
    }
}

using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the <b>expiry</b> of the smoothed-wait arm of WAL replay permit
/// admission (issue #3306): a mean nothing has tested for longer than the
/// configured maximum is not evidence about the present and must not refuse.
/// <para>
/// <b>The defect this fixture pins.</b> The mean is folded only as waits
/// <i>terminate</i>, so it has no clock of its own and no way to grow stale on
/// its own. Left unbounded it therefore answers "how long were the last few
/// waits this process ever completed" while
/// <see cref="BPlusLeafGrain.IsReplayPermitQueueNotDraining(TimeSpan)"/> reads
/// the answer as a description of now. One cold-replay storm is enough to set
/// it for the life of the process: the incident measured a smoothed wait of
/// 300,044 ms against a five-second maximum, on a gate that was by then idle.
/// </para>
/// <para>
/// <b>And it is a latch, not merely a stale reading.</b> A refusal completes no
/// wait, so it records no sample; the only thing that can bring the mean back
/// down is the admission the mean is itself suppressing. At 1/8 smoothing a
/// sixtyfold overshoot needs some thirty-one samples to clear, and the
/// mechanism denies itself every one of them - which is why the incident's
/// symptom was shedding for a whole process lifetime rather than a burst of it.
/// </para>
/// <para>
/// This is the same defect issue #3290 fixed on the sibling stall arm, on the
/// sibling field, and missed here. Because the two arms are disjoined, an
/// unguarded mean defeats a guarded stall arm outright - so the tests below
/// assert both that the latch is gone and that nothing the stall arm detects
/// was lost with it.
/// </para>
/// <para>
/// Every test here perturbs the process-wide gate statics, so the fixture is
/// <see cref="NonParallelizableAttribute"/> and resets them in teardown.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class BPlusLeafGrainReplayDrainMeanStalenessTests
{
    private static readonly TimeSpan MaxQueueWait = TimeSpan.FromSeconds(5);

    /// <summary>The smoothed wait the incident measured, to scale.</summary>
    private static readonly TimeSpan PoisonedMean = TimeSpan.FromMilliseconds(300_044);

    [TearDown]
    public void ResetGate() => BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

    /// <summary>
    /// <b>The regression.</b> A burst arriving after the storm that poisoned the
    /// mean has ended must be admitted, because no wait has terminated for
    /// longer than the maximum and the mean therefore describes a regime that is
    /// over.
    /// </summary>
    /// <remarks>
    /// This is the incident's exact shape. The queueing epoch is fresh - the
    /// burst has only just started, so the stall arm correctly stays silent
    /// thanks to issue #3290 - and the gate is idle. The only thing left saying
    /// "refuse" is a mean measured minutes ago, and before this fix that was
    /// enough on its own.
    /// </remarks>
    [Test]
    public void A_burst_after_the_storm_that_poisoned_the_mean_is_admitted()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            PoisonedMean,
            sinceLastProgress: TimeSpan.Zero,
            sinceLastSample: MaxQueueWait + TimeSpan.FromMinutes(10));

        // The first arrival of the new burst makes the queue non-empty, stamping
        // a fresh epoch exactly as it does in production.
        BPlusLeafGrain.NoteReplayPermitArrivalForTest();

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "a mean no wait has tested for longer than the maximum describes a regime that has "
                + "ended, and must not refuse the burst that ends the quiet period.");
    }

    /// <summary>
    /// The stale mean is <b>discarded</b>, not merely skipped for one decision.
    /// </summary>
    /// <remarks>
    /// This is the load-bearing half, and skipping alone would not have fixed
    /// the incident. A fossil that is only stepped over is still there to be
    /// folded into, so the very first wait to terminate after the quiet period
    /// would re-arm the arm on evidence that had already been judged
    /// inadmissible - and at 1/8 smoothing it would keep doing so for some
    /// thirty-one further samples, which is most of a second storm.
    /// </remarks>
    [Test]
    public void A_stale_mean_is_discarded_so_the_next_sample_cannot_re_arm_it()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            PoisonedMean,
            sinceLastProgress: TimeSpan.Zero,
            sinceLastSample: MaxQueueWait + TimeSpan.FromMinutes(10));

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "precondition: the stale mean must not refuse.");
        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitMeanForTest,
            Is.EqualTo(TimeSpan.Zero),
            "the fossil must be discarded rather than stepped over, or it survives to be folded into.");

        // A single healthy wait now terminates, as it would once the burst above
        // was admitted. It must produce a healthy mean, not seven-eighths of the
        // fossil.
        BPlusLeafGrain.NoteReplayPermitQueueWaitForTest(TimeSpan.FromMilliseconds(8), acquired: true);

        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitMeanForTest,
            Is.LessThan(MaxQueueWait),
            "the first wait after the discard must build a new mean, not decay the old one.");
        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "and the gate must stay open rather than re-arming on the discarded mean.");
    }

    /// <summary>
    /// A mean that is still being tested keeps refusing. This is the arm's whole
    /// purpose and expiry must not weaken it.
    /// </summary>
    [Test]
    public void A_mean_still_being_tested_by_terminating_waits_is_refused()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            PoisonedMean,
            sinceLastProgress: TimeSpan.Zero,
            sinceLastSample: MaxQueueWait - TimeSpan.FromSeconds(1));

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "a queue whose waits are still terminating slowly is the regime the mean exists to "
                + "detect, and must still be refused.");
        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitMeanForTest,
            Is.EqualTo(PoisonedMean),
            "a mean inside the horizon must be left intact.");
    }

    /// <summary>
    /// The horizon is the configured maximum itself, applied on both sides of
    /// the boundary.
    /// </summary>
    /// <remarks>
    /// No new option is introduced, deliberately. The mean's entire claim is
    /// that waits are exceeding
    /// <c>WalReplayPermitMaxQueueWait</c>; a claim no wait has tested for longer
    /// than the bound it is measured against has stopped being a measurement of
    /// anything. A second knob would only give an operator a way to restore the
    /// latch.
    /// </remarks>
    [Test]
    public void The_freshness_horizon_is_the_configured_maximum()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            PoisonedMean,
            sinceLastProgress: TimeSpan.Zero,
            sinceLastSample: MaxQueueWait - TimeSpan.FromMilliseconds(500));
        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "just inside the horizon the mean is still evidence.");

        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            PoisonedMean,
            sinceLastProgress: TimeSpan.Zero,
            sinceLastSample: MaxQueueWait + TimeSpan.FromMilliseconds(500));
        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.False,
            "just outside it the mean is a fossil.");
    }

    /// <summary>
    /// Nothing the stall arm detects is lost when the mean expires: a wedge
    /// produces no terminating waits at all, which is precisely the state that
    /// makes the mean stale, and the stall arm covers it directly.
    /// </summary>
    /// <remarks>
    /// This is the arm that stops the fix above from becoming a hole. The two
    /// arms are disjoined, so silencing one is only safe if the other is awake
    /// in exactly the regime the silenced one would have caught - and a wedged
    /// queue is the one case where "no sample for longer than the maximum" and
    /// "the harm is happening right now" coincide.
    /// </remarks>
    [Test]
    public void A_wedged_queue_is_still_refused_once_its_mean_has_expired()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);

        // The mean is a fossil for the worst possible reason: nothing has
        // terminated, because every permit is held by a wedged replay.
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            PoisonedMean,
            sinceLastProgress: null,
            sinceLastSample: MaxQueueWait + TimeSpan.FromMinutes(10));

        // The queueing epoch began longer ago than the maximum and never
        // reached zero, which is what separates a wedge from an idle period.
        BPlusLeafGrain.NoteReplayPermitArrivalForTest(
            epochAge: MaxQueueWait + TimeSpan.FromSeconds(1));

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "expiring the mean must not blind the gate to a wedge; the stall arm owns that state.");
    }

    /// <summary>
    /// The sample stamp is written by the <b>production</b> fold, for an
    /// acquisition and a cancellation alike, because both fold into the mean.
    /// </summary>
    /// <remarks>
    /// Asserted against the production path rather than the seeding seam on
    /// purpose. A fixture that only ever stamped the sample itself would keep
    /// passing with the stamp deleted from the fold, which is the precise shape
    /// of a test that proves nothing - and the arm it guards would silently
    /// expire on every single decision.
    /// </remarks>
    [Test]
    public void The_production_fold_stamps_the_sample_for_both_outcomes()
    {
        foreach (var acquired in new[] { true, false })
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
            Assert.That(
                BPlusLeafGrain.ReplayPermitWaitSampleAgeForTest,
                Is.Null,
                "precondition: a reset gate has no sample standing behind its mean.");

            BPlusLeafGrain.NoteReplayPermitQueueWaitForTest(TimeSpan.FromSeconds(40), acquired);

            Assert.That(
                BPlusLeafGrain.ReplayPermitWaitSampleAgeForTest,
                Is.Not.Null.And.LessThan(MaxQueueWait),
                $"a terminated wait (acquired: {acquired}) folds into the mean and must stamp it fresh, "
                    + "or the arm it feeds expires on the very next decision.");
        }
    }

    /// <summary>
    /// Resetting the gate clears the stamp along with the mean it dates, so a
    /// fixture cannot inherit a neighbour's freshness.
    /// </summary>
    [Test]
    public void Resetting_the_gate_clears_the_sample_stamp()
    {
        BPlusLeafGrain.NoteReplayPermitQueueWaitForTest(TimeSpan.FromSeconds(40), acquired: true);
        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitSampleAgeForTest,
            Is.Not.Null,
            "precondition: the fold stamped a sample.");

        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitSampleAgeForTest,
            Is.Null,
            "the stamp dates the mean, so it must be cleared with it.");
        Assert.That(BPlusLeafGrain.ReplayPermitWaitMeanForTest, Is.EqualTo(TimeSpan.Zero));
    }

    /// <summary>
    /// A fixture that says nothing about freshness gets a fresh sample, so every
    /// test written before the mean had an expiry keeps proving exactly what it
    /// proved.
    /// </summary>
    [Test]
    public void Seeding_without_a_freshness_argument_yields_a_fresh_sample()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(PoisonedMean, sinceLastProgress: TimeSpan.Zero);

        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitSampleAgeForTest,
            Is.Not.Null.And.LessThan(MaxQueueWait),
            "the default must stamp the sample now.");
        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(MaxQueueWait),
            Is.True,
            "so a seeded high mean still refuses, as it did before the expiry existed.");
    }

    /// <summary>
    /// Expiry does not resurrect the disabled state: a non-positive maximum
    /// still short-circuits before any of it runs.
    /// </summary>
    [Test]
    public void A_non_positive_maximum_still_disables_the_half_without_expiring_the_mean()
    {
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            PoisonedMean,
            sinceLastProgress: TimeSpan.Zero,
            sinceLastSample: MaxQueueWait + TimeSpan.FromMinutes(10));

        Assert.That(
            BPlusLeafGrain.IsReplayPermitQueueNotDraining(TimeSpan.Zero),
            Is.False,
            "zero must disable the drain half.");
        Assert.That(
            BPlusLeafGrain.ReplayPermitWaitMeanForTest,
            Is.EqualTo(PoisonedMean),
            "and must not mutate gate state on its way out; the half is off, not running.");
    }
}

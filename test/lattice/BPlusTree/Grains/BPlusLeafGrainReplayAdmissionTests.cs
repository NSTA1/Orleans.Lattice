using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for WAL replay permit <b>admission control</b> (issue #3284).
/// <para>
/// <b>What this fixture exists to establish.</b> The permit gate was correctly
/// sized and had no policy whatsoever on the queue in front of it, so an
/// arbitrary number of activations could enrol behind a ceiling of six - 87 were
/// measured - and every one of them was going to burn its request deadline while
/// queued and then enqueue a replacement. The gate's own sizing is not the
/// defect and is not touched here; the absence of a bound on who may join the
/// queue is.
/// </para>
/// <para>
/// <b>Why the two classes are asserted against each other and not only against
/// the bound.</b> A cap that refused both classes at the same depth would refuse
/// the foreground read as readily as the O(corpus) walk that filled the queue,
/// and the walk would win the race more often because it arrives in bulk.
/// Reserving the last <c>ceiling</c> slots for interactive work is what makes
/// this a priority rather than a cap, so the ordering relation between the two
/// bounds is the property, not either bound's value.
/// </para>
/// <para>
/// Every test here perturbs the process-wide gate statics, so each restores them
/// and the fixture is <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class BPlusLeafGrainReplayAdmissionTests
{
    [TearDown]
    public void ResetGate() => BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

    /// <summary>
    /// The admitted-waiter queue is bounded, and the bound is derived from the
    /// resolved ceiling rather than configured as an absolute depth.
    /// </summary>
    /// <remarks>
    /// Asserted at two different ceilings on purpose. A bound that happened to
    /// equal the right number at one ceiling but did not scale with it would pass
    /// a single-ceiling assertion while being a hard-coded constant - which is
    /// exactly the shape that would need retuning between a 2-vCPU box and a
    /// 64-vCPU one, and so would in practice be left wrong on one of them.
    /// </remarks>
    [Test]
    public void Admission_bound_scales_with_the_resolved_permit_ceiling()
    {
        const int DepthPerPermit = 4;

        foreach (var ceiling in new[] { 2, 6, 32 })
        {
            var expected = ceiling * DepthPerPermit;

            // One below the bound is admitted; at the bound it is refused.
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, expected - 1);
            Assert.That(
                BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                    DepthPerPermit, LatticeReplayAdmissionClass.Interactive, out _, out var bound),
                Is.True,
                $"ceiling {ceiling}: a waiter below the bound must be admitted.");
            Assert.That(bound, Is.EqualTo(expected), $"ceiling {ceiling}: bound must scale with the ceiling.");

            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, expected);
            Assert.That(
                BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                    DepthPerPermit, LatticeReplayAdmissionClass.Interactive, out var queued, out _),
                Is.False,
                $"ceiling {ceiling}: a waiter at the bound must be refused.");
            Assert.That(queued, Is.EqualTo(expected), "the refusal must report the count it judged.");
        }
    }

    /// <summary>
    /// A bulk caller is refused strictly earlier than an interactive one, and the
    /// reserved margin is exactly one ceiling's worth of queue.
    /// </summary>
    [Test]
    public void Bulk_is_refused_a_whole_ceiling_of_queue_before_interactive_is()
    {
        const int Ceiling = 6;
        const int DepthPerPermit = 4;

        BPlusLeafGrain.SeedReplayAdmissionStateForTest(Ceiling, 0);
        BPlusLeafGrain.TryAdmitReplayPermitWaiter(
            DepthPerPermit, LatticeReplayAdmissionClass.Interactive, out _, out var interactiveBound);
        BPlusLeafGrain.TryAdmitReplayPermitWaiter(
            DepthPerPermit, LatticeReplayAdmissionClass.Bulk, out _, out var bulkBound);

        Assert.That(
            interactiveBound - bulkBound,
            Is.EqualTo(Ceiling),
            "interactive work must keep a full ceiling's worth of queue that bulk cannot occupy.");

        // At the bulk bound: bulk refused, interactive still admitted. That gap is
        // the whole property - a cap both classes shared would fail here.
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(Ceiling, bulkBound);
        Assert.That(
            BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                DepthPerPermit, LatticeReplayAdmissionClass.Bulk, out _, out _),
            Is.False,
            "bulk must be refused at its own lower bound.");
        Assert.That(
            BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                DepthPerPermit, LatticeReplayAdmissionClass.Interactive, out _, out _),
            Is.True,
            "interactive must still be admitted in the queue reserved for it.");
    }

    /// <summary>
    /// Bulk is never excluded outright, however narrow the configured depth.
    /// </summary>
    /// <remarks>
    /// At a depth of one the reserved margin would consume the entire bound, and a
    /// bulk bound of zero refuses every background walk unconditionally - which
    /// would not be back-pressure but a permanent outage of the ANN open, in the
    /// configuration an operator reaches for precisely when they want the tightest
    /// bound. The floor is what keeps the narrow configuration usable.
    /// </remarks>
    [Test]
    public void Bulk_retains_at_least_one_admitted_slot_at_the_narrowest_depth()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);

        Assert.That(
            BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                queueDepthPerPermit: 1, LatticeReplayAdmissionClass.Bulk, out _, out var bulkBound),
            Is.True,
            "a bulk caller must still be admitted into an empty queue at depth one.");
        Assert.That(bulkBound, Is.GreaterThanOrEqualTo(1), "the bulk bound must never floor to zero.");
    }

    /// <summary>
    /// A zero depth restores the historical unbounded queue exactly, so the change
    /// can be turned off in the field without redeploying.
    /// </summary>
    [Test]
    public void A_zero_depth_admits_an_unbounded_queue()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 10_000);

        foreach (var admissionClass in new[]
                 {
                     LatticeReplayAdmissionClass.Interactive, LatticeReplayAdmissionClass.Bulk,
                 })
        {
            Assert.That(
                BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                    queueDepthPerPermit: 0, admissionClass, out _, out var bound),
                Is.True,
                $"{admissionClass}: a zero depth must admit unconditionally.");
            Assert.That(bound, Is.Zero, "an unbounded queue must report no bound rather than a large one.");
        }
    }

    /// <summary>
    /// An unsized gate admits unconditionally, because refusing the activation
    /// that is about to size it would be a deadlock.
    /// </summary>
    [Test]
    public void An_unsized_gate_admits_unconditionally()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 0, queued: 1_000);

        Assert.That(
            BPlusLeafGrain.TryAdmitReplayPermitWaiter(
                queueDepthPerPermit: 4, LatticeReplayAdmissionClass.Interactive, out _, out var bound),
            Is.True,
            "an unsized gate has no backlog to refuse.");
        Assert.That(bound, Is.Zero);
    }

    /// <summary>
    /// The ambient class defaults to interactive and is restored on dispose, so a
    /// bulk scope cannot leak onto an unrelated caller on the same context.
    /// </summary>
    /// <remarks>
    /// Leakage here would be silent and would present as an unrelated foreground
    /// read being refused a permit it was entitled to - a failure attributed to
    /// saturation rather than to classification, which is the hardest kind to
    /// trace back.
    /// </remarks>
    [Test]
    public void The_ambient_admission_class_defaults_to_interactive_and_is_restored()
    {
        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive),
            "absent any scope, a caller must be treated as interactive.");

        using (LatticeReplayAdmissionContext.BeginBulkScope())
        {
            Assert.That(LatticeReplayAdmissionContext.Current, Is.EqualTo(LatticeReplayAdmissionClass.Bulk));

            // Nested scopes must not corrupt the restore.
            using (LatticeReplayAdmissionContext.BeginBulkScope())
            {
                Assert.That(LatticeReplayAdmissionContext.Current, Is.EqualTo(LatticeReplayAdmissionClass.Bulk));
            }

            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Bulk),
                "an inner scope's dispose must restore the outer scope, not the default.");
        }

        Assert.That(
            LatticeReplayAdmissionContext.Current,
            Is.EqualTo(LatticeReplayAdmissionClass.Interactive),
            "the class must be restored when the outermost scope ends.");
    }
}

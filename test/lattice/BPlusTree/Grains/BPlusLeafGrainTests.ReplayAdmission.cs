using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// End-to-end coverage for the WAL replay permit admission control of issue
/// #3284, through a real activation rather than through the bound in isolation.
/// <para>
/// <b>Why both halves are needed.</b>
/// <see cref="BPlusLeafGrainReplayAdmissionTests"/> pins the arithmetic of the
/// bound: it is a pure function of the resolved ceiling, the configured depth,
/// and the caller's class, and it is tested as one. That fixture is satisfied by
/// a build in which the bound is computed correctly and then never consulted,
/// which is the whole defect restated. These tests close that gap by driving an
/// activation to the seam and asserting on what came out of it - a typed,
/// retryable refusal rather than an unbounded enqueue, counted under its own
/// reason so the refusal is legible in the same instrument that reports the
/// cancellation it replaces.
/// </para>
/// <para>
/// Every test here perturbs the process-wide admission statics, so each restores
/// the queued count it seeded and is <see cref="NonParallelizableAttribute"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Seeds the admitted-waiter count against the gate's real resolved ceiling
    /// and returns that ceiling, so a test states its intent in terms of the
    /// bound rather than a magic number.
    /// </summary>
    /// <remarks>
    /// <b>Also seeds a stalled queue (issue #3290).</b> Refusal is now a
    /// conjunction: the queue must be past the depth bound <em>and</em> failing
    /// to drain. Depth alone was never sufficient evidence of harm - it is
    /// derived from <c>ceiling * depthPerPermit</c>, where the ceiling is a
    /// supply-side CPU quantity and the queue is filled by demand-side fan-out,
    /// so the same healthy activation storm was admitted on a 16-processor host
    /// and refused on a 4-processor one. Seeding both halves is what keeps these
    /// tests aimed at their actual subject, which is that a refusal is typed,
    /// counted, and does not inflate the waiter count - not at which predicate
    /// produced it. The one test here that asserts <em>admission</em> gets the
    /// same stalled queue and is still admitted, which is the useful converse:
    /// a stalled queue alone does not refuse either.
    /// </remarks>
    private static async Task<int> SeedAdmittedWaitersAsync(Func<int, int> queuedForCeiling)
    {
        await QuiescentReplayGateAsync();
        var ceiling = BPlusLeafGrain.ReplayConcurrencyCeilingForTest;

        Assert.That(ceiling, Is.GreaterThan(0),
            "instrument validation: the gate must be sized, or the admission bound is the "
            + "deadlock guard rather than the bound under test and every activation is admitted");

        BPlusLeafGrain.SeedReplayAdmissionStateForTest(
            ceiling, queuedForCeiling(ceiling));
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            new LatticeOptions().WalReplayPermitMaxQueueWait,
            sinceLastAcquisition: TimeSpan.Zero);
        return ceiling;
    }

    /// <summary>
    /// Returns the seeded waiter count to zero and clears the seeded stall,
    /// leaving the sized gate in place, so a following fixture does not start
    /// against a fabricated queue.
    /// </summary>
    private static void ClearSeededAdmittedWaiters()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(
            BPlusLeafGrain.ReplayConcurrencyCeilingForTest, 0);
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastAcquisition: null);
    }

    [Test]
    [NonParallelizable]
    public async Task A_bulk_activation_past_the_bound_is_refused_rather_than_queued()
    {
        // The defect, stated as a test. Before the admission gate there was no
        // arrival this activation could make that was not an enqueue: it joined
        // a queue 87 deep against a ceiling of 6, burned its request deadline
        // without reaching the head, and the retry enqueued a replacement -
        // which is why the wedge was regenerative rather than merely slow.
        var options = new LatticeOptions();
        var ceiling = await SeedAdmittedWaitersAsync(
            c => c * options.WalReplayPermitQueueDepthPerPermit);

        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = UniqueReplayPermitTree();

            using (LatticeReplayAdmissionContext.BeginBulkScope())
            {
                var refusal = Assert.ThrowsAsync<LatticeSaturatedException>(
                    async () => await LeafActivationHarness.ActivateAsync(
                        (IGrainBase)grain, CancellationToken.None),
                    "a bulk caller arriving past the bound must be refused, not enqueued");

                Assert.That(refusal!.Message, Does.Contain(ceiling.ToString()),
                    "the refusal must name the ceiling it was measured against, or an operator "
                    + "reading it cannot tell a saturated silo from a misconfigured depth");
            }
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }

        Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest,
            Is.EqualTo(0),
            "a refused activation must not have incremented the admitted-waiter count - "
            + "counting refusals as waiters would make the bound tighten under its own refusals");
    }

    [Test]
    [NonParallelizable]
    public async Task A_refused_activation_is_counted_under_its_own_reason()
    {
        // The refusal replaces a cancellation, so without its own reason value it
        // would land on `faulted` and be indistinguishable from a replay that
        // threw - or, worse, be read as an increase in genuine faults caused by
        // the very change that removed them.
        var options = new LatticeOptions();
        await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit);

        ConcurrentBag<(long Value, KeyValuePair<string, object?>[] Tags)> records;
        var treeId = UniqueReplayPermitTree();

        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = treeId;

            records = CaptureActivationFailures(out var listener);
            using (listener)
            using (LatticeReplayAdmissionContext.BeginBulkScope())
            {
                Assert.ThrowsAsync<LatticeSaturatedException>(
                    async () => await LeafActivationHarness.ActivateAsync(
                        (IGrainBase)grain, CancellationToken.None));
            }
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }

        Assert.That(records, Is.Not.Empty,
            "instrument validation: the listener must have observed the failure, or the "
            + "assertion below reads a tag off a measurement that never happened");

        var reasons = records
            .SelectMany(r => r.Tags)
            .Where(t => t.Key == LatticeMetrics.TagReason)
            .Select(t => (string?)t.Value)
            .ToArray();

        Assert.That(reasons, Does.Contain("refused_replay_admission"),
            $"the refusal must carry its own reason. Observed reasons: [{string.Join(", ", reasons)}]");

        Assert.That(reasons, Does.Not.Contain("canceled_awaiting_permit"),
            "a refusal never reached the queue, so reporting it as a cancellation there would "
            + "inflate exactly the population this change exists to shrink");
    }

    [Test]
    [NonParallelizable]
    public async Task An_interactive_activation_is_still_admitted_where_bulk_is_refused()
    {
        // The reservation, proved end to end rather than arithmetically. A bound
        // that refused both classes at the same depth would be a global cap, and
        // a global cap starves the interactive reads the silo exists to serve at
        // precisely the moment a bulk walk is saturating the gate.
        var options = new LatticeOptions();
        var ceiling = await SeedAdmittedWaitersAsync(
            c => c * options.WalReplayPermitQueueDepthPerPermit - c);

        try
        {
            Assert.That(
                LatticeReplayAdmissionContext.Current,
                Is.EqualTo(LatticeReplayAdmissionClass.Interactive),
                "instrument validation: this test must run outside any bulk scope, or it "
                + "measures the bulk bound twice and the reservation is never exercised");

            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = UniqueReplayPermitTree();

            Assert.DoesNotThrowAsync(
                async () => await LeafActivationHarness.ActivateAsync(
                    (IGrainBase)grain, CancellationToken.None),
                $"an interactive caller must still be admitted at a depth of {ceiling * options.WalReplayPermitQueueDepthPerPermit - ceiling} "
                + "admitted waiters, which is exactly where a bulk caller is refused");
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }
    }

    /// <summary>
    /// A queue past the depth bound that is nonetheless draining admits the
    /// activation. Issue #3290, end to end.
    /// </summary>
    /// <remarks>
    /// This is the defect of issue #3290 stated as a test, at the same seam as
    /// the two refusal tests above and deliberately adjacent to them: the depth
    /// they seed is identical, and only the drain evidence differs. Without the
    /// demand-side half, this activation is refused with
    /// <c>LatticeSaturatedException</c> - which is exactly what was happening to
    /// the system registry tree on a 4-processor CI runner, where a bound of 16
    /// met an ordinary fan-out that offered 40 waiters and drained them in
    /// milliseconds. The depth bound's inputs contain no arrival term and no
    /// latency term, so depth alone could not tell that queue apart from the
    /// 87-deep regenerative backlog of issue #3284.
    /// </remarks>
    [Test]
    [NonParallelizable]
    public async Task An_activation_past_the_bound_is_admitted_while_the_queue_is_draining()
    {
        var options = new LatticeOptions();
        await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit);

        try
        {
            // The only difference from the refusal tests above: the queue is
            // draining, so there is no evidence of the harm being guarded against.
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                TimeSpan.FromMilliseconds(2), sinceLastAcquisition: TimeSpan.FromMilliseconds(5));

            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = UniqueReplayPermitTree();

            Assert.DoesNotThrowAsync(
                async () => await LeafActivationHarness.ActivateAsync(
                    (IGrainBase)grain, CancellationToken.None),
                "a queue past the depth bound that is draining in milliseconds is healthy "
                + "fan-out, and refusing it fails load that would have succeeded");
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }
    }
}

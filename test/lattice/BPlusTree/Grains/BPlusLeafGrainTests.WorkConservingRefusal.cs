using System.Collections.Concurrent;
using System.Diagnostics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Work-conserving replay admission (issue #5992, A1): a wait-arm reading never
/// refuses an arrival while a replay permit is free and no waiter is parked
/// ahead of it, and the reading itself expires once its samples age out.
/// Refinement: <c>WorkConservingRefusal</c> and the <c>RefuseWait</c> action of
/// <c>spec/scheduling/ReplayPermits</c>.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task A_wait_exceeded_reading_never_refuses_while_a_permit_is_free_and_no_waiter_is_ahead()
    {
        // A wait_exceeded reading (a fresh mean at the bound) over a queue whose
        // waiters have all been served: the admitted-waiter count still reads past
        // the bound, but every permit is free and none is parked. Refusing here
        // sheds nothing, since the caller retries, and leaves a permit idle.
        var options = new LatticeOptions();
        await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit, holdPermits: false);
        var treeId = UniqueReplayPermitTree();
        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        try
        {
            Assert.That(BPlusLeafGrain.ReplayConcurrencyGateForTest!.CurrentCount,
                Is.EqualTo(BPlusLeafGrain.ReplayConcurrencyCeilingForTest),
                "instrument validation: every permit is free, so no waiter is parked");
            Assert.That(
                BPlusLeafGrain.ClassifyReplayPermitQueueDrainWithoutSideEffects(options.WalReplayPermitMaxQueueWait, out _),
                Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.WaitExceeded),
                "instrument validation: the reading alone would refuse");

            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = treeId;

            using (ListenForSaturationRefusals(treeId, refusals))
            {
                Assert.DoesNotThrowAsync(
                    async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None),
                    "a free permit with no waiter ahead admits whatever the wait arm reads");
            }

            Assert.Multiple(() =>
            {
                Assert.That(grain.ReplayAdmissionPhaseForTest, Is.EqualTo(BPlusLeafGrain.ReplayAdmissionPhase.HoldsPermit));
                Assert.That(refusals, Is.Empty);
            });
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }
    }

    [Test]
    [NonParallelizable]
    public void The_replay_gate_reset_clears_every_reading_admission_judges_by()
    {
        // Every input of the admission decision is process-wide, so a reading one
        // test leaves behind could flip another test's verdict depending on suite
        // order. The reset every gate-touching fixture calls must clear them all.
        var maxQueueWait = new LatticeOptions().WalReplayPermitMaxQueueWait;
        try
        {
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 4, queued: 40);
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                maxQueueWait,
                sinceLastProgress: maxQueueWait + TimeSpan.FromSeconds(1),
                sinceLastSample: TimeSpan.Zero,
                sinceHolderProgress: TimeSpan.Zero);
            Assert.That(
                BPlusLeafGrain.ClassifyReplayPermitQueueDrainWithoutSideEffects(maxQueueWait, out _),
                Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.WaitExceeded),
                "instrument validation: the seeded reading refuses");

            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

            Assert.Multiple(() =>
            {
                Assert.That(
                    BPlusLeafGrain.ClassifyReplayPermitQueueDrainWithoutSideEffects(maxQueueWait, out var sinceProgress),
                    Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.Draining));
                Assert.That(sinceProgress, Is.EqualTo(TimeSpan.Zero), "no progress stamp survives");
                Assert.That(BPlusLeafGrain.ReplayPermitWaitMeanForTest, Is.EqualTo(TimeSpan.Zero));
                Assert.That(BPlusLeafGrain.ReplayPermitWaitSampleTimestampForTest, Is.Zero);
                Assert.That(BPlusLeafGrain.ReplayHolderProgressAgeForTest, Is.Null);
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.Zero);
                Assert.That(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, Is.Zero);
                Assert.That(BPlusLeafGrain.ReplayConcurrencyGateForTest, Is.Null);
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public void A_wait_exceeded_reading_expires_once_its_samples_age_out()
    {
        // Judged on an injected clock, in Stopwatch units, so the verdict depends on
        // the samples' age and never on how long the test takes or the platform's
        // tick rate.
        var maxQueueWait = new LatticeOptions().WalReplayPermitMaxQueueWait;
        try
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(maxQueueWait, sinceLastProgress: null, sinceLastSample: TimeSpan.Zero);
            var sampledAt = BPlusLeafGrain.ReplayPermitWaitSampleTimestampForTest;
            Assert.That(sampledAt, Is.Not.Zero, "instrument validation: the reading has a sample");

            static long After(long start, TimeSpan elapsed) => start + (long)(elapsed.TotalSeconds * Stopwatch.Frequency);

            Assert.Multiple(() =>
            {
                Assert.That(
                    BPlusLeafGrain.ClassifyReplayPermitQueueDrainAt(maxQueueWait, After(sampledAt, maxQueueWait / 2), out _),
                    Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.WaitExceeded),
                    "a sample younger than the bound still reads wait_exceeded");
                Assert.That(
                    BPlusLeafGrain.ClassifyReplayPermitQueueDrainAt(maxQueueWait, After(sampledAt, maxQueueWait * 2), out _),
                    Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.Draining),
                    "once the sample has aged past the bound the reading expires and refuses nothing");
            });
        }
        finally
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: null);
        }
    }
}

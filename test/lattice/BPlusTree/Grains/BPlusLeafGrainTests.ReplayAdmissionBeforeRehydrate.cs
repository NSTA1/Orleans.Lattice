using System.Collections.Concurrent;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the replay admission changes of issue #5877.
/// <para>
/// <b>Admission before the rehydrate (E1).</b> On the Layer 3 cold-restart rig
/// every refused replay had already spent a 9-37 s snapshot rehydrate, because
/// the admission decision was made only at permit acquisition, after it. The
/// refused leaf's barrier then re-armed and paid the rehydrate again.
/// </para>
/// <para>
/// <b>Slow holders are not stalled holders (E2).</b> The four permit holders were
/// each advancing a WAL slice every few seconds, yet every refusal said "the
/// permits already issued are not coming back". The classifier now tells the two
/// apart on a holder-progress stamp; admission itself is unchanged.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Seeds a queue past the interactive bound with no acquisition for longer
    /// than the maximum queue wait, and a zero smoothed wait, so only the
    /// depth-gated hold-time arms can refuse.
    /// </summary>
    private static async Task<(int Ceiling, LatticeOptions Options)> SeedStalledQueuePastTheBoundAsync(
        TimeSpan? sinceHolderProgress)
    {
        var options = new LatticeOptions();
        var ceiling = await SeedAdmittedWaitersAsync(c => c * options.WalReplayPermitQueueDepthPerPermit);
        BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
            TimeSpan.Zero,
            sinceLastProgress: options.WalReplayPermitMaxQueueWait + TimeSpan.FromSeconds(1),
            sinceHolderProgress: sinceHolderProgress);
        return (ceiling, options);
    }

    [Test]
    [NonParallelizable]
    public async Task A_rearmed_replay_that_admission_would_refuse_is_refused_before_the_snapshot_rehydrate()
    {
        await SeedStalledQueuePastTheBoundAsync(sinceHolderProgress: null);
        var treeId = UniqueReplayPermitTree();
        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        try
        {
            var (grain, state, snapshot, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = treeId;
            int Loads() => snapshot.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILeafSnapshotStorageGrain.LoadAsync));

            using (ListenForSaturationRefusals(treeId, refusals))
            {
                // The first replay of an activation resolves its options inside
                // the rehydrate, so it is judged at acquisition, as before.
                Assert.ThrowsAsync<LatticeSaturatedException>(
                    async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));
                Assert.That(Loads(), Is.EqualTo(1), "instrument validation: the first attempt rehydrates");

                // The harness awaited the barrier directly, so the first request
                // joins the faulted barrier and disarms it; the next request
                // re-arms a fresh replay. On the #5877 rig every such re-armed
                // replay paid a 9-37 s rehydrate only to be refused again.
                Assert.ThrowsAsync<LatticeSaturatedException>(async () => await grain.GetAsync("k"));
                Assert.That(refusals, Has.Count.EqualTo(1), "instrument validation: joining the faulted barrier runs no replay");

                // A stale mean the pre-check must leave in place: it writes
                // nothing, so the acquisition after it decides on the state it
                // would have seen without the pre-check.
                var maxQueueWait = new LatticeOptions().WalReplayPermitMaxQueueWait;
                BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                    maxQueueWait,
                    sinceLastProgress: maxQueueWait + TimeSpan.FromSeconds(1),
                    sinceLastSample: maxQueueWait + TimeSpan.FromSeconds(1));

                var rearmed = Assert.ThrowsAsync<LatticeSaturatedException>(
                    async () => await grain.GetAsync("k"));
                Assert.That(rearmed!.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
                Assert.That(BPlusLeafGrain.ReplayPermitWaitMeanForTest, Is.EqualTo(maxQueueWait),
                    "the pre-check must not discard the stale mean; only the acquisition may");
            }

            Assert.Multiple(() =>
            {
                Assert.That(refusals, Has.Count.EqualTo(2),
                    "instrument validation: the second request must have run a fresh replay attempt");
                Assert.That(Loads(), Is.EqualTo(1),
                    "a re-armed replay that admission refuses must not spend another snapshot rehydrate (issue #5877)");
                Assert.That(grain.ReplayAdmissionPhaseForTest,
                    Is.EqualTo(BPlusLeafGrain.ReplayAdmissionPhase.RefusedAdmission));
            });
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }

        Assert.That(refusals.Select(ArmOf), Is.All.EqualTo("no_progress"));
    }

    [Test]
    [NonParallelizable]
    public async Task An_admitted_replay_still_rehydrates_from_its_snapshot()
    {
        await QuiescentReplayGateAsync();
        var (grain, state, snapshot, _) = CreateGrainWithSnapshotAndCoordinator(
            preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
        state.State.TreeId = UniqueReplayPermitTree();

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await snapshot.Received(1).LoadAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    [NonParallelizable]
    public async Task A_refusal_while_permit_holders_advance_is_attributed_to_slow_holders()
    {
        await SeedStalledQueuePastTheBoundAsync(sinceHolderProgress: TimeSpan.Zero);
        var treeId = UniqueReplayPermitTree();
        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        LatticeSaturatedException? refusal;
        try
        {
            var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
            state.State.TreeId = treeId;

            using (ListenForSaturationRefusals(treeId, refusals))
            {
                refusal = Assert.ThrowsAsync<LatticeSaturatedException>(
                    async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None),
                    "slow holders past the bound are refused exactly as stalled ones are; only the attribution differs");
            }
        }
        finally
        {
            ClearSeededAdmittedWaiters();
        }

        Assert.Multiple(() =>
        {
            Assert.That(refusals, Has.Count.EqualTo(1));
            Assert.That(ArmOf(refusals.Single()), Is.EqualTo("holders_slow"));
            Assert.That(refusal!.Message, Does.Contain("still advancing"));
            Assert.That(refusal.Message, Does.Not.Contain("not coming back"));
        });
    }

    [Test]
    [NonParallelizable]
    public void ClassifyReplayPermitQueueDrain_tells_slow_holders_from_stalled_ones()
    {
        var bound = TimeSpan.FromSeconds(5);
        try
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                TimeSpan.Zero, sinceLastProgress: bound + TimeSpan.FromSeconds(3), sinceHolderProgress: TimeSpan.Zero);
            var slow = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(bound, out _);

            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                TimeSpan.Zero, sinceLastProgress: bound + TimeSpan.FromSeconds(3),
                sinceHolderProgress: bound + TimeSpan.FromSeconds(1));
            var staleHolders = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(bound, out _);

            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                TimeSpan.Zero, sinceLastProgress: bound + TimeSpan.FromSeconds(3), sinceHolderProgress: null);
            var noHolders = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(bound, out _);

            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(
                TimeSpan.Zero, sinceLastProgress: TimeSpan.FromMilliseconds(5), sinceHolderProgress: TimeSpan.Zero);
            var draining = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(bound, out _);

            Assert.Multiple(() =>
            {
                Assert.That(slow, Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.HoldersSlow));
                Assert.That(staleHolders, Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.NoProgress));
                Assert.That(noHolders, Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.NoProgress));
                Assert.That(draining, Is.EqualTo(BPlusLeafGrain.ReplayPermitDrainVerdict.Draining),
                    "recent acquisitions are draining whatever the holders are doing");
                Assert.That(BPlusLeafGrain.IsReplayPermitQueueNotDraining(bound), Is.False);
            });
        }
        finally
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: null);
        }
    }

    [Test]
    [NonParallelizable]
    public void The_side_effect_free_classifier_agrees_with_the_expiring_one_and_writes_nothing()
    {
        var bound = TimeSpan.FromSeconds(5);
        var states = new (TimeSpan Mean, TimeSpan? SinceProgress, TimeSpan? SinceSample, TimeSpan? SinceHolder)[]
        {
            (bound, TimeSpan.Zero, TimeSpan.Zero, null),
            (bound, TimeSpan.Zero, bound + TimeSpan.FromSeconds(1), null),
            (bound, bound + TimeSpan.FromSeconds(2), bound + TimeSpan.FromSeconds(1), null),
            (TimeSpan.Zero, bound + TimeSpan.FromSeconds(2), TimeSpan.Zero, TimeSpan.Zero),
            (TimeSpan.FromMilliseconds(2), TimeSpan.FromMilliseconds(5), TimeSpan.Zero, null),
            (TimeSpan.Zero, null, null, null),
        };
        try
        {
            foreach (var (mean, sinceProgress, sinceSample, sinceHolder) in states)
            {
                BPlusLeafGrain.SeedReplayPermitWaitStateForTest(mean, sinceProgress, sinceSample, sinceHolder);
                var pure = BPlusLeafGrain.ClassifyReplayPermitQueueDrainWithoutSideEffects(bound, out _);
                var meanAfterPure = BPlusLeafGrain.ReplayPermitWaitMeanForTest;
                var expiring = BPlusLeafGrain.ClassifyReplayPermitQueueDrain(bound, out _);

                var context = $"mean={mean} sinceProgress={sinceProgress} sinceSample={sinceSample} sinceHolder={sinceHolder}";
                Assert.That(pure, Is.EqualTo(expiring), context);
                Assert.That(meanAfterPure, Is.EqualTo(mean), "the pre-check must leave the mean as it found it: " + context);
            }
        }
        finally
        {
            BPlusLeafGrain.SeedReplayPermitWaitStateForTest(TimeSpan.Zero, sinceLastProgress: null);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_replay_that_reads_a_slice_stamps_holder_progress()
    {
        await QuiescentReplayGateAsync();
        BPlusLeafGrain.SeedReplayHolderProgressForTest(null);
        try
        {
            var treeId = UniqueReplayPermitTree();
            var (grain, state, _, coordinator) = CreateGrainWithSnapshotAndCoordinator(
                preloadedSnapshot: null, persistedCheckpoint: -1, walHead: 2);
            state.State.TreeId = treeId;
            ReplaySliceStub.ServeBothOverloads(coordinator,
            [
                new CommitLogSliceEntry(0, new LatticeMutation { TreeId = treeId, Kind = MutationKind.Set, Key = "k0", Value = [1] }),
                new CommitLogSliceEntry(1, new LatticeMutation { TreeId = treeId, Kind = MutationKind.Set, Key = "k1", Value = [2] }),
            ]);

            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

            Assert.That(BPlusLeafGrain.ReplayHolderProgressAgeForTest, Is.Not.Null.And.LessThan(TimeSpan.FromSeconds(30)));
        }
        finally
        {
            BPlusLeafGrain.SeedReplayHolderProgressForTest(null);
        }
    }

    [Test]
    public void DescribeReplayAdmissionRefusal_holders_slow_names_the_capacity_condition()
    {
        var message = BPlusLeafGrain.DescribeReplayAdmissionRefusal(
            BPlusLeafGrain.ReplayPermitDrainVerdict.HoldersSlow,
            queued: 614,
            bound: 256,
            LatticeReplayAdmissionClass.Interactive,
            ceiling: 4,
            smoothedWait: TimeSpan.Zero,
            sinceLastProgress: TimeSpan.FromSeconds(195),
            maxQueueWait: TimeSpan.FromSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(message, Does.Contain("no permit has been released to the queue in 195 s"));
            Assert.That(message, Does.Contain("still advancing"));
            Assert.That(message, Does.Contain("capacity condition"));
            Assert.That(message, Does.Contain("orleans.lattice.wal.replay.permit_hold"));
            Assert.That(message, Does.Not.Contain("not coming back"));
            Assert.That(message, Does.Not.Contain("smoothed"));
        });
    }

    [Test]
    public void The_holders_slow_arm_carries_its_documented_key_and_value()
        => Assert.That(LatticeMetrics.SaturationArmHoldersSlow,
            Is.EqualTo(new KeyValuePair<string, object?>("arm", "holders_slow")));
}

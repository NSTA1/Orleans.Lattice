using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3575. The coverage-lag timer's starvation drive and the WAL GC
/// sweep's drew on one small process-wide share of replay permits, first come,
/// first served. The timer re-drives thousands of leaves in bursts, so it won:
/// the sweep - the only caller that lifts a pin holding a tree's cursor floor -
/// was refused about nine touches in ten, and the timer's own refusals escaped
/// the timer callback as an unhandled <see cref="LatticeSaturatedException"/>,
/// logged twice each by the runtime and counted nowhere.
/// <para>
/// These fixtures pin the replay gate's ceiling (and so the GC share's width)
/// by resetting the process-wide gate and letting the leaf's own activation
/// size it, so they behave the same on a two-core agent as on a sixty-four-core
/// one. The reservation arithmetic itself is covered without a grain by
/// <see cref="StarvationReplayAdmissionTests"/>.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// A four-permit gate: a two-slot GC share, of which a timer drive may hold
    /// one, so a refusal the timer receives leaves the sweep's slot free.
    /// </summary>
    private const int TimerAdmissionCeiling = 4;

    private const BPlusLeafGrain.StarvationDriveOrigin TimerDrive =
        BPlusLeafGrain.StarvationDriveOrigin.CoverageLagTimer;

    private const BPlusLeafGrain.StarvationDriveOrigin SweepDrive =
        BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep;

    private static readonly string TimerDriveRefusedReason =
        (string)LatticeMetrics.DriverDeclineRecheckDriveRefused.Value!;

    private static readonly string TimerDriveDeferredReason =
        (string)LatticeMetrics.DriverDeclineRecheckDriveDeferred.Value!;

    /// <summary>What one coverage-lag tick did with the drive it offered.</summary>
    private enum TimerDriveVerdict
    {
        Admitted,
        Refused,
        Deferred,
    }

    /// <summary>
    /// The defect fixture. A tick that routes a frozen leaf to the drive while
    /// the timer's part of the GC share is full must not throw, must count the
    /// refusal, and must leave the sweep's slot free.
    /// <para>
    /// RED pre-fix: the fourth tick throws the refusal out of
    /// <c>OnCoverageLagTimerTickAsync</c>, which is what the runtime logged as
    /// "Caught and ignored exception thrown from timer callback" about 14,000
    /// times an hour.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Coverage_lag_tick_absorbs_and_counts_a_starvation_drive_refused_admission()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var treeId = UniqueCoverageRepairTreeId("timer-drive-refused");
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            treeId: treeId,
            maxConcurrentReplays: TimerAdmissionCeiling);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldTimerShare(gate);
        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.ReplayConcurrencyCeilingForTest, Is.EqualTo(TimerAdmissionCeiling),
                    "precondition: the leaf's activation must have sized the gate to the pinned ceiling, "
                    + "or the share this fixture reasons about is not the one in force.");
                Assert.That(held, Is.EqualTo(1),
                    "precondition: at a two-slot share the timer may hold exactly one slot, and this "
                    + "fixture holds it, so the leaf's own timer drive has nothing left to take.");
            });

            var reasons = new List<string>();
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                for (var tick = 0; tick < TicksToReachStalledDrive; tick++)
                {
                    Assert.DoesNotThrowAsync(
                        () => grain.OnCoverageLagTimerTickAsync(CancellationToken.None),
                        "a refused admission is back-pressure the timer has no caller to hand to. Letting it "
                        + "escape the callback is what made it 95% of a deployment's warnings, with no metric.");
                }
            }

            Assert.Multiple(() =>
            {
                Assert.That(reasons, Does.Contain(LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value),
                    "input count: the ticks must have routed the frozen leaf to the drive, or the refusal "
                    + "assertion below is about a drive that was never requested.");
                Assert.That(reasons.Count(r => r == TimerDriveRefusedReason), Is.EqualTo(1),
                    "the refusal must be counted exactly once, on its own arm beside the routing arm.");
            });

            Assert.DoesNotThrowAsync(
                () => grain.DriveStarvedCheckpointAsync(),
                "the timer was refused its part of the share, not the whole of it: a drive through the grain "
                + "interface is the WAL GC sweep's, and the slot kept for it must still admit it. Before issue "
                + "#3575 the timer could hold every slot, and the sweep was refused about nine touches in ten.");
        }
        finally
        {
            ReleaseTimerShare(gate, held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    /// <summary>
    /// A refused timer drive backs off before asking again, by a count of drive
    /// opportunities that grows with consecutive refusals, and a drive that is
    /// admitted ends the backoff.
    /// <para>
    /// Driven on the never-checkpointed route (issue #3300), which offers the
    /// drive on every tick, so each tick is exactly one opportunity. The first
    /// refusal always defers one opportunity; the second defers two or three and
    /// the third four to seven, the spread within each being the per-leaf jitter.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_refused_timer_drive_backs_off_before_asking_again_and_an_admitted_drive_ends_the_backoff()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var treeId = UniqueCoverageRepairTreeId("timer-drive-backoff");
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            treeId: treeId,
            maxConcurrentReplays: TimerAdmissionCeiling);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.LessThan(0L),
            "precondition: no partition is checkpointed, so every tick offers the drive.");

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldTimerShare(gate);
        try
        {
            Assert.That(await TimerDriveVerdictAsync(grain, treeId), Is.EqualTo(TimerDriveVerdict.Refused),
                "precondition: with the timer's part of the share held, its drive is refused.");
            Assert.That(await TimerDriveVerdictAsync(grain, treeId), Is.EqualTo(TimerDriveVerdict.Deferred),
                "a refused leaf must not ask again at its very next opportunity: that is the re-drive "
                + "pattern that turned steady demand into 50-60 refusals a second.");
            Assert.That(await TimerDriveVerdictAsync(grain, treeId), Is.EqualTo(TimerDriveVerdict.Refused),
                "the first backoff is a single opportunity, after which the leaf asks again.");

            var deferred = await DeferredOpportunitiesUntilNextAskAsync(grain, treeId);
            Assert.That(deferred.Deferred, Is.InRange(2, 3),
                "a second consecutive refusal must double the backoff, plus up to as much again of jitter.");
            Assert.That(deferred.Next, Is.EqualTo(TimerDriveVerdict.Refused),
                "the share is still held, so the next ask is refused a third time.");

            ReleaseTimerShare(gate, held);
            held = 0;

            var drained = await DeferredOpportunitiesUntilNextAskAsync(grain, treeId);
            Assert.That(drained.Deferred, Is.InRange(4, 7),
                "the third refusal's backoff doubles again, and a released share does not cut it short.");
            Assert.That(drained.Next, Is.EqualTo(TimerDriveVerdict.Admitted),
                "once the share has room the leaf's retry must be admitted, or the backoff has latched the "
                + "timer's remedy off - trading a refusal storm for a leaf that is never repaired.");

            held = HoldTimerShare(gate);
            Assert.That(await TimerDriveVerdictAsync(grain, treeId), Is.EqualTo(TimerDriveVerdict.Refused));
            Assert.That(await TimerDriveVerdictAsync(grain, treeId), Is.EqualTo(TimerDriveVerdict.Deferred));
            Assert.That(await TimerDriveVerdictAsync(grain, treeId), Is.EqualTo(TimerDriveVerdict.Refused),
                "an admitted drive must end the backoff, so the next refusal is again the first and defers a "
                + "single opportunity rather than resuming at the length the earlier run had reached.");
        }
        finally
        {
            ReleaseTimerShare(gate, held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    /// <summary>
    /// End to end, with a WAL that has work in it: the one free slot of a
    /// two-slot share is the last, so the timer's drive is refused it and the
    /// sweep's drive then takes it and replays. The held slot stands in for
    /// another tree's sweep drive, which is the case a per-origin cap would get
    /// wrong: it would count only the timer's own slots and let it take the last.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_timer_drive_leaves_the_last_free_gc_slot_to_a_sweep_drive_that_then_replays_in_it()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var treeId = UniqueStarvationDriveTree();
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: treeId,
            persistedCheckpoint: -1,
            starvationDriveBudget: TimeSpan.FromSeconds(10),
            maxConcurrentReplays: TimerAdmissionCeiling);
        await ActivateAsync(grain);

        // Rows and no durable checkpoint, so every tick routes the leaf to the
        // drive (issue #3300). The WAL grows only after activation, so only a
        // drive can advance the checkpoint.
        SeedRow(grain);
        wal.GrowTo(3);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, SweepDrive), Is.True,
            "precondition: one slot of the two-slot share is held, as another tree's sweep drive would hold it.");
        try
        {
            Assert.That(await TimerDriveVerdictAsync(grain, treeId), Is.EqualTo(TimerDriveVerdict.Refused),
                "the free slot is the last one, so the timer must be refused it. Before issue #3575 it took "
                + "it, and at the timer's volume that left nothing for the sweep.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(-1L),
                "a refused drive must not have replayed anything.");

            await grain.DriveStarvedCheckpointAsync();

            Assert.Multiple(() =>
            {
                Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                    "the sweep's drive must be admitted to the slot the timer left free, and replay in it.");
                Assert.That(gate.CurrentCount, Is.EqualTo(TimerAdmissionCeiling - 1),
                    "and it must return its permit, leaving only the held one out.");
            });
        }
        finally
        {
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [TestCase(1, 1, 1)]
    [TestCase(2, 2, 3)]
    [TestCase(3, 4, 7)]
    [TestCase(4, 4, 7)]
    [TestCase(40, 4, 7)]
    public void ComputeTimerDriveDeferrals_doubles_per_refusal_up_to_its_cap_within_the_jitter_band(
        int consecutiveRefusals, int least, int most)
    {
        var observed = new HashSet<int>();
        for (var hash = -500; hash < 500; hash++)
        {
            var deferrals = BPlusLeafGrain.ComputeTimerDriveDeferrals(hash * 7919, consecutiveRefusals);
            Assert.That(deferrals, Is.InRange(least, most),
                $"hash {hash * 7919}: the backoff must stay inside its band, or a leaf is either retried "
                + "early or held off longer than the cap allows.");
            Assert.That(
                BPlusLeafGrain.ComputeTimerDriveDeferrals(hash * 7919, consecutiveRefusals),
                Is.EqualTo(deferrals),
                "a leaf's backoff is derived rather than drawn, so it must be reproducible.");
            observed.Add(deferrals);
        }

        Assert.That(observed, Has.Count.EqualTo(most - least + 1),
            "the jitter must actually spread leaves across the whole band. A backoff every leaf shares "
            + "would bring leaves refused together back together.");
    }

    /// <summary>
    /// Holds every slot of the GC share a timer drive may take, and returns how
    /// many it took.
    /// </summary>
    private static int HoldTimerShare(SemaphoreSlim gate)
    {
        var held = 0;
        while (BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, TimerDrive))
        {
            held++;
            Assert.That(held, Is.LessThanOrEqualTo(TimerAdmissionCeiling),
                "the timer's part of the share is bounded; an unbounded loop here means it is not.");
        }

        return held;
    }

    private static void ReleaseTimerShare(SemaphoreSlim gate, int held)
    {
        for (var i = 0; i < held; i++)
        {
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }
    }

    /// <summary>
    /// Runs one coverage-lag tick and reports what became of the drive it offered.
    /// </summary>
    private static async Task<TimerDriveVerdict> TimerDriveVerdictAsync(BPlusLeafGrain grain, string treeId)
    {
        var reasons = new List<string>();
        using (ListenForDriverDeclinesOnTree(treeId, reasons))
        {
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        }

        Assert.That(reasons, Does.Contain(LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Value),
            "input count: every tick here must route the leaf to the drive, or its verdict is about a tick "
            + "that never offered one.");

        if (reasons.Contains(TimerDriveRefusedReason))
        {
            return TimerDriveVerdict.Refused;
        }

        return reasons.Contains(TimerDriveDeferredReason) ? TimerDriveVerdict.Deferred : TimerDriveVerdict.Admitted;
    }

    /// <summary>
    /// Ticks until a tick asks for the drive again, and returns how many
    /// opportunities were deferred first and what that ask came to.
    /// </summary>
    private static async Task<(int Deferred, TimerDriveVerdict Next)> DeferredOpportunitiesUntilNextAskAsync(
        BPlusLeafGrain grain,
        string treeId)
    {
        var deferred = 0;
        while (true)
        {
            var verdict = await TimerDriveVerdictAsync(grain, treeId);
            if (verdict != TimerDriveVerdict.Deferred)
            {
                return (deferred, verdict);
            }

            Assert.That(++deferred, Is.LessThanOrEqualTo(1 << (BPlusLeafGrain.MaxTimerDriveDeferralDoublings + 1)),
                "the backoff never ended, so the timer's remedy has been latched off.");
        }
    }
}

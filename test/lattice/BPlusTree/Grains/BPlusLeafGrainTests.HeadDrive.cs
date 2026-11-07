using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The head drive (#5851 B, a5e2e2ef's BH1-BH5): main's #3389 stall arm, retriggered for a
/// live-written leaf. A live-advance step moves the checkpoint only to the leaf's own
/// high, so it no longer counts as progress; a leaf held below the WAL head by other
/// authors' records is driven to the head at the stall arm's cadence, a background drive
/// that steps aside while replays queue on the gate and stays owed.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// A pre-arm leaf on a growing WAL whose first write is banked, with the stall
    /// detector's baseline taken.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, GrowingWal Wal, LiveAdvanceRig Rig, string TreeId)> HeadDriveLeafAsync(string name)
    {
        var wal = new GrowingWal();
        wal.GrowTo(0);
        var rig = new LiveAdvanceRig(firstOffset: 1);
        rig.FenceGate.IsPreArmOpen = true;
        var treeId = UniqueCoverageRepairTreeId(name);
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: 0,
            treeId: treeId,
            maxConcurrentReplays: TimerAdmissionCeiling,
            replayCoordinator: wal.Coordinator,
            liveAdvance: rig);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await InitialiseOwnMarkerAsync(rig);
        await SetStampedAsync(grain, "w", "0");
        wal.GrowTo(rig.Writer.NextOffset(0) - 1);
        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        return (grain, wal, rig, treeId);
    }

    /// <summary>
    /// One tick on which the leaf writes once, live-advances over it, and other authors
    /// append <paramref name="foreign"/> records above it.
    /// </summary>
    private static async Task<bool> HeadDriveTickAsync(BPlusLeafGrain grain, GrowingWal wal, LiveAdvanceRig rig, int tick, int foreign)
    {
        await SetStampedAsync(grain, $"t{tick}", "v");
        for (var i = 0; i < foreign; i++)
        {
            rig.Writer.ForeignAppend();
        }

        wal.GrowTo(rig.Writer.NextOffset(0) - 1);
        wal.Coordinator.ClearReceivedCalls();
        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        return ReadTheWal(wal);
    }

    [Test]
    [NonParallelizable]
    public async Task HeadDrive_a_live_advancing_leaf_held_below_the_head_is_driven_to_it_at_the_stall_cadence()
    {
        // Detector 1 (BH1): every tick the live advance moves the checkpoint to the own high,
        // and other authors' records lie above it. The stall arm counts those ticks and
        // drives the leaf to the head.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, wal, rig, _) = await HeadDriveLeafAsync("head-drive");
            var drovenAt = -1;
            var liveMoved = false;
            for (var tick = 0; tick < 8 && drovenAt < 0; tick++)
            {
                var before = grain.GetCurrentCheckpointForPartition(0);
                if (await HeadDriveTickAsync(grain, wal, rig, tick, foreign: 3))
                {
                    drovenAt = tick;
                }
                else
                {
                    liveMoved |= grain.GetCurrentCheckpointForPartition(0) > before;
                }
            }

            Assert.Multiple(() =>
            {
                Assert.That(liveMoved, Is.True, "precondition: the live advance moved the checkpoint on a tick");
                Assert.That(drovenAt, Is.InRange(2, 7), "a live-advancing leaf held below the head was never driven, or was driven before the stall threshold");
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(wal.NewestOffset), "the drive did not read to the head");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task HeadDrive_a_leaf_its_live_advance_keeps_at_the_head_is_never_driven()
    {
        // Detector 2 (BH1): with no other author's record above it the live advance reaches
        // the head; the stall arm's head probe finds it caught up, and nothing replays.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, wal, rig, treeId) = await HeadDriveLeafAsync("head-drive-caught-up");
            var reasons = new List<string>();
            var drives = 0;
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                for (var tick = 0; tick < 8; tick++)
                {
                    drives += await HeadDriveTickAsync(grain, wal, rig, tick, foreign: 0) ? 1 : 0;
                }
            }

            Assert.Multiple(() =>
            {
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(wal.NewestOffset), "precondition: the live advance kept the leaf at the head");
                Assert.That(reasons, Does.Not.Contain((string)LatticeMetrics.DriverDeclineRecheckCheckpointStalled.Value!), "a leaf at the head was routed to the stall drive");
                Assert.That(drives, Is.Zero, "a leaf at the head was driven");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task HeadDrive_steps_aside_while_replays_queue_and_stays_owed()
    {
        // Detectors 3 and 6 (BH3): while replays wait on the gate the stalled drive is not
        // requested, which is counted; it stays owed, and the first tick with no waiter
        // drives it.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, wal, rig, treeId) = await HeadDriveLeafAsync("head-drive-yield");
            var reasons = new List<string>();
            var drivenWhileQueued = false;
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                // Queued until the first yield, then free on the very next tick: a drive kept
                // owed runs there, whatever phase its jitter left the stall count in.
                var yieldReason = (string)LatticeMetrics.DriverDeclineRecheckDriveYielded.Value!;
                for (var tick = 0; tick < 8 && !reasons.Contains(yieldReason); tick++)
                {
                    BPlusLeafGrain.SeedReplayAdmissionStateForTest(TimerAdmissionCeiling, queued: 1);
                    drivenWhileQueued |= await HeadDriveTickAsync(grain, wal, rig, tick, foreign: 3);
                }
            }

            var yielded = reasons.Count(r => r == (string)LatticeMetrics.DriverDeclineRecheckDriveYielded.Value!);
            BPlusLeafGrain.SeedReplayAdmissionStateForTest(TimerAdmissionCeiling, queued: 0);
            var drivenOnFree = await HeadDriveTickAsync(grain, wal, rig, 8, foreign: 3);

            Assert.Multiple(() =>
            {
                Assert.That(drivenWhileQueued, Is.False, "the drive competed with queued replays");
                Assert.That(yielded, Is.GreaterThan(0), "a yielded drive was not counted");
                Assert.That(drivenOnFree, Is.True, "a drive that yielded was not still owed when the gate freed");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task HeadDrive_keeps_a_hinted_uncovered_partitions_hold_back()
    {
        // Detector 7 (BH5, #5886): a split sibling born with a hint at an uncovered partition
        // is driven to the head. Its capture fails, so the drive's flush persists nothing
        // above -1 there; the advance stays pending, never above coverage.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var wal = new GrowingWal();
            wal.GrowTo(0);
            var (grain, state, snapshot, _) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1L,
                reClassifyEveryN: 1000,
                treeId: UniqueCoverageRepairTreeId("head-drive-hinted"),
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: wal.Coordinator);
            snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                .Returns<Task<LeafSnapshotSaveOutcome>>(_ => throw new InvalidOperationException("snapshot store is down"));
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            SeedRow(grain);
            Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(-1L), "precondition: nothing persisted before the hint");
            await grain.SetCheckpointOffsetHintsAsync([7]);
            wal.GrowTo(30);

            wal.Coordinator.ClearReceivedCalls();
            await grain.DispatchTimerDriveAsync(1, "arm=stalled-drive");

            Assert.Multiple(() =>
            {
                Assert.That(ReadTheWal(wal), Is.True, "precondition: the drive replayed");
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.GreaterThan(7L), "precondition: the drive advanced the hinted partition");
                Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L), "precondition: no capture landed");
                Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(-1L),
                    "a head drive persisted a hinted partition above coverage after its capture failed (#5886, BH5)");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task HeadDrive_is_driven_at_most_once_per_stall_window_and_never_on_a_write()
    {
        // Detector 4 (BH2): the cadence is the stall arm's, re-armed with jitter, so over many
        // ticks a leaf held below the head is driven at most once per threshold window; a
        // write alone never drives it.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, wal, rig, _) = await HeadDriveLeafAsync("head-drive-cadence");
            wal.Coordinator.ClearReceivedCalls();
            for (var i = 0; i < 5; i++)
            {
                await SetStampedAsync(grain, $"x{i}", "v");
                rig.Writer.ForeignAppend();
                wal.GrowTo(rig.Writer.NextOffset(0) - 1);
            }

            var readOnWrites = ReadTheWal(wal);
            var drives = 0;
            for (var tick = 0; tick < 12; tick++)
            {
                drives += await HeadDriveTickAsync(grain, wal, rig, tick, foreign: 3) ? 1 : 0;
            }

            Assert.Multiple(() =>
            {
                Assert.That(readOnWrites, Is.False, "a write drove the leaf");
                Assert.That(drives, Is.InRange(1, 12 / 3), "the drive ran more often than once per stall window");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}

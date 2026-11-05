using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The reported-bound catch-up (#5851, a5e2e2ef's ruling 2): a pre-arm leaf whose
/// claimless advance or split hint is held only by a reported foreign or unstamped
/// record above its checkpoint reads past it on its next coverage-lag tick, through the
/// existing drive's replay, once per bound - and never on the write that reported it.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task A_reported_bound_hold_is_read_past_on_the_next_tick_once()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var wal = new GrowingWal();
            wal.GrowTo(0);
            var rig = new LiveAdvanceRig(firstOffset: 1);
            rig.FenceGate.IsPreArmOpen = true;
            var (grain, _, _, _) = CreateLeafForCoverageRepair(
                persistedCheckpoint: 0,
                treeId: UniqueCoverageRepairTreeId("reported-bound-catch-up"),
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: wal.Coordinator,
                liveAdvance: rig);
            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
            await InitialiseOwnMarkerAsync(rig);

            var foreign = rig.Writer.ForeignAppend();
            rig.ReportedWatermark = foreign;
            await SetStampedAsync(grain, "a", "1");
            wal.GrowTo(foreign + 1);
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.LessThan(foreign),
                "precondition: the reported record is above the leaf's checkpoint");

            wal.Coordinator.ClearReceivedCalls();
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            var readOnFirstTick = wal.Coordinator.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(ILeafReplayCoordinatorGrain.ReadSliceAsync));
            var caughtUp = grain.GetCurrentCheckpointForPartition(0);

            wal.Coordinator.ClearReceivedCalls();
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            var readOnSecondTick = wal.Coordinator.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(ILeafReplayCoordinatorGrain.ReadSliceAsync));

            Assert.Multiple(() =>
            {
                Assert.That(readOnFirstTick, Is.True, "the tick did not read past the reported record");
                Assert.That(caughtUp, Is.GreaterThanOrEqualTo(foreign), "the catch-up did not move the checkpoint past the reported bound");
                Assert.That(readOnSecondTick, Is.False, "a caught-up bound was read again");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    public async Task A_reported_bound_hold_reads_nothing_on_the_write_path()
    {
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await InitialiseOwnMarkerAsync(rig);
        var foreign = rig.Writer.ForeignAppend();
        rig.ReportedWatermark = foreign;
        await SetStampedAsync(grain, "a", "1");

        var held = !await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True, "precondition: the reported bound holds the claimless advance");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.LessThan(foreign), "the hold read past the record itself");
            Assert.That(grain.TakeReportedBoundCatchUp(), Is.True, "the hold owed no catch-up");
            Assert.That(grain.TakeReportedBoundCatchUp(), Is.False, "one bound owed its catch-up twice");
        });
    }

    [Test]
    public async Task A_split_capped_only_by_its_reported_bound_owes_the_donor_a_catch_up()
    {
        var (grain, _, _, _) = await PreArmSplitAsync(new WalSplitCut(Head: 42, ForeignHigh: -1, UnstampedWatermark: 35), frontier: 30);
        var owed = grain.TakeReportedBoundCatchUp();
        var again = grain.TakeReportedBoundCatchUp();

        Assert.Multiple(() =>
        {
            Assert.That(owed, Is.True, "a split capped only by the shared watermark owed its donor no catch-up");
            Assert.That(again, Is.False, "one bound owed its catch-up twice");
        });
    }

    [Test]
    public async Task A_split_capped_by_a_close_head_owes_no_catch_up()
    {
        var (grain, _, _, _) = await PreArmSplitAsync(
            new WalSplitCut(Head: 42, ForeignHigh: -1, UnstampedWatermark: 35),
            frontier: 30,
            onState: (_, rig) => rig.ShardMarker = new Orleans.Lattice.BPlusTree.State.ShardHandOffState
            {
                Initialised = true,
                CompactedHeads = [41],
            });

        Assert.That(grain.TakeReportedBoundCatchUp(), Is.False, "a close head, which only a hand-off's fence clears, owed a catch-up read");
    }
}

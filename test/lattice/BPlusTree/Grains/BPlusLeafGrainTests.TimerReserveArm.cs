using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A coverage-lag timer drive kept off the GC share's reserved slot is counted
/// under its own refusal arm, <c>timer_reserve</c>, not <c>gc_share</c>
/// (issue #6013). The reservation (issue #3575) refuses the timer with most
/// replay permits free, so counting it as <c>gc_share</c> read as saturation
/// (issue #6009).
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task A_timer_drive_kept_off_the_reserved_slot_is_counted_as_timer_reserve_not_gc_share()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var treeId = UniqueCoverageRepairTreeId("timer-reserve-arm");
        var wal = new GrowingWal();
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            treeId: treeId,
            maxConcurrentReplays: TimerAdmissionCeiling,
            replayCoordinator: wal.Coordinator);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4, CancellationToken.None);
        wal.GrowTo(5);

        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        var held = HoldTimerShare(gate);
        var refusals = new ConcurrentBag<KeyValuePair<string, object?>[]>();
        try
        {
            Assert.That(held, Is.EqualTo(1),
                "precondition: at a two-slot share the timer may hold one slot, and the other is reserved "
                + "for the sweep, so the leaf's own timer drive is refused by the reservation alone");

            using (ListenForSaturationRefusals(treeId, refusals))
            {
                for (var tick = 0; tick < TicksToReachStalledDrive; tick++)
                    await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }

            Assert.Multiple(() =>
            {
                Assert.That(refusals, Is.Not.Empty, "instrument validation: the timer drive was refused");
                Assert.That(refusals.Select(ArmOf), Is.All.EqualTo("timer_reserve"),
                    "a timer drive kept off the slot reserved for the sweep is the reservation working, with "
                    + "replay permits free; counting it as gc_share reads as saturation (issue #6009)");
                Assert.That(gate.CurrentCount, Is.GreaterThan(0),
                    "instrument validation: permits were free when the timer was refused");
            });
        }
        finally
        {
            ReleaseTimerShare(gate, held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}

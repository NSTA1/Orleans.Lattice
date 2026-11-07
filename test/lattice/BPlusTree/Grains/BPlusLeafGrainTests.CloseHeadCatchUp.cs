using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The widened catch-up (#5851, ruling 2 widened under a5e2e2ef's W1-W5): a pre-arm leaf
/// whose claimless advance or split hint is held only by reported bounds and concrete close
/// heads - its own birth close head among them - reads past them on its next coverage-lag
/// tick, to <c>max(reported bound, close head - 1)</c>. Detectors 1-6 of the ruling, each
/// named for the condition it pins.
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Records the leaf's own birth close on its marker with <paramref name="heads"/>, its
    /// donor bound witnessed by its link unless <paramref name="witnessed"/> is false.
    /// </summary>
    private static Task BirthCloseAsync(LiveAdvanceRig rig, long[] heads, bool witnessed = true, bool openToken = false) =>
        rig.SetLeafMarkerAsync(m =>
        {
            m.Initialised = true;
            HandOffMarkerCore.Open(m, "split:birth", null, null);
            HandOffMarkerCore.CloseWithDeferredBound(m, "split:birth", heads, ClaimlessDonor, ClaimlessDonorToken, 0, 1);
            if (witnessed)
            {
                HandOffMarkerCore.WitnessLink(m, ClaimlessDonor, 0, 1);
            }

            if (openToken)
            {
                HandOffMarkerCore.Open(m, "reclaim:open", null, null);
            }

            return true;
        });

    /// <summary>
    /// A pre-arm leaf on a growing WAL whose first write is banked, then held by its own
    /// concrete birth close at the allocator head after <paramref name="foreign"/> foreign
    /// records, with one own write above them. The WAL is readable through
    /// <paramref name="readableThrough"/>, or every allocated offset when null.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, GrowingWal Wal, LiveAdvanceRig Rig, long CloseHead)> BirthCloseHeldLeafAsync(
        string name, int foreign = 6, long? readableThrough = null)
    {
        var wal = new GrowingWal();
        wal.GrowTo(0);
        var rig = new LiveAdvanceRig(firstOffset: 1);
        rig.FenceGate.IsPreArmOpen = true;
        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: 0,
            treeId: UniqueCoverageRepairTreeId(name),
            maxConcurrentReplays: TimerAdmissionCeiling,
            replayCoordinator: wal.Coordinator,
            liveAdvance: rig);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        await InitialiseOwnMarkerAsync(rig);

        // A warm-up tick banks the first write, so no tick under test is the starved arm.
        await SetStampedAsync(grain, "w", "0");
        wal.GrowTo(rig.Writer.NextOffset(0) - 1);
        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        for (var i = 0; i < foreign; i++)
        {
            rig.Writer.ForeignAppend();
        }

        var closeHead = rig.Writer.NextOffset(0);
        await BirthCloseAsync(rig, [closeHead]);

        // G4: the witnessed birth close stays carried, so only a stamp issued after a marker
        // read put it on the fence covers it; the write under test is one.
        await SetStampedAsync(grain, "p", "0");
        await grain.TryAdvanceLiveCheckpointAsync();
        await SetStampedAsync(grain, "a", "1");
        wal.GrowTo(readableThrough ?? rig.Writer.NextOffset(0) - 1);
        return (grain, wal, rig, closeHead);
    }

    /// <summary>
    /// A write and a marker read, so the fence carries the leaf's witnessed birth close (G4)
    /// and every later stamp's report and cut reader cover it; until then a guard that
    /// requires that coverage would hold the leaf, and a W4 detector would pass vacuously.
    /// </summary>
    private static async Task PrimeCarriedClosesAsync(BPlusLeafGrain grain)
    {
        await SetStampedAsync(grain, "p", "0");
        await grain.TryAdvanceLiveCheckpointAsync();
    }

    private static bool ReadTheWal(GrowingWal wal) =>
        wal.Coordinator.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(ILeafReplayCoordinatorGrain.ReadSliceAsync));

    [Test]
    [NonParallelizable]
    public async Task CloseHeadCatchUp_held_only_by_its_own_concrete_birth_close_head_is_read_past_once_and_the_next_split_takes_the_hint()
    {
        // Detector 1.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, wal, _, closeHead) = await BirthCloseHeldLeafAsync("close-head-catch-up");
            var before = grain.GetCurrentCheckpointForPartition(0);

            wal.Coordinator.ClearReceivedCalls();
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            var readOnFirstTick = ReadTheWal(wal);
            var caughtUp = grain.GetCurrentCheckpointForPartition(0);

            wal.Coordinator.ClearReceivedCalls();
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            var readOnSecondTick = ReadTheWal(wal);

            var hints = await grain.UpliftSplitHintsForTestAsync([caughtUp], [new WalSplitCut(Head: closeHead + 10, ForeignHigh: -1, UnstampedWatermark: -1)]);

            Assert.Multiple(() =>
            {
                Assert.That(before, Is.LessThan(closeHead - 1), "precondition: the birth close head holds the leaf");
                Assert.That(readOnFirstTick, Is.True, "the tick did not read past the birth close head");
                Assert.That(caughtUp, Is.GreaterThanOrEqualTo(closeHead - 1), "the catch-up did not pass the close head");
                Assert.That(readOnSecondTick, Is.False, "a caught-up close head was read again");
                Assert.That(hints[0], Is.EqualTo(closeHead + 9), "the next split did not take the D4 hint");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    public async Task CloseHeadCatchUp_a_close_deferred_at_every_partition_is_never_a_target()
    {
        // Detector 2 (W3): a DeferredHead is the arm's to resolve, never a replay's target.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await rig.SetLeafMarkerAsync(static m =>
        {
            m.Initialised = true;
            HandOffMarkerCore.Open(m, "reclaim:deferred", null, null);
            return HandOffMarkerCore.CloseDeferred(m, "reclaim:deferred", 1, 0, 1);
        });
        await SetStampedAsync(grain, "a", "1");

        var held = !await grain.TryAdvanceLiveCheckpointAsync();
        var owedByAdvance = grain.TakeReportedBoundCatchUp();
        var hints = await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: 9, ForeignHigh: -1, UnstampedWatermark: -1)]);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True, "precondition: the deferred close holds the claimless advance");
            Assert.That(owedByAdvance, Is.False, "a deferred head was targeted by the claimless advance's catch-up");
            Assert.That(grain.TakeReportedBoundCatchUp(), Is.False, "a deferred head was targeted by the split's catch-up");
            Assert.That(hints[0], Is.EqualTo(-1L), "a deferred head raised the hint");
        });
    }

    [Test]
    public async Task CloseHeadCatchUp_a_close_deferred_at_another_partition_never_satisfies_held_only_by()
    {
        // Detector 2 (W3): resolved at partition 0 and deferred at partition 1, the close is
        // deferred somewhere, so even its concrete head at 0 is not a catch-up target.
        var rig = new LiveAdvanceRig(partitions: 2);
        rig.FenceGate.IsPreArmOpen = true;
        var grain = CreateGrain(liveAdvance: rig);
        await rig.SetLeafMarkerAsync(static m =>
        {
            m.Initialised = true;
            HandOffMarkerCore.Open(m, "reclaim:half", null, null);
            HandOffMarkerCore.CloseDeferred(m, "reclaim:half", 2, 0, 1);
            return HandOffMarkerCore.ResolveDeferred(m, 0, 0, 1, 5);
        });
        await SetStampedAsync(grain, "a", "1");

        await grain.UpliftSplitHintsForTestAsync([-1, -1], [
            new WalSplitCut(Head: 9, ForeignHigh: -1, UnstampedWatermark: -1),
            new WalSplitCut(Head: 1009, ForeignHigh: -1, UnstampedWatermark: -1),
        ]);

        Assert.That(grain.TakeReportedBoundCatchUp(), Is.False, "a close deferred at another partition was caught up past at this one");
    }

    [Test]
    public async Task CloseHeadCatchUp_a_stale_high_risen_after_the_catch_up_caps_the_next_cut()
    {
        // Detector 3 (W2): the next cut re-reads every cap; a catch-up that passed the stale
        // high as it stood is never remembered as passing it now.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await BirthCloseAsync(rig, [8]);
        await PrimeCarriedClosesAsync(grain);
        await SetStampedAsync(grain, "a", "1");

        await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: 10, ForeignHigh: 5, UnstampedWatermark: -1)]);
        var owed = grain.TakeReportedBoundCatchUp();
        await grain.SetCheckpointOffsetHintsAsync([7]);
        var caughtUp = grain.GetCurrentCheckpointForPartition(0);

        var hints = await grain.UpliftSplitHintsForTestAsync([caughtUp], [new WalSplitCut(Head: 12, ForeignHigh: 9, UnstampedWatermark: -1)]);

        Assert.Multiple(() =>
        {
            Assert.That(owed, Is.True, "precondition: the close head and stale high owed a catch-up");
            Assert.That(caughtUp, Is.EqualTo(7L), "precondition: the catch-up passed its target");
            Assert.That(hints[0], Is.EqualTo(7L), "the next cut took the hint past a stale high that rose after the catch-up");
            Assert.That(grain.TakeReportedBoundCatchUp(), Is.True, "the risen stale high owed no new catch-up");
        });
    }

    [Test]
    public async Task CloseHeadCatchUp_a_hold_by_an_open_token_owes_no_catch_up()
    {
        // Detector 4 (W4): an open token holds the leaf, so its close heads are not alone.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await BirthCloseAsync(rig, [8], openToken: true);
        await PrimeCarriedClosesAsync(grain);
        await SetStampedAsync(grain, "a", "1");

        var held = !await grain.TryAdvanceLiveCheckpointAsync();
        var owedByAdvance = grain.TakeReportedBoundCatchUp();
        await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: 10, ForeignHigh: -1, UnstampedWatermark: -1)]);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True, "precondition: the open token holds the claimless advance");
            Assert.That(owedByAdvance, Is.False, "a claimless hold by an open token owed a catch-up");
            Assert.That(grain.TakeReportedBoundCatchUp(), Is.False, "a split held by an open token owed a catch-up");
        });
    }

    [Test]
    public async Task CloseHeadCatchUp_a_hold_by_an_unwitnessed_bound_owes_no_catch_up()
    {
        // Detector 4 (W4): the birth close's donor bound is pending, so its sibling's link is
        // unwitnessed and its stale high may still rise; the reports cover it.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await BirthCloseAsync(rig, [8], witnessed: false);
        await SetStampedAsync(grain, "w", "0");
        await grain.TryAdvanceLiveCheckpointAsync();
        await SetStampedAsync(grain, "a", "1");

        var held = !await grain.TryAdvanceLiveCheckpointAsync();
        var owedByAdvance = grain.TakeReportedBoundCatchUp();
        await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: 10, ForeignHigh: -1, UnstampedWatermark: -1)]);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True, "precondition: the pending bound's close holds the claimless advance");
            Assert.That(owedByAdvance, Is.False, "a claimless hold by an unwitnessed bound owed a catch-up");
            Assert.That(grain.TakeReportedBoundCatchUp(), Is.False, "a split held by an unwitnessed bound owed a catch-up");
        });
    }

    [Test]
    public async Task CloseHeadCatchUp_a_split_target_covers_every_carried_stale_high()
    {
        // a5e2e2ef's addition to W1-W5: the cut's foreign high folds in the stale high of
        // every close its reader carried, and the catch-up's target covers it, so a read to
        // the target passes both the close head and the carried stale high.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await BirthCloseAsync(rig, [8]);
        await PrimeCarriedClosesAsync(grain);
        await SetStampedAsync(grain, "a", "1");

        await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: 14, ForeignHigh: 12, UnstampedWatermark: -1)]);

        Assert.That(grain.PendingCatchUpTargetForTest(0), Is.EqualTo(12L),
            "the catch-up targeted the close head alone, below the carried stale high the cut reported");
    }

    [Test]
    public async Task CloseHeadCatchUp_a_close_head_hold_with_the_watermark_owes_no_catch_up()
    {
        // Detector 4 (W4): the shared watermark holds as well as the close head, on the
        // claimless advance (whose reported bound folds it in) and on the split's cut.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await BirthCloseAsync(rig, [8]);
        await PrimeCarriedClosesAsync(grain);
        rig.ReportedWatermark = 4;
        await SetStampedAsync(grain, "a", "1");

        var held = !await grain.TryAdvanceLiveCheckpointAsync();
        var owedByAdvance = grain.TakeReportedBoundCatchUp();
        await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: 10, ForeignHigh: -1, UnstampedWatermark: 4)]);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True, "precondition: the close head and watermark hold the claimless advance");
            Assert.That(owedByAdvance, Is.False, "a claimless hold by the watermark and a close head owed a catch-up");
            Assert.That(grain.TakeReportedBoundCatchUp(), Is.False, "a split held by the watermark and a close head owed a catch-up");
        });
    }

    [Test]
    [NonParallelizable]
    public async Task CloseHeadCatchUp_is_dispatched_once_per_target_on_the_tick_and_never_on_a_write()
    {
        // Detector 5 (W5): the write that meets the hold, the advance and a split cut read
        // nothing; a target the tick's read cannot reach is not dispatched again on later
        // ticks; raised twice within a tick, it is dispatched once.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, wal, _, closeHead) = await BirthCloseHeldLeafAsync("close-head-catch-up-rate", readableThrough: 4);

            wal.Coordinator.ClearReceivedCalls();
            await SetStampedAsync(grain, "b", "2");
            await grain.TryAdvanceLiveCheckpointAsync();
            await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: closeHead + 2, ForeignHigh: -1, UnstampedWatermark: -1)]);
            var readOnWrite = ReadTheWal(wal);

            var reads = new List<bool>();
            for (var tick = 0; tick < 3; tick++)
            {
                wal.Coordinator.ClearReceivedCalls();
                await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
                reads.Add(ReadTheWal(wal));
            }

            await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: closeHead + 4, ForeignHigh: closeHead + 1, UnstampedWatermark: -1)]);
            await grain.UpliftSplitHintsForTestAsync([-1], [new WalSplitCut(Head: closeHead + 5, ForeignHigh: closeHead + 2, UnstampedWatermark: -1)]);
            var owed = grain.TakeReportedBoundCatchUp();
            var owedTwice = grain.TakeReportedBoundCatchUp();

            Assert.Multiple(() =>
            {
                Assert.That(readOnWrite, Is.False, "a catch-up was read on the write path");
                Assert.That(reads, Is.EqualTo(new[] { true, false, false }), "an unreachable target was dispatched on every tick");
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.LessThan(closeHead - 1), "precondition: the read could not reach the target");
                Assert.That(owed, Is.True, "a risen target owed no catch-up");
                Assert.That(owedTwice, Is.False, "a target raised twice within a tick was dispatched twice");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task CloseHeadCatchUp_whose_read_stops_at_a_hole_advances_only_to_the_contiguous_position()
    {
        // Detector 6 (W1): the WAL is readable only through offset 4, below the close head;
        // the catch-up's checkpoint is its read's, never its target.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var (grain, wal, _, closeHead) = await BirthCloseHeldLeafAsync("close-head-catch-up-hole", readableThrough: 4);

            wal.Coordinator.ClearReceivedCalls();
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(closeHead - 1, Is.GreaterThan(4L), "precondition: the target lies past the hole");
                Assert.That(ReadTheWal(wal), Is.True, "precondition: the tick read for the catch-up");
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(4L), "the catch-up advanced past its contiguous read position");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}

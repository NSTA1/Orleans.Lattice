using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The consistent-cut advance (#6166): a stalled leaf whose own writes are all applied - one behind a bulk append's right edge applies none -
/// takes its partition's cut head where the cut and a view read after it prove it has read
/// every record below the head it could own - the split's guard (#5837, D4) - and is held,
/// owing a catch-up read, where they do not.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const long CutAdvanceCheckpoint = 5;

    private static async Task<(BPlusLeafGrain Grain, LiveAdvanceRig Rig)> IdleCutLeafAsync(WalSplitCut cut, Action<LiveAdvanceRig>? configure = null, string? highKey = null, Action<LeafNodeState>? configureState = null)
    {
        var rig = PreArmRig();
        rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(cut));
        configure?.Invoke(rig);
        var state = new FakePersistentState<LeafNodeState>();
        state.State.ProjectionCheckpointOffset = CutAdvanceCheckpoint;
        state.State.ProjectionCheckpointOffsetAssigned = true;
        state.State.HighKeyExclusive = highKey;
        configureState?.Invoke(state.State);
        var grain = CreateGrain(
            state,
            liveAdvance: rig,
            configureGrainFactory: f => f.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(rig.Shard));
        await InitialiseOwnMarkerAsync(rig);

        // Its one write resolves its append fence, as an activation's replay does; it is applied.
        await SetStampedAsync(grain, "a", "1");
        return (grain, rig);
    }

    [Test]
    public async Task CutAdvance_a_quiescent_leaf_past_every_bound_takes_the_cut_head_without_a_replay()
    {
        var (grain, _) = await IdleCutLeafAsync(new WalSplitCut(Head: 40, ForeignHigh: 3, UnstampedWatermark: CutAdvanceCheckpoint));

        var reached = await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1);

        Assert.Multiple(() =>
        {
            Assert.That(reached, Is.EqualTo(BPlusLeafGrain.CutAdvanceOutcome.Reached), "a leaf past every bound did not reach its cut head");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(39L), "the checkpoint is not one below the cut head");
        });
    }

    [TestCase(-1L, 12L)]
    [TestCase(12L, -1L)]
    public async Task CutAdvance_a_reported_bound_above_the_checkpoint_holds_it_and_owes_a_catch_up(long foreignHigh, long watermark)
    {
        var (grain, _) = await IdleCutLeafAsync(new WalSplitCut(Head: 40, ForeignHigh: foreignHigh, UnstampedWatermark: watermark));

        var reached = await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1);

        Assert.Multiple(() =>
        {
            Assert.That(reached, Is.EqualTo(BPlusLeafGrain.CutAdvanceOutcome.CatchUp), "a leaf held only by a known read was not sent to read to it, and no further");
            Assert.That(grain.CutCatchUpBoundsForReplay, Is.EqualTo(new long[] { 12 }));
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(CutAdvanceCheckpoint), "the advance passed a record it had not read");
            Assert.That(grain.PendingCatchUpTargetForTest(0), Is.EqualTo(12L), "the held leaf owes no catch-up to the bound");
        });
    }

    [Test]
    public async Task CutAdvance_a_close_head_above_the_checkpoint_holds_it()
    {
        var (grain, rig) = await IdleCutLeafAsync(new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1));
        await rig.SetLeafMarkerAsync(static m =>
        {
            HandOffMarkerCore.Open(m, "reclaim:x", null, null);
            return HandOffMarkerCore.Close(m, "reclaim:x", [20]);
        });

        var reached = await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1);

        Assert.Multiple(() =>
        {
            Assert.That(reached, Is.EqualTo(BPlusLeafGrain.CutAdvanceOutcome.CatchUp));
            Assert.That(grain.CutCatchUpBoundsForReplay, Is.EqualTo(new long[] { 19 }), "the catch-up does not read to one below the close head");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(CutAdvanceCheckpoint), "the advance passed a close head it had not read to");
        });
    }

    [Test]
    public async Task CutAdvance_reads_the_marker_view_after_the_cut()
    {
        // As the split's V3: a hand-off opened after the view and before the cut could hold
        // records below the cut's head that no bound the cut reports covers.
        var order = new List<string>();
        var (grain, _) = await IdleCutLeafAsync(
            new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1),
            rig =>
            {
                rig.Shard.When(s => s.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())).Do(_ => order.Add("cut"));
                rig.ShardRoot.When(s => s.GetLiveAdvanceViewAsync(Arg.Any<GrainId>(), Arg.Any<string?>(), Arg.Any<string?>())).Do(_ => order.Add("view"));
            });

        await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1);

        Assert.Multiple(() =>
        {
            Assert.That(order, Does.Contain("cut"), "precondition: the advance read its cut");
            Assert.That(order.LastIndexOf("view"), Is.GreaterThan(order.IndexOf("cut")), "the marker view was read before the cut");
        });
    }

    [Test]
    public async Task CutAdvance_at_activation_reads_the_cut_for_no_activation_and_the_stall_drive_for_its_own()
    {
        // #6166: at activation nothing above the checkpoint is applied yet, so the cut must be
        // bounded by every activation's records, as a resumed close reads them (#5837, rule R).
        var readers = new List<WalAppendStamp>();
        var (grain, rig) = await IdleCutLeafAsync(new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1));
        rig.Shard.When(s => s.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>()))
            .Do(c => readers.Add(c.Arg<WalAppendStamp>()));

        await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1);
        await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1, atActivation: true);

        Assert.That(readers.Select(r => r.Activation == Guid.Empty), Is.EqualTo(new[] { false, true }),
            "the activation's cut excluded an activation's records it had not applied, or the drive's its own applied ones");
    }

    [Test]
    public async Task CutAdvance_at_activation_is_refused_while_durable_saga_work_is_unrestored()
    {
        // #6166: the activation's cut runs before the replay restores the deferred saga work a
        // previous activation recorded, so a terminal above the checkpoint is not yet bound.
        var (grain, _) = await IdleCutLeafAsync(
            new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1),
            configureState: s => s.UnresolvedReplayWork = [new UnresolvedReplayWorkEntry(0, 3, default)]);

        var outcome = await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1, atActivation: true);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(BPlusLeafGrain.CutAdvanceOutcome.NotTaken));
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(CutAdvanceCheckpoint));
        });
    }

    [Test]
    public async Task CutAdvance_a_shard_hand_off_open_over_the_leaf_defers_the_drive()
    {
        var (grain, rig) = await IdleCutLeafAsync(new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1));
        HandOffMarkerCore.Open(rig.ShardMarker, "bulk:open", null, null);

        var outcome = await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(BPlusLeafGrain.CutAdvanceOutcome.Deferred),
                "a leaf an open switch holds was sent to replay what it must read again past the switch's close");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(CutAdvanceCheckpoint));
        });
    }

    [TestCase(true, 39L)]
    [TestCase(false, CutAdvanceCheckpoint)]
    public async Task CutAdvance_a_leaf_holding_its_own_birth_close_is_not_held_by_the_shards_bulk_closes(bool birthCloseRecorded, long expected)
    {
        // #6166 (EvictionSparesBirthClose): a later fence of the run, which the shard marker's
        // retention folds into heads bearing on every bulk-born leaf, holds only a leaf whose
        // own marker holds no birth close; one that does is held by that close alone.
        var (grain, rig) = await IdleCutLeafAsync(new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1), highKey: "m");
        HandOffMarkerCore.Open(rig.ShardMarker, "bulk:later", null, null);
        HandOffMarkerCore.Close(rig.ShardMarker, "bulk:later", [30]);
        if (birthCloseRecorded)
        {
            await rig.SetLeafMarkerAsync(m => HandOffMarkerCore.RecordBulkBirthClose(m, "bulk:birth", [3]));
        }

        await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1);

        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(expected));
    }

    [Test]
    public async Task CutAdvance_a_never_checkpointed_leaf_is_left_to_the_starvation_drive()
    {
        var rig = PreArmRig();
        var grain = CreateGrain(
            liveAdvance: rig,
            configureGrainFactory: f => f.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(rig.Shard));
        await InitialiseOwnMarkerAsync(rig);
        await SetStampedAsync(grain, "a", "1");

        var reached = await grain.TryAdvanceCheckpointFromCutAsync(partitions: 1);

        Assert.Multiple(() =>
        {
            Assert.That(reached, Is.EqualTo(BPlusLeafGrain.CutAdvanceOutcome.NotTaken));
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));
        });
        await rig.Shard.DidNotReceive().GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    [NonParallelizable]
    public async Task CutAdvance_a_timer_drive_takes_the_cut_only_inside_the_timer_share()
    {
        // #6166: every resident leaf's stall drive read its cut unadmitted, so the
        // background load grew with the tree and crowded a bulk seed off the WAL shards.
        // The cut is now admitted against the timer's share of the replay gate's width,
        // as the replay drive is, holding no replay permit.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: TimerAdmissionCeiling, queued: 0);
        var (grain, rig) = await IdleCutLeafAsync(new WalSplitCut(Head: 40, ForeignHigh: -1, UnstampedWatermark: -1));
        var held = 0;
        try
        {
            await grain.DispatchTimerDriveAsync(1, "arm=test", cutFirst: true);
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(39L), "precondition: an admitted cut drive takes the cut head");

            // A four-permit gate leaves the timer one slot of its two-slot share; it is taken.
            BPlusLeafGrain.AdjustTimerCutDrivesForTest(held = 1);
            rig.Shard.ClearReceivedCalls();
            rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(new WalSplitCut(Head: 80, ForeignHigh: -1, UnstampedWatermark: -1)));
            await grain.DispatchTimerDriveAsync(1, "arm=test", cutFirst: true);

            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(39L), "a cut drive refused the timer share advanced");
            await rig.Shard.DidNotReceive().GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>());
        }
        finally
        {
            BPlusLeafGrain.AdjustTimerCutDrivesForTest(-held);
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}

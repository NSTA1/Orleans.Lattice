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

    private static async Task<(BPlusLeafGrain Grain, LiveAdvanceRig Rig)> IdleCutLeafAsync(WalSplitCut cut, Action<LiveAdvanceRig>? configure = null)
    {
        var rig = PreArmRig();
        rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(cut));
        configure?.Invoke(rig);
        var state = new FakePersistentState<LeafNodeState>();
        state.State.ProjectionCheckpointOffset = CutAdvanceCheckpoint;
        state.State.ProjectionCheckpointOffsetAssigned = true;
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
            Assert.That(reached, Is.True, "a leaf past every bound did not reach its cut head");
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
            Assert.That(reached, Is.False);
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
            Assert.That(reached, Is.False);
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
            Assert.That(reached, Is.False);
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L));
        });
        await rig.Shard.DidNotReceive().GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }
}

using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5872: a single verdict that descent does not reach a leaf - from its proof or its
/// live-advance view - is a suspicion, not the #5709 latch (a5e2e2ef's R1-R5). The leaf is
/// undecided: it appends unstamped for a backoff, never pre-arm and never a claim, then
/// proves again; a reachable re-check proves it, and only a second unreachable verdict
/// after the backoff, or a structural witness at once, latches it. At the split cut an
/// unconfirmed verdict means no hint and no latch.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_reachable_leaf_whose_view_reads_unreachable_once_appends_unstamped_then_is_proven()
    {
        // Detector 1.
        var rig = PreArmRig();
        rig.Reachable = false;
        var grain = CreateGrain(liveAdvance: rig);
        await InitialiseOwnMarkerAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        var fence = grain.AppendFenceForTesting!;
        var latchedOnOneVerdict = fence.IsUnreachable;
        var suspected = fence.IsUnreachableSuspectedForTesting;
        await SetStampedAsync(grain, "b", "2");

        rig.Reachable = true;
        fence.ElapseBackoffForTesting();
        await SetStampedAsync(grain, "c", "3");

        Assert.Multiple(() =>
        {
            Assert.That(latchedOnOneVerdict, Is.False, "one unreachable verdict latched the leaf");
            Assert.That(suspected, Is.True, "precondition: the verdict left the leaf undecided");
            Assert.That(rig.Writer.Stamps.Take(2), Is.All.Null, "an undecided leaf stamped during its backoff");
            Assert.That(fence.IsReachabilityProvenForTesting, Is.True, "a reachable re-check did not prove the leaf");
            Assert.That(fence.IsUnreachable, Is.False);
            Assert.That(fence.IsUnreachableSuspectedForTesting, Is.False, "the proof left the suspicion pending");
            Assert.That(rig.Writer.Stamps[^1], Is.Not.Null, "a proven leaf still appended unstamped");
        });
    }

    [Test]
    public async Task A_leaf_unreachable_on_both_reads_across_the_backoff_latches()
    {
        // Detector 2.
        var rig = PreArmRig();
        rig.Reachable = false;
        var grain = CreateGrain(liveAdvance: rig);
        await InitialiseOwnMarkerAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        var fence = grain.AppendFenceForTesting!;
        var latchedOnOneVerdict = fence.IsUnreachable;

        fence.ElapseBackoffForTesting();
        await SetStampedAsync(grain, "b", "2");

        Assert.Multiple(() =>
        {
            Assert.That(latchedOnOneVerdict, Is.False, "precondition: one verdict did not latch");
            Assert.That(fence.IsUnreachable, Is.True, "a second unreachable verdict after the backoff did not latch the leaf");
            Assert.That(rig.Writer.Stamps, Is.All.Null);
        });
    }

    [TestCase("orphan-repair")]
    [TestCase("absorbed")]
    [TestCase("superseded")]
    public async Task A_structural_witness_latches_on_the_first_unreachable_verdict(string witness)
    {
        // Detector 3: with a structural witness, the view's first unreachable verdict latches.
        var rig = PreArmRig();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, liveAdvance: rig);
        await InitialiseOwnMarkerAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        var fence = grain.AppendFenceForTesting!;
        var provenFirst = fence.IsReachabilityProvenForTesting;
        rig.Reachable = false;
        switch (witness)
        {
            case "orphan-repair":
                rig.OrphanRepairInProgress = true;
                break;
            case "absorbed":
                state.State.ReclaimRetired = true;
                break;
            default:
                fence.MarkStaleForTesting();
                break;
        }

        await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(provenFirst, Is.True, "precondition: the leaf was proven reachable");
            Assert.That(fence.IsUnreachable, Is.True, $"a structural witness ({witness}) did not latch on the first verdict");
        });
    }

    [Test]
    public async Task A_transient_unreachable_verdict_at_the_split_cut_takes_no_hint_and_does_not_latch()
    {
        // Detector 4: the view read after the cut reads unreachable once. The cut takes no
        // D4 hint - the sibling starts at the donor's frontier - and the donor is not latched.
        var cutTaken = false;
        var unreachableAtCut = 0;
        var (grain, _, sibling, _) = await PreArmSplitAsync(
            new WalSplitCut(Head: 42, ForeignHigh: 20, UnstampedWatermark: 25),
            frontier: 30,
            configure: rig =>
            {
                rig.Shard.When(s => s.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>()))
                    .Do(_ => cutTaken = true);
                rig.ShardRoot.GetLiveAdvanceViewAsync(Arg.Any<GrainId>(), Arg.Any<string?>(), Arg.Any<string?>())
                    .Returns(call =>
                    {
                        var reachable = !(cutTaken && unreachableAtCut++ == 0);
                        return Task.FromResult(LeafLiveAdvanceView.For(
                            rig.ShardMarker, call.ArgAt<string?>(1), call.ArgAt<string?>(2), reachable, false));
                    });
            });

        Assert.Multiple(() =>
        {
            Assert.That(unreachableAtCut, Is.GreaterThanOrEqualTo(1), "precondition: the view read after the cut was unreachable");
            Assert.That(grain.AppendFenceForTesting!.IsUnreachable, Is.False, "one unreachable verdict at the split cut latched the donor");
        });
        await sibling.Received(1).SetCheckpointOffsetHintsAsync(Arg.Is<long[]>(h => h.Length == 1 && h[0] == 30L));
    }
}

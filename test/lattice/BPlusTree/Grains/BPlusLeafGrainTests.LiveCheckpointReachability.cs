using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5709: an unreachable leaf never appends stamped. A leaf proves it is reachable
/// by descent before its first append-epoch claim; one that is not, with no hand-off
/// marker open on it, appends unstamped for the rest of its activation, so the reachable
/// leaf that owns the same keys must replay its records before it can arm (the model's
/// <c>OldAlloc</c>; dropping the report is <c>ReadPositionHonestUnstampedNotReported</c>).
/// A split sibling the parent has not yet learned of is born under an open marker and
/// keeps its claim.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_descent_unreachable_leaf_never_claims_and_appends_unstamped_and_reported()
    {
        // #5872: the first verdict leaves the leaf undecided, the second after the backoff
        // latches it; it never claims and appends unstamped throughout.
        var rig = new LiveAdvanceRig { Reachable = false };
        var grain = CreateGrain(liveAdvance: rig);

        await SetStampedAsync(grain, "a", "1");
        var fence = grain.AppendFenceForTesting!;
        var reportedFirst = fence.ReportedUnstamped(0);
        fence.ElapseBackoffForTesting();
        await SetStampedAsync(grain, "b", "2");

        Assert.Multiple(() =>
        {
            Assert.That(fence.IsUnreachable, Is.True);
            Assert.That(fence.Epoch, Is.Zero, "no epoch is claimed");
            Assert.That(reportedFirst, Is.EqualTo(0L), "its own append is unstamped and reported");
            Assert.That(fence.ReportedUnstamped(0), Is.EqualTo(1L), "its own append is unstamped and reported");
        });
        await rig.Shard.DidNotReceiveWithAnyArgs().ClaimAppendEpochAsync(default, default, default);
    }

    [Test]
    public async Task A_sibling_born_under_an_open_split_marker_still_claims_and_stamps()
    {
        var rig = new LiveAdvanceRig { Reachable = false };
        await rig.SetLeafMarkerAsync(m => HandOffMarkerCore.Open(m, "split:donor", null, null));
        var grain = CreateGrain(liveAdvance: rig);

        await SetStampedAsync(grain, "a", "1");

        var fence = grain.AppendFenceForTesting!;
        Assert.Multiple(() =>
        {
            Assert.That(fence.IsUnreachable, Is.False);
            Assert.That(fence.ClaimHead(0), Is.Not.Null, "the sibling claims its epoch eagerly");
            Assert.That(fence.ReportedUnstamped(0), Is.EqualTo(-1L), "and stamps its append");
        });
    }

    [Test]
    public async Task A_tick_that_finds_the_leaf_unreachable_stops_its_stamping_for_the_activation()
    {
        // #5872: the tick's unreachable verdict leaves the leaf undecided; the proof its next
        // append makes after the backoff confirms it, and the latch then holds for the
        // activation even once descent reaches it again.
        var (grain, rig) = await ArmedLeafWithWritesAsync("a");
        rig.Reachable = false;

        Assert.That(await grain.TryAdvanceLiveCheckpointAsync(), Is.False);
        var fence = grain.AppendFenceForTesting!;
        fence.ElapseBackoffForTesting();
        await SetStampedAsync(grain, "b", "2");
        rig.Reachable = true;
        await SetStampedAsync(grain, "c", "3");

        Assert.Multiple(() =>
        {
            Assert.That(fence.IsUnreachable, Is.True, "latched for the activation, even once descent reaches it again");
            Assert.That(fence.ReportedUnstamped(0), Is.EqualTo(2L), "the next appends are unstamped and reported");
        });
    }

    [Test]
    public async Task An_undecided_reachability_proof_appends_unstamped_without_latching()
    {
        var rig = new LiveAdvanceRig();
        rig.ShardRoot.IsReachableByDescentAsync(Arg.Any<GrainId>(), Arg.Any<string?>())
            .ThrowsAsync(new TimeoutException("the shard root did not answer"));
        var grain = CreateGrain(liveAdvance: rig);

        await SetStampedAsync(grain, "a", "1");

        var fence = grain.AppendFenceForTesting!;
        Assert.Multiple(() =>
        {
            Assert.That(fence.IsUnreachable, Is.False, "an unanswered proof decides nothing");
            Assert.That(fence.Epoch, Is.Zero, "but it does not claim on it");
            Assert.That(fence.ReportedUnstamped(0), Is.EqualTo(0L));
        });
    }
}
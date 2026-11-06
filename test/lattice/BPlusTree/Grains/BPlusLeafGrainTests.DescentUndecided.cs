using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5876, the leaf's consumers of an undecided descent (a5e2e2ef's D2, D3): an F1
/// span-routing refresh never routes on one, and neither undecided reads nor two reads
/// inside one backoff ever latch a leaf.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task An_F1_refresh_during_a_link_batch_does_not_route()
    {
        // Detector 3 (a5e2e2ef's condition 1 on #5876): the receiver is D' - its own state
        // declares the span [m, ..), so the post-refresh re-check admits the key and only the
        // descent's verdict decides. A real shard root holds a link batch open; its descent
        // would name this receiver, but overlapping the batch it is undecided, so nothing is
        // routed. Without D1 the positive descent refreshes and the retry is routed.
        var rig = PreArmRig();
        var grain = await SpanBornSiblingAsync(rig);
        var receiver = GrainId.Create("leaf", rig.LeafId.ToString("N"));
        var shard = DescentVerdictHarness.Create(rightLeafId: receiver);
        rig.ShardRoot.IsReachableByDescentAsync(Arg.Any<GrainId>(), Arg.Any<string?>())
            .Returns(call => shard.Grain.IsReachableByDescentAsync(call.ArgAt<GrainId>(0), call.ArgAt<string?>(1)));
        rig.ShardMarker = new ShardHandOffState { Initialised = true, RoutingGeneration = 5 };
        rig.Writer.ThrowNext = StaleSpan(generation: 3, refusal: 5);

        var batch = await shard.HoldLinkBatchAsync();
        try
        {
            await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(TransferRows("n")));
        }
        finally
        {
            await batch.ReleaseAsync();
        }

        var settled = await shard.Grain.IsReachableByDescentAsync(receiver, "n");
        Assert.Multiple(() =>
        {
            Assert.That(settled, Is.True, "precondition: a settled descent names the receiver, so only the verdict refuses the refresh");
            Assert.That(rig.Writer.LastRouting.IsSet, Is.False, "a transfer was routed on a descent that overlapped a link batch");
        });
    }

    [TestCase("undecided-twice")]
    [TestCase("undecided-view")]
    [TestCase("same-backoff")]
    public async Task Two_undecided_or_same_turn_reads_never_latch(string shape)
    {
        // Detector 5 (#5872 R2 as amended): the latch needs a decided unreachable verdict
        // from a later read than the first. Two undecided reads across the backoff, or two
        // unreachable reads inside one backoff, leave the leaf undecided and unstamped.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await InitialiseOwnMarkerAsync(rig);
        if (shape is "undecided-twice" or "undecided-view")
        {
            rig.VerdictOverride = DescentVerdict.Undecided;
        }
        else
        {
            rig.Reachable = false;
        }

        await SetStampedAsync(grain, "a", "1");
        var fence = grain.AppendFenceForTesting!;
        if (shape == "undecided-twice")
        {
            fence.ElapseBackoffForTesting();
            await SetStampedAsync(grain, "b", "2");
        }
        else if (shape == "undecided-view")
        {
            // The live-advance view's descent is undecided on two ticks across the backoff.
            await grain.TryAdvanceLiveCheckpointAsync();
            fence.ElapseBackoffForTesting();
            await grain.TryAdvanceLiveCheckpointAsync();
            await SetStampedAsync(grain, "b", "2");
        }
        else
        {
            await grain.TryAdvanceLiveCheckpointAsync();
            await SetStampedAsync(grain, "b", "2");
        }

        Assert.Multiple(() =>
        {
            Assert.That(fence.IsUnreachable, Is.False, $"the leaf latched on {shape}");
            Assert.That(fence.IsReachabilityProvenForTesting, Is.False, "an undecided descent proved the leaf");
            Assert.That(rig.Writer.Stamps, Is.All.Null, "an undecided leaf stamped");
        });
    }
}

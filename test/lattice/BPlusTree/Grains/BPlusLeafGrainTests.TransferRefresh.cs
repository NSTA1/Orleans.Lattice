using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The split transfer's refresh after a stale refusal (#5851, a5e2e2ef's ruling 1): a
/// transfer refused stale re-reads its span's generation from the shard as an admission
/// would, retries routed once, and keeps the refreshed generation for later transfers; a
/// refresh for a span the leaf no longer owns, or an unusable generation, falls back to
/// an unstamped append.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId TransferRefreshDonor = GrainId.Create("leaf", "transfer-refresh-donor");

    private static async Task<IBPlusLeafGrain> SpanBornSiblingAsync(LiveAdvanceRig rig, long spanGeneration = 3)
    {
        var grain = CreateGrain(liveAdvance: rig);
        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "live-advance-tree",
            ShardIndex = 0,
            LowKeyInclusive = "m",
            HighKeyExclusive = null,
            PrevSibling = TransferRefreshDonor,
            SplitBirth = true,
            SpanShard = 0,
            SpanGeneration = spanGeneration,
        });
        return grain;
    }

    private static Dictionary<string, LwwValue<byte[]>> TransferRows(string key) =>
        new() { [key] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), HybridLogicalClock.Tick(HybridLogicalClock.Zero)) };

    private static WalRoutingGenerationStaleException StaleSpan(long generation, long refusal) =>
        new(new WalRoutingInitiator(WalRoutingInitiatorKind.ShardRoot, 0), generation, refusal);

    [Test]
    public async Task A_stale_transfer_refreshes_its_span_generation_and_retries_routed()
    {
        var rig = PreArmRig();
        var grain = await SpanBornSiblingAsync(rig);
        rig.ShardMarker = new ShardHandOffState { Initialised = true, RoutingGeneration = 5 };
        rig.Writer.ThrowNext = StaleSpan(generation: 3, refusal: 5);

        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(TransferRows("n")));
        var retried = rig.Writer.LastRouting;
        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(TransferRows("p")));
        var later = rig.Writer.LastRouting;

        Assert.Multiple(() =>
        {
            Assert.That(retried, Is.EqualTo(WalRoutingStamp.ForShardRoot(0, 5)), "the refused transfer was not retried under the shard's refreshed generation");
            Assert.That(later, Is.EqualTo(WalRoutingStamp.ForShardRoot(0, 5)), "a later transfer lost the span's routing after the refresh");
        });
    }

    [Test]
    public async Task A_pre_link_sibling_refreshes_through_its_donors_reachability()
    {
        var rig = PreArmRig();
        rig.Reachable = false;
        rig.ShardRoot.IsReachableByDescentAsync(TransferRefreshDonor, Arg.Any<string?>()).Returns(Task.FromResult(true));
        var grain = await SpanBornSiblingAsync(rig);
        rig.ShardMarker = new ShardHandOffState { Initialised = true, RoutingGeneration = 5 };
        rig.Writer.ThrowNext = StaleSpan(generation: 3, refusal: 5);

        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(TransferRows("n")));

        Assert.That(rig.Writer.LastRouting, Is.EqualTo(WalRoutingStamp.ForShardRoot(0, 5)), "a pre-link sibling whose donor descent reaches did not refresh");
    }

    [Test]
    public async Task A_stale_receiver_whose_upper_keys_moved_is_never_routed()
    {
        // F1: a stale activation whose upper keys a later split moved away keeps its old
        // span in memory; descent for its low key still reaches it, but not for the moved
        // key, so the refresh is refused and the transfer is appended unstamped.
        var rig = PreArmRig();
        var grain = await SpanBornSiblingAsync(rig);
        rig.ShardRoot.IsReachableByDescentAsync(Arg.Any<GrainId>(), "z").Returns(Task.FromResult(false));
        rig.ShardMarker = new ShardHandOffState { Initialised = true, RoutingGeneration = 5 };
        rig.Writer.ThrowNext = StaleSpan(generation: 3, refusal: 5);

        var rows = TransferRows("n");
        rows["z"] = rows["n"];
        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(rows));

        Assert.That(rig.Writer.LastRouting.IsSet, Is.False, "a transfer carrying a key descent no longer routes to the receiver was appended routed");
    }

    [Test]
    public async Task A_refreshed_transfer_whose_keys_left_the_receivers_span_appends_unstamped()
    {
        // #5876 (a5e2e2ef's option c): the refresh succeeds - a usable generation, no
        // hand-off open, descent reaching the receiver - but the receiver narrowed while it
        // refreshed, so the transfer's key is no longer in its [Low, High). The routed retry
        // re-decides the transfer class after the refresh, and a key outside the span sends
        // it unstamped, which the key's new owner replays; it is never admitted routed under
        // a generation the new owner's bounds do not count.
        var rig = PreArmRig();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state, liveAdvance: rig);
        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "live-advance-tree",
            ShardIndex = 0,
            LowKeyInclusive = "m",
            HighKeyExclusive = null,
            PrevSibling = TransferRefreshDonor,
            SplitBirth = true,
            SpanShard = 0,
            SpanGeneration = 3,
        });
        rig.ShardMarker = new ShardHandOffState { Initialised = true, RoutingGeneration = 5 };
        rig.ShardRoot
            .When(r => r.GetLiveAdvanceViewAsync(Arg.Any<GrainId>(), Arg.Any<string?>(), Arg.Any<string?>()))
            .Do(_ => state.State.HighKeyExclusive = "n");
        rig.Writer.ThrowNext = StaleSpan(generation: 3, refusal: 5);

        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(TransferRows("n")));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.HighKeyExclusive, Is.EqualTo("n"), "precondition: the receiver narrowed during the refresh");
            Assert.That(rig.Writer.LastRouting, Is.Not.EqualTo(WalRoutingStamp.ForShardRoot(0, 5)),
                "a transfer whose key left the receiver's span was retried routed under the refreshed generation");
            Assert.That(rig.Writer.LastRouting.IsSet, Is.False, "the transfer was not appended unstamped");
        });
    }

    [TestCase("unreachable")]
    [TestCase("held-open")]
    [TestCase("below-refusal")]
    [TestCase("refused-again")]
    public async Task A_stale_transfer_without_an_admissible_refresh_appends_unstamped(string reason)
    {
        var rig = PreArmRig();
        var grain = await SpanBornSiblingAsync(rig);
        var marker = new ShardHandOffState { Initialised = true, RoutingGeneration = 5 };
        var refusal = 5L;
        switch (reason)
        {
            case "unreachable":
                rig.Reachable = false;
                break;
            case "held-open":
                HandOffMarkerCore.Open(marker, "bulk:op", "a", null);
                break;
            case "below-refusal":
                refusal = 6;
                break;
            case "refused-again":
                rig.ShardRoot
                    .When(r => r.GetLiveAdvanceViewAsync(Arg.Any<GrainId>(), Arg.Any<string?>(), Arg.Any<string?>()))
                    .Do(_ => rig.Writer.ThrowNext = StaleSpan(generation: 5, refusal: 6));
                break;
        }

        rig.ShardMarker = marker;
        rig.Writer.ThrowNext = StaleSpan(generation: 3, refusal: refusal);

        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(TransferRows("n")));
        var fallback = rig.Writer.LastRouting;
        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(TransferRows("p")));
        var later = rig.Writer.LastRouting;

        Assert.Multiple(() =>
        {
            Assert.That(fallback.IsSet, Is.False, $"a transfer without an admissible refresh ({reason}) was appended routed");
            Assert.That(later.IsSet, Is.False, $"the span's routing survived a failed refresh ({reason})");
        });
    }
}

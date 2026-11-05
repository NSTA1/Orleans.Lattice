using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The deferred reachability state (a5e2e2ef's D1-D5 on #5851): a split sibling not yet
/// reached by descent whose marker holds its donor's pending bound stamps pre-arm instead
/// of appending unstamped for a backoff, so it never raises the shared watermark; it never
/// claims, arms or advances claimlessly; an unrouted write still goes unstamped (C4); it
/// stamps only under the pre-arm gate; and a latch is never undone by it.
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    private sealed class SettablePreArmGate : IWalAppendFenceGate
    {
        public bool IsOpen { get; set; } = true;

        public bool IsPreArmOpen { get; set; } = true;
    }

    private static (LeafAppendEpochFence Fence, SettablePreArmGate Gate, IWalShardGrain Shard) DeferredFence()
    {
        var gate = new SettablePreArmGate();
        var shard = Substitute.For<IWalShardGrain>();
        var fence = new LeafAppendEpochFence(
            Leaf,
            gate,
            new EnumerableMemoryGrainStorage(),
            _ => { },
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree"),
            LatticeTenantLabel.ForTree("tree"),
            requireReachabilityProof: true);
        fence.OnReachabilityDeferred();
        return (fence, gate, shard);
    }

    [Test]
    public async Task DeferredSiblingClaims()
    {
        // DeferredSiblingClaims: a deferred sibling stamps pre-arm, never an epoch stamp.
        var (fence, _, shard) = DeferredFence();

        var stamp = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);
        fence.OnStampedAppended(0, -1, stamp!.Value);

        Assert.Multiple(() =>
        {
            Assert.That(fence.IsDeferredForTesting, Is.True, "precondition: the activation is deferred");
            Assert.That(stamp.Value.IsPreArm, Is.True, "a deferred sibling did not stamp pre-arm");
            Assert.That(stamp.Value.Activation, Is.Not.EqualTo(Guid.Empty), "the pre-arm stamp did not name its activation");
            Assert.That(fence.HasClaimedAnywhere, Is.False, "a deferred sibling claimed");
            Assert.That(fence.TryGetPreArmView(0, out _), Is.False, "a deferred sibling may advance claimlessly");
            Assert.That(fence.TryGetArmingView(0, out _, out _), Is.False, "a deferred sibling may arm");
        });
        await shard.DidNotReceiveWithAnyArgs().ClaimAppendEpochAsync(default, default, default);
    }

    [Test]
    public async Task DeferredSiblingStampsUnrouted()
    {
        // DeferredSiblingStampsUnrouted: C4 still holds for a deferred sibling - only a
        // routed write rides the pre-arm class.
        var (fence, _, shard) = DeferredFence();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(5L));
        shard.AppendStampedAsync(Arg.Any<WalRecord>(), Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalStampedAppendResult(6, -1, WalAppendFenceOutcome.Admitted)));

        var unrouted = await WalFencedAppendDispatch.AppendPointAsync(
            shard, new WalRecord(), batched: false, partition: 0, fence, WalRoutingStamp.None, waitForDeadline: false, CancellationToken.None);
        await shard.DidNotReceiveWithAnyArgs().AppendStampedAsync(default!, default, default);
        var routed = await WalFencedAppendDispatch.AppendPointAsync(
            shard, new WalRecord(), batched: false, partition: 0, fence, WalRoutingStamp.ForShardRoot(0, 1), waitForDeadline: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(unrouted, Is.EqualTo(5), "an unrouted write of a deferred sibling was not appended unstamped");
            Assert.That(routed, Is.EqualTo(6), "a routed write of a deferred sibling was not stamped");
        });
        await shard.Received(1).AppendStampedAsync(
            Arg.Any<WalRecord>(),
            Arg.Is<WalAppendStamp>(s => s.IsPreArm && s.Initiator.IsSet && s.Generation == 1),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_deferred_sibling_stamps_only_under_the_pre_arm_gate()
    {
        // D2.
        var (fence, gate, shard) = DeferredFence();
        gate.IsPreArmOpen = false;

        var stamp = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);

        Assert.That(stamp, Is.Null, "a deferred sibling stamped with the pre-arm gate closed");
        await shard.DidNotReceiveWithAnyArgs().ClaimAppendEpochAsync(default, default, default);
    }

    [Test]
    public async Task A_deferred_sibling_proves_again_after_its_backoff_and_a_latch_wins()
    {
        // D1 and D3: the deferral re-proves on the backoff's cadence; proof promotes it, a
        // latch ends it, and nothing un-latches.
        var (fence, _, shard) = DeferredFence();
        var dueAtOnce = fence.NeedsReachabilityProof;

        var (later, _, _) = DeferredFence();
        later.ClaimFailureBackoffForTesting = TimeSpan.Zero;
        later.OnReachabilityDeferred();
        var dueAfterBackoff = later.NeedsReachabilityProof;
        later.OnReachabilityProven();

        fence.StopStampingUnreachable();
        fence.OnReachabilityDeferred();
        var latched = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(dueAtOnce, Is.False, "a deferred sibling re-proved before its backoff");
            Assert.That(dueAfterBackoff, Is.True, "a deferred sibling never proved again");
            Assert.That(later.IsReachabilityProvenForTesting, Is.True, "a proof did not promote a deferred sibling");
            Assert.That(fence.IsUnreachable, Is.True, "a deferral undid a latch");
            Assert.That(latched, Is.Null, "a latched activation stamped");
        });
    }

    [Test]
    public async Task A_proven_activation_with_a_pending_bound_is_demoted_and_never_claims()
    {
        // a5e2e2ef's B2 ruling (ProvenSiblingClaimsPreLink): a P1-proven sibling whose own
        // link is not witnessed is demoted to deferred, so it cannot claim before its link,
        // and is proven again once descent reaches it.
        var (fence, gate, shard) = DeferredFence();
        fence.OnReachabilityProven();
        var provenFirst = fence.IsReachabilityProvenForTesting;

        fence.OnReachabilityDeferred();
        gate.IsPreArmOpen = false;
        var underEagerGate = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);
        gate.IsPreArmOpen = true;
        fence.PromoteDeferredReachable();

        Assert.Multiple(() =>
        {
            Assert.That(provenFirst, Is.True, "precondition: the activation was proven");
            Assert.That(underEagerGate, Is.Null, "a demoted sibling claimed under the eager gate");
            Assert.That(fence.IsReachabilityProvenForTesting, Is.True, "a reachable demoted sibling was not proven again");
        });
        await shard.DidNotReceiveWithAnyArgs().ClaimAppendEpochAsync(default, default, default);
    }
}

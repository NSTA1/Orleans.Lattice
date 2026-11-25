using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5872, detector 5: an activation left undecided by one unreachable verdict appends
/// unstamped - its stamp is null - and never pre-arm or a claim, for its backoff; then it
/// proves again (a5e2e2ef's R1).
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    [Test]
    public async Task An_undecided_activation_stamps_null_never_pre_arm()
    {
        var fence = new LeafAppendEpochFence(
            Leaf,
            new PreArmGate(),
            new EnumerableMemoryGrainStorage(),
            _ => { },
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree"),
            LatticeTenantLabel.ForTree("tree"),
            requireReachabilityProof: true);
        fence.OnReachabilityProven();
        var shard = Substitute.For<IWalShardGrain>();
        var provenStamp = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);

        var latched = fence.OnUnreachableVerdict();
        var undecidedStamp = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);
        var reproveDuringBackoff = fence.NeedsReachabilityProof;
        fence.ElapseBackoffForTesting();

        Assert.Multiple(() =>
        {
            Assert.That(provenStamp?.IsPreArm, Is.True, "precondition: a proven activation stamps pre-arm under the pre-arm gate");
            Assert.That(latched, Is.False, "one verdict latched the activation");
            Assert.That(undecidedStamp, Is.Null, "an undecided activation stamped");
            Assert.That(fence.IsUnreachable, Is.False);
            Assert.That(reproveDuringBackoff, Is.False, "the activation proved again inside its backoff");
            Assert.That(fence.NeedsReachabilityProof, Is.True, "the activation did not prove again after its backoff");
        });
        await shard.DidNotReceiveWithAnyArgs().ClaimAppendEpochAsync(default, default, default);
    }

    [Test]
    public async Task A_deferred_activation_suspected_unreachable_never_stamps_pre_arm_after_its_backoff()
    {
        // a5e2e2ef's S1 on #5884: a deferred sibling whose bound is now witnessed reads
        // unreachable once. It is demoted with its deferral, so when the backoff ends it
        // appends unstamped until a fresh proof - never pre-arm on the stale deferral.
        var fence = new LeafAppendEpochFence(
            Leaf,
            new PreArmGate(),
            new EnumerableMemoryGrainStorage(),
            _ => { },
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree"),
            LatticeTenantLabel.ForTree("tree"),
            requireReachabilityProof: true);
        fence.OnReachabilityDeferred();
        var shard = Substitute.For<IWalShardGrain>();
        var deferredStamp = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);

        fence.OnUnreachableVerdict();
        fence.ElapseBackoffForTesting();
        var afterBackoff = await fence.StampForAsync(shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(deferredStamp?.IsPreArm, Is.True, "precondition: the deferred activation stamped pre-arm");
            Assert.That(fence.IsDeferred, Is.False, "an unreachable suspicion left the activation deferred");
            Assert.That(afterBackoff, Is.Null, "a demoted activation stamped pre-arm after its backoff");
            Assert.That(fence.NeedsReachabilityProof, Is.True, "the demoted activation does not prove again");
        });
    }

    [Test]
    public async Task An_undecided_live_view_holds_off_a_proven_activations_stamps_but_not_an_unproven_ones_proof()
    {
        // #6166: a bulk-seeded leaf reads its view while the operation's graft is pending.
        // Unproven, it stamps nothing either way, so a backoff would only hold off the proof
        // its first live write - the next chunk's fill - makes, sending that fill unstamped.
        // A proven activation still appends unstamped for the backoff (#5876, D2).
        var unproven = NewPreArmFence();
        unproven.OnLiveViewUndecided();
        var proven = NewPreArmFence();
        proven.OnReachabilityProven();
        proven.OnLiveViewUndecided();
        var shard = Substitute.For<IWalShardGrain>();
        var provenStamp = await proven.StampForAsync(shard, partition: 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(unproven.NeedsReachabilityProof, Is.True, "an undecided view held off an unproven activation's proof");
            Assert.That(provenStamp, Is.Null, "a proven activation stamped through an undecided view's backoff");
            Assert.That(proven.IsUnreachable, Is.False);
        });

        static LeafAppendEpochFence NewPreArmFence() => new(
            Leaf,
            new PreArmGate(),
            new EnumerableMemoryGrainStorage(),
            _ => { },
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree"),
            LatticeTenantLabel.ForTree("tree"),
            requireReachabilityProof: true);
    }
}

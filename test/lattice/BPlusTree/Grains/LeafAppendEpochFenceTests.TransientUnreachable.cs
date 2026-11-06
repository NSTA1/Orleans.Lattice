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
}

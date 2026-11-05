using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The leaf's reachability proof defers a pre-link split sibling (a5e2e2ef's D1 and D4 on
/// #5851): one not reached by descent whose marker holds its donor's pending bound is
/// deferred - stamping pre-arm - rather than backed off unstamped, so it never raises the
/// shared watermark while it waits for its link (the stamping itself is pinned by
/// <c>LeafAppendEpochFenceTests.DeferredSiblingStampsUnrouted</c>).
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_pre_link_sibling_with_a_pending_donor_bound_is_deferred_and_stamps()
    {
        var rig = PreArmRig();
        var grain = await UnreachableExemptLeafAsync(rig);
        var fence = grain.AppendFenceForTesting!;

        Assert.Multiple(() =>
        {
            Assert.That(fence.IsDeferredForTesting, Is.True, "a pre-link sibling with a pending donor bound was not deferred");
            Assert.That(fence.IsUnreachable, Is.False, "a deferred sibling was latched");
            Assert.That(fence.IsReachabilityProvenForTesting, Is.False, "a deferral proved the sibling");
        });
    }

    private static async Task<BPlusLeafGrain> BirthProvenSiblingWithPendingCloseAsync(LiveAdvanceRig rig)
    {
        var grain = CreateGrain(liveAdvance: rig);
        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "live-advance-tree",
            ShardIndex = 0,
            LowKeyInclusive = "m",
            SplitBirth = true,
        });
        await PendingDonorCloseAsync(rig);
        return grain;
    }

    [Test]
    public async Task A_birth_proven_sibling_whose_shard_raised_for_another_link_defers_and_never_claims()
    {
        // a5e2e2ef's B2 ruling, detector 1: another split's link raised the shard's
        // generation past the bound; this sibling's own link has not landed. It is demoted
        // to deferred - not latched, never claiming - and its stamps keep the pending close.
        var rig = PreArmRig();
        rig.Reachable = false;
        rig.ShardMarker.RoutingGeneration = 9;
        var grain = await BirthProvenSiblingWithPendingCloseAsync(rig);
        var fence = grain.AppendFenceForTesting!;
        var provenAtBirth = fence.IsReachabilityProvenForTesting;
        await SetStampedAsync(grain, "n", "1");

        await grain.TryAdvanceLiveCheckpointAsync(withinDrive: true);

        Assert.Multiple(() =>
        {
            Assert.That(provenAtBirth, Is.True, "precondition: the sibling was proven at its split birth");
            Assert.That(fence.IsDeferredForTesting, Is.True, "a pre-link sibling was not deferred after another link's raise");
            Assert.That(fence.IsUnreachable, Is.False, "a pre-link sibling was latched after another link's raise");
            Assert.That(fence.HasClaimedAnywhere, Is.False, "a pre-link sibling claimed");
            Assert.That(fence.TryGetCutReader(out var reader) && reader.Close1.IsSet, Is.True, "the sibling's stamps dropped its pending close");
        });
        await rig.Shard.DidNotReceiveWithAnyArgs().ArmAppendEpochAsync(default, default, default, default, default, default);
    }

    [Test]
    public async Task A_deferred_sibling_is_proven_again_once_its_own_link_is_witnessed_and_reachable()
    {
        // Detector 3: after its own link and raise, the leaf is proven and the close resolves.
        var rig = PreArmRig();
        rig.Reachable = false;
        rig.ShardMarker.RoutingGeneration = 9;
        var grain = await BirthProvenSiblingWithPendingCloseAsync(rig);
        await SetStampedAsync(grain, "n", "1");
        await grain.TryAdvanceLiveCheckpointAsync();
        var deferred = grain.AppendFenceForTesting!.IsDeferredForTesting;

        rig.Reachable = true;
        await rig.SetLeafMarkerAsync(m => HandOffMarkerCore.WitnessLink(m, ClaimlessDonor, 10));
        await grain.TryAdvanceLiveCheckpointAsync();

        var fence = grain.AppendFenceForTesting!;
        Assert.Multiple(() =>
        {
            Assert.That(deferred, Is.True, "precondition: the sibling was deferred before its link");
            Assert.That(fence.IsReachabilityProvenForTesting, Is.True, "a linked, reachable sibling was not proven again");
            Assert.That(fence.TryGetCutReader(out var reader) && !reader.Close1.IsSet, Is.True, "a witnessed close stayed pending");
        });
    }

    [Test]
    public async Task A_sibling_whose_own_link_is_witnessed_but_is_unreachable_latches()
    {
        // Detector 4 (ResolvedCloseStillDefers): a witnessed link means the keys moved since.
        var rig = PreArmRig();
        rig.Reachable = false;
        var grain = await BirthProvenSiblingWithPendingCloseAsync(rig);
        await rig.SetLeafMarkerAsync(m => HandOffMarkerCore.WitnessLink(m, ClaimlessDonor, 10));
        await SetStampedAsync(grain, "n", "1");

        await grain.TryAdvanceLiveCheckpointAsync();

        Assert.That(grain.AppendFenceForTesting!.IsUnreachable, Is.True, "an unreachable leaf whose own link was witnessed was not latched");
    }
}

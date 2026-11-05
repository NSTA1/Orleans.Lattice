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
}
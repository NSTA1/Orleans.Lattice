using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// a5e2e2ef's S1 on #5904: V3 dropped the link-batch guard, so while a split's link is held
/// descent still reaches the narrowed donor R for its unlinked sibling S's keys, and a stale
/// activation R' of R whose span predates the narrow can refresh and append an S key as an
/// R write. That positive is safe only by rule S: S's stamps carry R's donor close, the WAL
/// shard counts every R activation but the closing one in the stale high (B1), so the
/// bound reported to S lies at or above R''s write and S's advance cannot pass it.
/// </summary>
public partial class WalShardGrainTests
{
    private sealed class PreArmOnlyGate : IWalAppendFenceGate
    {
        public bool IsOpen => true;

        public bool IsPreArmOpen => true;
    }

    [Test]
    public async Task A_stale_donor_activation_refreshing_for_a_held_links_sibling_key_is_counted_in_the_siblings_bound()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());

        // R re-split with S's link held: its closing activation closed S's window under
        // generation 1; the shard then raised to 7. R', a stale activation of R, refreshed
        // to 7 (descent reaches R for S's key) and appended S's key as an R write.
        await grain.ClaimRoutingGenerationAsync(PreArmRoot, 7, CancellationToken.None);
        var stale = await grain.AppendStampedAsync(
            MakeEntry("s-key"), WalAppendStamp.PreArm(Donor, OtherToken) with { Initiator = PreArmRoot, Generation = 7 }, CancellationToken.None);

        // S carries R's donor close - its bound pending while the link is held - on its stamps.
        var sibling = new LeafAppendEpochFence(
            FenceLeaf,
            new PreArmOnlyGate(),
            new EnumerableMemoryGrainStorage(),
            _ => { },
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, "tree"),
            LatticeTenantLabel.ForTree("tree"));
        sibling.SetCarriedCloses([(new WalPendingClose(Donor, ClosingToken, PreArmRoot.Index, 1), new[] { false })], boundPending: true);
        var stamp = (await sibling.StampForAsync(grain, partition: 0, CancellationToken.None))!.Value with { Initiator = PreArmRoot, Generation = 7 };
        var appended = await grain.AppendStampedAsync(MakeEntry("s-own"), stamp, CancellationToken.None);
        sibling.OnStampedAppended(0, appended.UnstampedWatermark, stamp);
        var hasView = sibling.TryGetPreArmView(0, out var bound);

        Assert.Multiple(() =>
        {
            Assert.That(stamp.Close1.Donor, Is.EqualTo(Donor), "precondition: the sibling's stamp carries the donor's close");
            Assert.That(hasView, Is.True, "precondition: the report covered the carried close");
            Assert.That(bound, Is.GreaterThanOrEqualTo(stale.Offset), "the sibling's reported bound left out the stale donor activation's write, so its advance could pass it");
        });
    }
}

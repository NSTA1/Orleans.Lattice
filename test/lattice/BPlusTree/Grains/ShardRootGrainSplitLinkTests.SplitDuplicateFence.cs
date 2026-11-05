using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The shard root's half of a split link under #5837 (a5e2e2ef's rulings L1, L3 and C6's
/// read removal): a link batch holding a leaf split raises the routing generation once,
/// whatever this silo's view of the gate or the pre-arm class; it neither reads nor closes
/// the sibling's marker, which the donor closes itself with its deferred bound; and a raise
/// that fails leaves every donor of the batch unacknowledged, so the division re-surfaces.
/// </summary>
public sealed partial class ShardRootGrainSplitLinkTests
{
    [TestCase(true, true)]
    [TestCase(false, true)]
    [TestCase(false, false)]
    public async Task A_split_link_raises_the_routing_generation_once_per_batch_whatever_its_gate_view(bool preArmView, bool gateOpen)
    {
        // L1 (which settles #5840's S1).
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync(preArmView, gateOpen);
        ReturnSplit(h, donor, sibling);
        var before = (await HandOffMarkerRows.ReadShardAsync(storage, PreArmTreeId, 0)).State.RoutingGeneration;

        await h.Grain.SetAsync("c", [1]);

        var after = (await HandOffMarkerRows.ReadShardAsync(storage, PreArmTreeId, 0)).State.RoutingGeneration;
        Assert.That(after, Is.EqualTo(before + 1), "the link batch did not raise exactly once");
        await h.Leaf(donor).Received().AcknowledgeSplitLinkRecordedAsync(sibling);
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task A_split_link_neither_reads_nor_closes_its_sibling_marker(bool preArmView)
    {
        // C6 and rule S: the donor closes its sibling's window with its bound; the link only raises.
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync(preArmView);
        ReturnSplit(h, donor, sibling);
        storage.LeafMarkerReads = 0;

        await h.Grain.SetAsync("c", [1]);

        var linkReads = storage.LeafMarkerReads;
        var marker = (await HandOffMarkerRows.ReadLeafAsync(storage, sibling.GetGuidKey())).State;
        Assert.Multiple(() =>
        {
            Assert.That(linkReads, Is.Zero, "the link read a leaf's hand-off marker");
            Assert.That(storage.DeferredCloseWrites, Is.Empty, "the link closed the sibling's marker");
            Assert.That(marker.Open, Is.Not.Empty, "the link released the sibling the donor had not closed");
        });
    }

    [Test]
    public async Task A_split_link_whose_raise_fails_leaves_its_donor_unacknowledged()
    {
        // L3 / ObligationNotRedriven: the donor's obligation stays, so the link is re-driven.
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync();
        ReturnSplit(h, donor, sibling);
        storage.FailShardMarkerWrites = true;

        try
        {
            await h.Grain.SetAsync("c", [1]);
        }
        catch (InvalidOperationException)
        {
            // The link may surface the injected fault; what matters is the acknowledgement.
        }

        await h.Leaf(donor).DidNotReceive().AcknowledgeSplitLinkRecordedAsync(sibling);
    }
}

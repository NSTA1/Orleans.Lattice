using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The shard root's half of a split link under #5837 (a5e2e2ef's rulings L1, L3 and C6's
/// read removal): a link batch holding a leaf split raises the routing generation once,
/// whatever this silo's view of the gate or the pre-arm class; it never closes the
/// sibling's marker, which the donor closes itself with its deferred bound, but witnesses
/// the sibling's own link on it once the raise is durable (a5e2e2ef's B2 on #5866); and a
/// raise or a witness that fails leaves the donor unacknowledged, so the division re-surfaces.
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

    private static readonly Guid DonorClosingToken = new("c0000000-0000-0000-0000-000000005866");

    private static Task DonorClosesSiblingAsync(IGrainStorage storage, GrainId donor, GrainId sibling) =>
        HandOffMarkerRows.UpdateLeafAsync(storage, sibling.GetGuidKey(), m => HandOffMarkerCore.CloseWithDeferredBound(
            m, BPlusLeafGrain.SplitHandOffToken(donor.GetGuidKey()), [1], donor.GetGuidKey(), DonorClosingToken, 0, 1));

    [TestCase(true)]
    [TestCase(false)]
    public async Task A_split_link_never_closes_its_sibling_marker_and_witnesses_its_own_link_after_the_raise(bool preArmView)
    {
        // C6, rule S and a5e2e2ef's B2: the donor closes its sibling's window with its bound;
        // the link raises, then witnesses this sibling's own link on its marker.
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync(preArmView);
        await DonorClosesSiblingAsync(storage, donor, sibling);
        ReturnSplit(h, donor, sibling);
        storage.DeferredCloseWrites.Clear();

        await h.Grain.SetAsync("c", [1]);

        var raised = (await HandOffMarkerRows.ReadShardAsync(storage, PreArmTreeId, 0)).State.RoutingGeneration;
        var close = (await HandOffMarkerRows.ReadLeafAsync(storage, sibling.GetGuidKey())).State.Closes.Single(c => c.DonorLeaf == donor.GetGuidKey());
        Assert.Multiple(() =>
        {
            Assert.That(storage.DeferredCloseWrites, Is.Empty, "the link closed the sibling's marker");
            Assert.That(close.LinkedGeneration, Is.EqualTo(raised), "the sibling's link was not witnessed at the batch's raised generation");
            Assert.That(storage.LinkWitnessWrites, Is.Not.Empty.And.All.Matches<(long Witness, long DurableGeneration)>(w => w.DurableGeneration >= w.Witness),
                "a link was witnessed before its raise was durable");
        });
        await h.Leaf(donor).Received().AcknowledgeSplitLinkRecordedAsync(sibling);
    }

    [Test]
    public async Task A_split_link_whose_witness_fails_leaves_its_donor_unacknowledged_and_the_bound_pending()
    {
        // B2: a raise is not a witness; a failed witness re-surfaces the division (L3).
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync();
        await DonorClosesSiblingAsync(storage, donor, sibling);
        ReturnSplit(h, donor, sibling);
        storage.FailLeafMarkerWrites = true;

        try
        {
            await h.Grain.SetAsync("c", [1]);
        }
        catch (InvalidOperationException)
        {
            // The link may surface the injected fault; what matters is the acknowledgement.
        }

        storage.FailLeafMarkerWrites = false;
        var pending = new List<WalPendingClose>();
        var marker = (await HandOffMarkerRows.ReadLeafAsync(storage, sibling.GetGuidKey())).State;
        Assert.That(HandOffMarkerCore.CollectPendingDonorBounds(marker, null, null, pending), Is.EqualTo(1), "a failed witness resolved the bound");
        await h.Leaf(donor).DidNotReceive().AcknowledgeSplitLinkRecordedAsync(sibling);
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

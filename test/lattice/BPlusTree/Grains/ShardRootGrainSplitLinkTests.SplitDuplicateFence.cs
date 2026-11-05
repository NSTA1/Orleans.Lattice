using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The shard root's half of the split close under #5841 and review item S1: when it links a
/// split whose sibling's marker is still open, it fences the donor's other activations
/// (<see cref="LeafSplitDuplicateFence"/>) before the deferred close releases the sibling,
/// a failed fence leaves the sibling closed by nothing, and the close runs whatever this
/// silo's view of the pre-arm class - a donor that saw pre-arm leaves the close to the link.
/// </summary>
public sealed partial class ShardRootGrainSplitLinkTests
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task The_split_link_fences_the_donor_before_its_deferred_close(bool preArmView)
    {
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync(preArmView);
        ReturnSplit(h, donor, sibling);
        var closesWrittenAtFence = new List<int>();
        Guid? fenced = null;
        h.Grain.DonorSplitFenceForTesting = (s, shardFor, partitions, leaf, raised, claimed, ct) =>
        {
            closesWrittenAtFence.Add(storage.DeferredCloseWrites.Count);
            fenced = leaf;
            return LeafSplitDuplicateFence.FenceAsync(s, shardFor, partitions, leaf, raised, claimed, ct);
        };

        await h.Grain.SetAsync("c", [1]);

        Assert.Multiple(() =>
        {
            Assert.That(fenced, Is.EqualTo(donor.GetGuidKey()), "the link did not fence the donor");
            Assert.That(closesWrittenAtFence, Is.EqualTo(new[] { 0 }), "the deferred close was written before the donor was fenced");
            Assert.That(storage.DeferredCloseWrites, Is.Not.Empty, "the close did not land");
        });
    }

    [TestCase(true, true)]
    [TestCase(false, true)]
    [TestCase(false, false)]
    public async Task A_split_link_raises_the_routing_generation_once_per_batch_whatever_its_gate_view(bool preArmView, bool gateOpen)
    {
        // a5e2e2ef's ruling L1 on #5837 (which settles #5840's S1): a link batch holding a
        // leaf split raises once, even with no sibling marker open and whatever this shard
        // root's own view of the gate or the pre-arm class.
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync(preArmView, gateOpen);
        await HandOffMarkerRows.UpdateLeafAsync(storage, sibling.GetGuidKey(), m => HandOffMarkerCore.Close(m, BPlusLeafGrain.SplitHandOffToken(donor.GetGuidKey()), [0L]));
        ReturnSplit(h, donor, sibling);
        var before = (await HandOffMarkerRows.ReadShardAsync(storage, PreArmTreeId, 0)).State.RoutingGeneration;

        await h.Grain.SetAsync("c", [1]);

        var after = (await HandOffMarkerRows.ReadShardAsync(storage, PreArmTreeId, 0)).State.RoutingGeneration;
        Assert.Multiple(() =>
        {
            Assert.That(after, Is.EqualTo(before + 1), "the link batch did not raise exactly once");
            Assert.That(storage.DeferredCloseWrites, Is.Empty, "a sibling whose marker the donor already closed was closed again");
        });
        await h.Leaf(donor).Received().AcknowledgeSplitLinkRecordedAsync(sibling);
    }

    [Test]
    public async Task A_split_link_closes_an_open_sibling_marker_whatever_its_own_view_of_the_pre_arm_class()
    {
        // S1: a donor that saw the pre-arm class left its sibling's marker for the link. A
        // shard root whose own view is eager must still close it, or it stays open forever.
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync(preArmView: false);
        ReturnSplit(h, donor, sibling);

        await h.Grain.SetAsync("c", [1]);

        var marker = (await HandOffMarkerRows.ReadLeafAsync(storage, sibling.GetGuidKey())).State;
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Is.Empty, "the sibling's split marker was left open");
            Assert.That(marker.Closes.Any(c => c.DeferredGeneration > 0), Is.True, "the link did not close it by a deferred fence");
        });
    }

    [Test]
    public async Task A_split_link_whose_donor_fence_fails_leaves_the_sibling_open_and_the_donor_unacknowledged()
    {
        var (h, storage, _, donor, sibling) = await CreatePreArmSplitHarnessAsync();
        ReturnSplit(h, donor, sibling);
        h.Grain.DonorSplitFenceForTesting = (_, _, _, _, _, _, _) =>
            Task.FromException<long[]?>(new InvalidOperationException("injected: the donor's fence failed"));

        try
        {
            await h.Grain.SetAsync("c", [1]);
        }
        catch (InvalidOperationException)
        {
            // The link may surface the injected fault; what matters is what was written.
        }

        var marker = (await HandOffMarkerRows.ReadLeafAsync(storage, sibling.GetGuidKey())).State;
        Assert.Multiple(() =>
        {
            Assert.That(storage.DeferredCloseWrites, Is.Empty, "the sibling was released with the donor's duplicates unfenced");
            Assert.That(marker.Open, Is.Not.Empty, "the sibling's marker closed without a fence");
        });
        await h.Leaf(donor).DidNotReceive().AcknowledgeSplitLinkRecordedAsync(sibling);
    }
}

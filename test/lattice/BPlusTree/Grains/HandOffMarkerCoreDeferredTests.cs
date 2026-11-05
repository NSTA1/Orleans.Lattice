using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for deferred hand-off closes on a leaf marker (#5833, revision 2, C2, C3
/// and C5): a deferred close holds the owner at every partition until its arm records a
/// re-claim's head there; resolving at a generation covers every lower deferred close of
/// the same shard and never a later, higher one; and eviction never folds an unresolved
/// deferred head into the compacted heads, where it could never be resolved.
/// </summary>
[TestFixture]
public sealed class HandOffMarkerCoreDeferredTests
{
    [Test]
    public void A_deferred_close_holds_the_owner_at_every_partition_until_resolved()
    {
        var marker = new LeafHandOffState();
        HandOffMarkerCore.Open(marker, "split:d", null, null);

        var closed = HandOffMarkerCore.CloseDeferred(marker, "split:d", partitionCount: 2, shardIndex: 3, generation: 7);
        var pending = HandOffMarkerCore.TryGetPendingDeferred(marker, null, null, 0, out var shard, out var generation);

        Assert.Multiple(() =>
        {
            Assert.That(closed, Is.True);
            Assert.That(marker.Open, Is.Empty, "the close did not remove the open hand-off");
            Assert.That(HandOffMarkerCore.RequiredHead(marker, null, null, 0), Is.EqualTo(HandOffMarkerCore.DeferredHead));
            Assert.That(HandOffMarkerCore.RequiredHead(marker, null, null, 1), Is.EqualTo(HandOffMarkerCore.DeferredHead));
            Assert.That(pending, Is.True);
            Assert.That(shard, Is.EqualTo(3));
            Assert.That(generation, Is.EqualTo(7));
        });
    }

    [Test]
    public void Resolving_at_a_generation_records_the_head_for_lower_closes_only_at_that_partition()
    {
        // ReceiverArmBelowDeferredReclaimHead: the owner must replay to the re-claim's
        // head, and a close deferred above the resolved generation stays held.
        var marker = new LeafHandOffState();
        HandOffMarkerCore.CloseDeferred(marker, "split:a", 2, 0, 4);
        HandOffMarkerCore.CloseDeferred(marker, "split:b", 2, 0, 5);
        HandOffMarkerCore.ResolveDeferred(marker, partition: 0, shardIndex: 0, generation: 4, head: 40);
        var afterLow = HandOffMarkerCore.RequiredHead(marker, null, null, 0);

        HandOffMarkerCore.ResolveDeferred(marker, partition: 0, shardIndex: 0, generation: 5, head: 50);

        Assert.Multiple(() =>
        {
            Assert.That(afterLow, Is.EqualTo(HandOffMarkerCore.DeferredHead), "a re-claim at a lower generation resolved a higher deferred close");
            Assert.That(HandOffMarkerCore.RequiredHead(marker, null, null, 0), Is.EqualTo(50));
            Assert.That(HandOffMarkerCore.RequiredHead(marker, null, null, 1), Is.EqualTo(HandOffMarkerCore.DeferredHead),
                "resolving one partition resolved another");
            Assert.That(HandOffMarkerCore.TryGetPendingDeferred(marker, null, null, 0, out _, out _), Is.False);
            Assert.That(HandOffMarkerCore.TryGetPendingDeferred(marker, null, null, 1, out _, out var pendingGeneration), Is.True);
            Assert.That(pendingGeneration, Is.EqualTo(5), "the highest pending generation is the one to re-claim");
        });
    }

    [Test]
    public void Eviction_never_folds_an_unresolved_deferred_head_into_the_compacted_heads()
    {
        var marker = new LeafHandOffState();
        HandOffMarkerCore.CloseDeferred(marker, "split:old", 1, 0, 1);
        for (var i = 0; i < HandOffMarkerCore.MaxRetainedCloses; i++)
        {
            HandOffMarkerCore.Close(marker, $"reclaim:{i}", [i]);
        }

        Assert.Multiple(() =>
        {
            Assert.That(marker.CompactedHeads, Has.None.EqualTo(HandOffMarkerCore.DeferredHead),
                "an unresolved deferred head was compacted, where no arm can resolve it");
            Assert.That(HandOffMarkerCore.TryGetPendingDeferred(marker, null, null, 0, out _, out _), Is.True,
                "the deferred close was lost on eviction");
            Assert.That(marker.Closes, Has.Count.EqualTo(HandOffMarkerCore.MaxRetainedCloses));
        });
    }

    [Test]
    public void Eviction_folds_an_unresolved_deferred_close_into_a_later_one_of_its_shard()
    {
        var marker = new LeafHandOffState();
        HandOffMarkerCore.CloseDeferred(marker, "split:0", 1, 0, 1);
        for (var i = 1; i <= HandOffMarkerCore.MaxRetainedCloses; i++)
        {
            HandOffMarkerCore.CloseDeferred(marker, $"split:{i}", 1, 0, i + 1);
        }

        HandOffMarkerCore.ResolveDeferred(marker, 0, 0, HandOffMarkerCore.MaxRetainedCloses + 1, 99);

        Assert.Multiple(() =>
        {
            Assert.That(marker.Closes, Has.Count.EqualTo(HandOffMarkerCore.MaxRetainedCloses));
            Assert.That(HandOffMarkerCore.RequiredHead(marker, null, null, 0), Is.EqualTo(99),
                "a folded deferred close was not resolved by the later close's resolution");
        });
    }
}

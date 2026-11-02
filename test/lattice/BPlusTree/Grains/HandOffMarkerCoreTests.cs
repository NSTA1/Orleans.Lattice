using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the pure hand-off marker decisions (epic #5605, member 2; design
/// note section 2): opens are idempotent and never reopen a recorded close, a close
/// records the fence's heads over the hand-off's range, a disjoint owner is not held,
/// and evicting old closes only ever makes an owner replay further.
/// </summary>
[TestFixture]
public sealed class HandOffMarkerCoreTests
{
    [Test]
    public void An_open_is_idempotent_and_holds_only_overlapping_owners()
    {
        var marker = new ShardHandOffState();

        var first = HandOffMarkerCore.Open(marker, "rd:1", "f", "m");
        var again = HandOffMarkerCore.Open(marker, "rd:1", "a", "z");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(again, Is.False, "a resume re-opening its own marker changes nothing");
            Assert.That(marker.Open["rd:1"].LowInclusive, Is.EqualTo("f"));
            Assert.That(HandOffMarkerCore.IsHeldOpen(marker, "a", "g"), Is.True, "an overlapping leaf is held");
            Assert.That(HandOffMarkerCore.IsHeldOpen(marker, "m", "z"), Is.False,
                "a leaf disjoint from an open range-delete marker ignores it");
            Assert.That(HandOffMarkerCore.IsHeldOpen(marker, null, null), Is.True, "an unbounded leaf overlaps everything");
        });
    }

    [Test]
    public void A_close_records_the_fence_heads_over_the_hand_off_range_and_never_reopens()
    {
        var marker = new LeafHandOffState();
        HandOffMarkerCore.Open(marker, "split:d", null, null);

        var closed = HandOffMarkerCore.Close(marker, "split:d", [5, 9]);
        var closedAgain = HandOffMarkerCore.Close(marker, "split:d", [7, 12]);
        var reopened = HandOffMarkerCore.Open(marker, "split:d", null, null);

        Assert.Multiple(() =>
        {
            Assert.That(closed, Is.True);
            Assert.That(closedAgain, Is.False, "a re-driven close keeps the first close's heads");
            Assert.That(reopened, Is.False, "a late duplicate open of a recorded close does not hold the owner again");
            Assert.That(marker.Open, Is.Empty);
            Assert.That(HandOffMarkerCore.RequiredHeads(marker, "a", "b"), Is.EqualTo(new long[] { 5, 9 }));
        });
    }

    [Test]
    public void Required_heads_cover_only_overlapping_closes_and_every_evicted_one()
    {
        var marker = new ShardHandOffState();
        HandOffMarkerCore.Open(marker, "a", "a", "c");
        HandOffMarkerCore.Close(marker, "a", [10, 1]);
        HandOffMarkerCore.Open(marker, "b", "x", "z");
        HandOffMarkerCore.Close(marker, "b", [1, 20]);

        Assert.Multiple(() =>
        {
            Assert.That(HandOffMarkerCore.RequiredHeads(marker, "a", "b"), Is.EqualTo(new long[] { 10, 1 }));
            Assert.That(HandOffMarkerCore.RequiredHeads(marker, "y", null), Is.EqualTo(new long[] { 1, 20 }));
            Assert.That(HandOffMarkerCore.RequiredHeads(marker, "d", "e"), Is.Empty, "a disjoint owner needs no replay");
        });

        for (var i = 0; i < HandOffMarkerCore.MaxRetainedCloses; i++)
        {
            HandOffMarkerCore.Close(marker, $"later:{i}", [0, 0], "q", "r");
        }

        Assert.Multiple(() =>
        {
            Assert.That(marker.Closes, Has.Count.EqualTo(HandOffMarkerCore.MaxRetainedCloses));
            Assert.That(marker.CompactedHeads, Is.EqualTo(new long[] { 10, 20 }),
                "evicted closes fold into the compacted heads by a per-partition maximum");
            Assert.That(HandOffMarkerCore.RequiredHeads(marker, "d", "e"), Is.EqualTo(new long[] { 10, 20 }),
                "eviction only ever makes an owner replay further");
        });
    }

    [Test]
    public void A_close_of_a_token_never_opened_is_recorded_over_the_given_range()
    {
        var marker = new ShardHandOffState();

        var closed = HandOffMarkerCore.Close(marker, "fold:legacy", [4], "k", "p");

        Assert.Multiple(() =>
        {
            Assert.That(closed, Is.True);
            Assert.That(HandOffMarkerCore.RequiredHeads(marker, "m", "n"), Is.EqualTo(new long[] { 4 }));
        });
    }

    [Test]
    public void Max_heads_reads_a_missing_partition_as_minus_one()
    {
        Assert.That(HandOffMarkerCore.MaxHeads([3], [1, 2]), Is.EqualTo(new long[] { 3, 2 }));
    }
}

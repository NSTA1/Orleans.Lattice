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
    public void A_later_close_with_lower_heads_never_lowers_the_required_heads()
    {
        var marker = new ShardHandOffState();
        HandOffMarkerCore.Open(marker, "init", null, null);
        HandOffMarkerCore.Close(marker, "init", [8, 4]);
        HandOffMarkerCore.Open(marker, "window", "a", "c");
        HandOffMarkerCore.Close(marker, "window", [3, 6]);

        Assert.Multiple(() =>
        {
            Assert.That(HandOffMarkerCore.RequiredHeads(marker, "a", "b"), Is.EqualTo(new long[] { 8, 6 }),
                "a close with lower heads at a partition keeps the earlier close's head there");
            Assert.That(HandOffMarkerCore.RequiredHead(marker, "a", "b", 0), Is.EqualTo(8));
            Assert.That(HandOffMarkerCore.RequiredHead(marker, "a", "b", 1), Is.EqualTo(6));
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
    public void The_allocation_free_forms_agree_with_required_heads()
    {
        var marker = new ShardHandOffState();
        HandOffMarkerCore.Open(marker, "a", "a", "c");
        HandOffMarkerCore.Close(marker, "a", [10, 1]);
        HandOffMarkerCore.Open(marker, "b", "x", "z");
        HandOffMarkerCore.Close(marker, "b", [1, 20]);

        Assert.Multiple(() =>
        {
            foreach (var (low, high) in new[] { ("a", "b"), ("y", (string?)null), ("d", "e") })
            {
                var heads = HandOffMarkerCore.RequiredHeads(marker, low, high);
                for (var p = 0; p < 3; p++)
                {
                    Assert.That(HandOffMarkerCore.RequiredHead(marker, low, high, p), Is.EqualTo(HandOffMarkerCore.HeadAt(heads, p)),
                        $"[{low}, {high}) at partition {p}");
                }
            }

            Assert.That(HandOffMarkerCore.RequiredHeadsOrNull(marker, "a", "b"), Is.EqualTo(new long[] { 10, 1 }));
            Assert.That(HandOffMarkerCore.RequiredHeadsOrNull(marker, "d", "e"), Is.Null, "nothing bears on a disjoint owner, so no array");
            Assert.That(HandOffMarkerCore.HeadAt(null, 0), Is.EqualTo(-1L));
            Assert.That(HandOffMarkerCore.HeadAt([3], 1), Is.EqualTo(-1L));
        });
    }

    [Test]
    public void A_leaf_live_advance_view_projects_the_shard_marker_onto_the_leaf_range()
    {
        var marker = new ShardHandOffState { Initialised = true };
        HandOffMarkerCore.Open(marker, "open", "m", "p");
        HandOffMarkerCore.Open(marker, "closed", "a", "c");
        HandOffMarkerCore.Close(marker, "closed", [6]);

        var overlappingOpen = Orleans.Lattice.BPlusTree.LeafLiveAdvanceView.For(marker, "n", "o", reachable: true, orphanRepairInProgress: false);
        var overlappingClose = Orleans.Lattice.BPlusTree.LeafLiveAdvanceView.For(marker, "b", "d", reachable: false, orphanRepairInProgress: true);
        var disjoint = Orleans.Lattice.BPlusTree.LeafLiveAdvanceView.For(marker, "x", null, reachable: true, orphanRepairInProgress: false);

        Assert.Multiple(() =>
        {
            Assert.That(overlappingOpen.Initialised, Is.True);
            Assert.That(overlappingOpen.HeldOpen, Is.True);
            Assert.That(overlappingOpen.CloseHeads, Is.Null);
            Assert.That(overlappingClose.HeldOpen, Is.False);
            Assert.That(overlappingClose.CloseHeads, Is.EqualTo(new long[] { 6 }));
            Assert.That(overlappingClose.Reachable, Is.False);
            Assert.That(overlappingClose.OrphanRepairInProgress, Is.True);
            Assert.That(disjoint.HeldOpen, Is.False);
            Assert.That(disjoint.CloseHeads, Is.Null);
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

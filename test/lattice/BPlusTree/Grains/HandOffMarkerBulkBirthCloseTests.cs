using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The bulk birth close (#6166): a fence records a bulk switch's close on the markers of
/// the leaves it bore, and the shard marker keeps its bulk-switch closes apart, so its
/// retention folding a run's fences together never holds a leaf whose own marker holds
/// its close past that close. Model: <c>WalBulkSwitch</c>'s <c>BirthCloseHeld</c> and
/// <c>EvictionSparesBirthClose</c>.
/// </summary>
[TestFixture]
public sealed class HandOffMarkerBulkBirthCloseTests
{
    [Test]
    public void An_evicted_bulk_close_folds_into_the_compacted_bulk_heads_and_any_other_into_the_compacted_heads()
    {
        var marker = new ShardHandOffState();
        HandOffMarkerCore.Close(marker, "rd:old", [3]);
        for (var i = 0; i < HandOffMarkerCore.MaxRetainedCloses; i++)
        {
            HandOffMarkerCore.Open(marker, $"bulk:op{i}", $"k{i:D3}", null);
            HandOffMarkerCore.Close(marker, $"bulk:op{i}", [10 + i]);
        }

        HandOffMarkerCore.Open(marker, "bulk:last", "z", null);
        HandOffMarkerCore.Close(marker, "bulk:last", [100]);

        Assert.Multiple(() =>
        {
            Assert.That(marker.CompactedHeads, Is.EqualTo(new long[] { 3 }), "a non-bulk close folded anywhere but the compacted heads");
            Assert.That(marker.CompactedBulkHeads, Is.EqualTo(new long[] { 10 }), "an evicted bulk close was not kept apart");
            Assert.That(HandOffMarkerCore.RequiredHead(marker, "a", "b", 0), Is.EqualTo(10L),
                "an owner holding no birth close of its own was released from the evicted bulk close");
            Assert.That(HandOffMarkerCore.RequiredHeadsOrNull(marker, "a", "b", excludeBulk: true), Is.EqualTo(new long[] { 3 }),
                "a leaf holding its own birth close still read a bulk close of the shard");
        });
    }

    [Test]
    public void Required_heads_without_bulk_closes_keep_every_other_close_that_bears()
    {
        var marker = new ShardHandOffState();
        HandOffMarkerCore.Open(marker, "bulk:op", "k", null);
        HandOffMarkerCore.Close(marker, "bulk:op", [9]);
        HandOffMarkerCore.Open(marker, "rd:1", "a", "z");
        HandOffMarkerCore.Close(marker, "rd:1", [4]);

        Assert.Multiple(() =>
        {
            Assert.That(HandOffMarkerCore.RequiredHeadsOrNull(marker, "m", "n"), Is.EqualTo(new long[] { 9 }));
            Assert.That(HandOffMarkerCore.RequiredHeadsOrNull(marker, "m", "n", excludeBulk: true), Is.EqualTo(new long[] { 4 }));
            Assert.That(HandOffMarkerCore.AnyBulkCloseBears(marker, "a", "b"), Is.False, "a bulk close over [k, open) bore on [a, b)");
            Assert.That(HandOffMarkerCore.AnyBulkCloseBears(marker, "m", "n"), Is.True);
        });
    }

    [Test]
    public void A_bulk_birth_close_marks_the_leaf_and_a_re_close_raises_it_never_lowers_it()
    {
        var marker = new LeafHandOffState();

        HandOffMarkerCore.RecordBulkBirthClose(marker, "bulk:op", [5]);
        HandOffMarkerCore.RecordBulkBirthClose(marker, "bulk:op", [9]);
        var unchanged = HandOffMarkerCore.RecordBulkBirthClose(marker, "bulk:op", [7]);

        Assert.Multiple(() =>
        {
            Assert.That(marker.BulkBirthClose, Is.EqualTo(LeafHandOffState.BulkBirthCloseRecorded));
            Assert.That(HandOffMarkerCore.RequiredHead(marker, null, null, 0), Is.EqualTo(9L),
                "a retried switch's fence did not raise the leaf's own close (BirthCloseHeldRetryKeepsEarlierClose)");
            Assert.That(unchanged, Is.False, "a lower re-close changed the marker");
            Assert.That(marker.Closes, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void A_hand_off_onto_the_leaf_revokes_its_bulk_birth_close_for_good()
    {
        var before = new LeafHandOffState();
        HandOffMarkerCore.Open(before, "reclaim:x", null, null);
        HandOffMarkerCore.RecordBulkBirthClose(before, "bulk:op", [5]);

        var after = new LeafHandOffState();
        HandOffMarkerCore.RecordBulkBirthClose(after, "bulk:op", [5]);
        HandOffMarkerCore.Open(after, "reclaim:x", null, null);
        HandOffMarkerCore.Close(after, "reclaim:x", [6]);
        HandOffMarkerCore.RecordBulkBirthClose(after, "bulk:op2", [8]);

        Assert.Multiple(() =>
        {
            Assert.That(before.BulkBirthClose, Is.Not.EqualTo(LeafHandOffState.BulkBirthCloseRecorded),
                "a leaf receiving a hand-off at its birth close was marked");
            Assert.That(after.BulkBirthClose, Is.EqualTo(LeafHandOffState.BulkBirthCloseRevoked),
                "a widened leaf kept ignoring the shard's bulk closes");
        });
    }

    [Test]
    public void The_view_carries_the_heads_without_bulk_closes_and_reuses_them_when_none_bears()
    {
        var marker = new ShardHandOffState { Initialised = true };
        HandOffMarkerCore.Open(marker, "bulk:op", "k", null);
        HandOffMarkerCore.Close(marker, "bulk:op", [9]);

        var inBulkRange = LeafLiveAdvanceView.For(marker, "m", "n", reachable: true, orphanRepairInProgress: false);
        var elsewhere = LeafLiveAdvanceView.For(marker, "a", "b", reachable: true, orphanRepairInProgress: false);

        Assert.Multiple(() =>
        {
            Assert.That(inBulkRange.BulkSeparated, Is.True);
            Assert.That(inBulkRange.CloseHeads, Is.EqualTo(new long[] { 9 }));
            Assert.That(inBulkRange.NonBulkCloseHeads, Is.Null, "a bulk close bore on the leaf's heads without bulk closes");
            Assert.That(elsewhere.CloseHeads, Is.Null);
            Assert.That(elsewhere.NonBulkCloseHeads, Is.SameAs(elsewhere.CloseHeads));
        });
    }

    [Test]
    public void A_narrowed_leaf_takes_the_switch_close_only_when_no_bulk_append_bore_it_and_nothing_opened_onto_it()
    {
        // #6166: a leaf no bulk append bore - the shard's first - narrowed below a switch,
        // reads that switch's close as its own rather than the shard's folded bulk closes.
        var unborn = new LeafHandOffState();
        var born = new LeafHandOffState();
        HandOffMarkerCore.RecordBulkBirthClose(born, "bulk:birth", [5]);
        var opened = new LeafHandOffState();
        HandOffMarkerCore.Open(opened, "reclaim:x", null, null);
        HandOffMarkerCore.Close(opened, "reclaim:x", [4]);

        var recorded = HandOffMarkerCore.RecordNarrowedBirthClose(unborn, "bulk:narrow", [9]);
        var bornChanged = HandOffMarkerCore.RecordNarrowedBirthClose(born, "bulk:narrow", [9]);
        var openedChanged = HandOffMarkerCore.RecordNarrowedBirthClose(opened, "bulk:narrow", [9]);

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Is.True);
            Assert.That(unborn.BulkBirthClose, Is.EqualTo(LeafHandOffState.BulkBirthCloseRecorded));
            Assert.That(HandOffMarkerCore.RequiredHead(unborn, "a", "k", 0), Is.EqualTo(9L), "the narrowing switch's close does not hold the leaf");
            Assert.That(bornChanged, Is.False, "a later narrow raised a bulk-born leaf's own close past its switch's");
            Assert.That(HandOffMarkerCore.RequiredHead(born, "a", "k", 0), Is.EqualTo(5L));
            Assert.That(openedChanged, Is.False, "a leaf a hand-off opened onto, which may be widened, stopped reading the shard's bulk closes");
            Assert.That(opened.BulkBirthClose, Is.EqualTo(LeafHandOffState.BulkBirthCloseRevoked));
        });
    }

    [Test]
    public void Only_a_bulk_switch_token_records_a_bulk_birth_close()
    {
        Assert.Throws<ArgumentException>(() => HandOffMarkerCore.RecordBulkBirthClose(new LeafHandOffState(), "split:d", [1]));
    }
}

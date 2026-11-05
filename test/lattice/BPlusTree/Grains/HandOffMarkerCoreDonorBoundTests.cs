using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for a split donor's own close of its sibling's birth window (#5837, rule S
/// and a5e2e2ef's B2 on #5866): the close records its heads and the donor's deferred bound
/// in one change, the bound stays pending until the sibling's own link is witnessed on its
/// marker - whatever the shard's routing generation - and eviction never folds a bound away.
/// </summary>
[TestFixture]
public sealed class HandOffMarkerCoreDonorBoundTests
{
    private static readonly Guid Donor = new("d0000000-0000-0000-0000-000000005837");
    private static readonly Guid Token = new("c0000000-0000-0000-0000-000000005837");

    private static LeafHandOffState ClosedWithBound(int shard = 1, long generation = 4)
    {
        var marker = new LeafHandOffState();
        HandOffMarkerCore.Open(marker, "split:d", null, null);
        HandOffMarkerCore.CloseWithDeferredBound(marker, "split:d", [5, 6], Donor, Token, shard, generation);
        return marker;
    }

    [Test]
    public void A_donor_close_records_its_heads_and_bound_in_one_change()
    {
        var marker = new LeafHandOffState();
        HandOffMarkerCore.Open(marker, "split:d", null, null);

        var closed = HandOffMarkerCore.CloseWithDeferredBound(marker, "split:d", [5, 6], Donor, Token, 1, 4);
        var close = marker.Closes.Single();

        Assert.Multiple(() =>
        {
            Assert.That(closed, Is.True);
            Assert.That(marker.Open, Is.Empty, "the close left the birth window open");
            Assert.That(close.Heads, Is.EqualTo(new long[] { 5, 6 }));
            Assert.That((close.DonorLeaf, close.DonorToken, close.AdmittedShard, close.AdmittedGeneration), Is.EqualTo((Donor, Token, 1, 4L)));
        });
    }

    [Test]
    public void A_donor_bound_stays_pending_until_its_own_link_is_witnessed()
    {
        // B2 (BoundResolvesOnAnyRaise): no shard-wide generation resolves a bound; only the
        // witness of this sibling's own link does, once.
        var marker = ClosedWithBound(shard: 1, generation: 4);

        int Pending()
        {
            var pending = new List<WalPendingClose>();
            return HandOffMarkerCore.CollectPendingDonorBounds(marker, null, null, pending);
        }

        var unwitnessed = Pending();
        var own = HandOffMarkerCore.WitnessLink(marker, Donor, 7);
        var again = HandOffMarkerCore.WitnessLink(marker, Donor, 9);

        Assert.Multiple(() =>
        {
            Assert.That(unwitnessed, Is.EqualTo(1), "an unwitnessed bound was resolved");
            Assert.That(own, Is.True, "the sibling's own link was not witnessed");
            Assert.That(Pending(), Is.Zero, "a witnessed bound stayed pending");
            Assert.That(again, Is.False, "a witness was overwritten");
            Assert.That(marker.Closes.Single().LinkedGeneration, Is.EqualTo(7));
            Assert.That(marker.SiblingLinkedGeneration, Is.EqualTo(7));
        });
    }

    [Test]
    public void A_witness_that_lands_before_the_donors_close_resolves_the_close_written_after_it()
    {
        // a5e2e2ef's G1: the link batch witnesses while the donor's token is still open (a
        // re-driven or recovered close); the close written afterwards is resolved at once.
        var marker = new LeafHandOffState();
        HandOffMarkerCore.Open(marker, "split:d", null, null);

        var witnessed = HandOffMarkerCore.WitnessLink(marker, Donor, 7);
        HandOffMarkerCore.CloseWithDeferredBound(marker, "split:d", [5], Donor, Token, 1, 4);
        var pending = new List<WalPendingClose>();

        Assert.Multiple(() =>
        {
            Assert.That(witnessed, Is.True, "a witness before the close was dropped");
            Assert.That(marker.Closes.Single().LinkedGeneration, Is.EqualTo(7), "the later close did not inherit the witness");
            Assert.That(HandOffMarkerCore.CollectPendingDonorBounds(marker, null, null, pending), Is.Zero, "a witnessed sibling's later close stayed pending");
        });
    }

    [Test]
    public void A_pending_bound_names_the_donor_its_closing_activation_and_generation()
    {
        var marker = ClosedWithBound(shard: 1, generation: 4);
        var pending = new List<WalPendingClose>();

        HandOffMarkerCore.CollectPendingDonorBounds(marker, "a", "z", pending);

        Assert.That(pending, Is.EqualTo(new[] { new WalPendingClose(Donor, Token, 1, 4) }));
    }

    [Test]
    public void Eviction_never_folds_a_donor_bound_away()
    {
        var marker = ClosedWithBound();
        for (var i = 0; i < HandOffMarkerCore.MaxRetainedCloses + 4; i++)
        {
            var token = "reclaim:" + i;
            HandOffMarkerCore.Open(marker, token, null, null);
            HandOffMarkerCore.Close(marker, token, [i, i]);
        }

        var pending = new List<WalPendingClose>();
        HandOffMarkerCore.CollectPendingDonorBounds(marker, null, null, pending);

        Assert.Multiple(() =>
        {
            Assert.That(pending, Has.Count.EqualTo(1), "eviction folded a pending donor bound into the compacted heads");
            Assert.That(marker.Closes, Has.Count.LessThanOrEqualTo(HandOffMarkerCore.MaxRetainedCloses + 1));
        });
    }

    [Test]
    public void A_donor_bound_names_its_donor_and_closing_activation()
    {
        var marker = new LeafHandOffState();
        HandOffMarkerCore.Open(marker, "split:d", null, null);

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(() => HandOffMarkerCore.CloseWithDeferredBound(marker, "split:d", [1], Guid.Empty, Token, 0, 1));
            Assert.Throws<ArgumentException>(() => HandOffMarkerCore.CloseWithDeferredBound(marker, "split:d", [1], Donor, Guid.Empty, 0, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => HandOffMarkerCore.CloseWithDeferredBound(marker, "split:d", [1], Donor, Token, 0, 0));
        });
    }
}

using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for a split donor's own close of its sibling's birth window (#5837, rule S
/// and L2): the close records its heads and the donor's deferred bound in one change, the
/// bound stays pending until the shard's durable routing generation is strictly above
/// its admitted generation at its own shard, and eviction never folds a bound away.
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
        HandOffMarkerCore.CloseWithDonorBound(marker, "split:d", [5, 6], Donor, Token, shard, generation);
        return marker;
    }

    [Test]
    public void A_donor_close_records_its_heads_and_bound_in_one_change()
    {
        var marker = new LeafHandOffState();
        HandOffMarkerCore.Open(marker, "split:d", null, null);

        var closed = HandOffMarkerCore.CloseWithDonorBound(marker, "split:d", [5, 6], Donor, Token, 1, 4);
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
    public void A_donor_bound_resolves_only_strictly_above_its_admitted_generation()
    {
        // L2.
        var marker = ClosedWithBound(shard: 1, generation: 4);

        int Pending(long routingGeneration, int shardIndex = 1)
        {
            var pending = new List<WalPendingClose>();
            return HandOffMarkerCore.CollectPendingDonorBounds(marker, null, null, shardIndex, routingGeneration, pending);
        }

        Assert.Multiple(() =>
        {
            Assert.That(Pending(4), Is.EqualTo(1), "a bound resolved at its own admitted generation");
            Assert.That(Pending(0), Is.EqualTo(1), "a view from a build that sends no generation resolved a bound");
            Assert.That(Pending(5), Is.Zero, "a bound did not resolve above its admitted generation");
            Assert.That(Pending(9, shardIndex: 2), Is.EqualTo(1), "another shard root's generation resolved a bound");
        });
    }

    [Test]
    public void A_pending_bound_names_the_donor_its_closing_activation_and_generation()
    {
        var marker = ClosedWithBound(shard: 1, generation: 4);
        var pending = new List<WalPendingClose>();

        HandOffMarkerCore.CollectPendingDonorBounds(marker, "a", "z", 1, 4, pending);

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
        HandOffMarkerCore.CollectPendingDonorBounds(marker, null, null, 1, 4, pending);

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
            Assert.Throws<ArgumentException>(() => HandOffMarkerCore.CloseWithDonorBound(marker, "split:d", [1], Guid.Empty, Token, 0, 1));
            Assert.Throws<ArgumentException>(() => HandOffMarkerCore.CloseWithDonorBound(marker, "split:d", [1], Donor, Guid.Empty, 0, 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => HandOffMarkerCore.CloseWithDonorBound(marker, "split:d", [1], Donor, Token, 0, 0));
        });
    }
}

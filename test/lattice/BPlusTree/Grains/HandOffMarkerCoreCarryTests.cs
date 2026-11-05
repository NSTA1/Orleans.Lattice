using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A donor close's carriage after its sibling's link is witnessed (a5e2e2ef's G4 and
/// A1-A6 on #5866): the witness ends only the bound; the close stays carried at each WAL
/// partition until the sibling's arm there re-claims the close's shard's generation at or
/// above the witness, which raises the close's head there to the re-claim head and resolves
/// that partition alone. A re-claim below the witness, or a witness from another shard root,
/// resolves nothing.
/// </summary>
[TestFixture]
public sealed class HandOffMarkerCoreCarryTests
{
    private static readonly Guid Donor = new("d0000000-0000-0000-0000-00000000c4a1");
    private static readonly Guid Token = new("c0000000-0000-0000-0000-00000000c4a1");

    private static LeafHandOffState WitnessedClose(int admittedShard = 0, int linkedShard = 0, long witness = 7)
    {
        var marker = new LeafHandOffState { Initialised = true };
        HandOffMarkerCore.Open(marker, "split:d", null, null);
        HandOffMarkerCore.CloseWithDeferredBound(marker, "split:d", [5, 6], Donor, Token, admittedShard, 3);
        HandOffMarkerCore.WitnessLink(marker, Donor, linkedShard, witness);
        return marker;
    }

    private static List<(WalPendingClose Close, bool[] ResolvedAt)> Carried(LeafHandOffState marker, int partitions = 2)
    {
        var carried = new List<(WalPendingClose Close, bool[] ResolvedAt)>();
        HandOffMarkerCore.CollectCarriedDonorCloses(marker, null, null, partitions, carried);
        return carried;
    }

    [Test]
    public void A_witnessed_close_stays_carried_until_its_siblings_arm()
    {
        // G4 detector 1's precondition: the witness ends the bound, never the carriage.
        var marker = WitnessedClose();
        var pending = new List<WalPendingClose>();

        Assert.Multiple(() =>
        {
            Assert.That(HandOffMarkerCore.CollectPendingDonorBounds(marker, null, null, pending), Is.Zero, "precondition: the bound is witnessed");
            Assert.That(Carried(marker), Has.Count.EqualTo(1), "the witness ended the close's carriage");
            Assert.That(HandOffMarkerCore.TryGetCarriedWitness(marker, null, null, 0, out var shard, out var generation), Is.True);
            Assert.That((shard, generation), Is.EqualTo((0, 7L)), "the arm was not asked for the witness's shard and generation");
        });
    }

    [Test]
    public void An_arm_at_the_witness_raises_that_partitions_head_and_resolves_it_alone()
    {
        // G4 detector 2 (CarryEndsWithoutHeadRaise, CarryEndsLeafWide).
        var marker = WitnessedClose();

        var changed = HandOffMarkerCore.ResolveCarried(marker, 2, 0, 0, 7, 20);
        var close = marker.Closes.Single();

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.True);
            Assert.That(close.Heads, Is.EqualTo(new long[] { 20, 6 }), "the resolved partition's head was not raised to the re-claim head, or another was");
            Assert.That(close.CarryResolvedAt, Is.EqualTo(new[] { true, false }), "the arm resolved a partition it did not re-claim");
            Assert.That(Carried(marker), Has.Count.EqualTo(1), "one partition's arm ended the carriage everywhere");
            Assert.That(HandOffMarkerCore.TryGetCarriedWitness(marker, null, null, 0, out _, out _), Is.False, "a resolved partition still asked for an arm");
            Assert.That(HandOffMarkerCore.TryGetCarriedWitness(marker, null, null, 1, out _, out _), Is.True, "an unresolved partition stopped asking for an arm");
        });

        HandOffMarkerCore.ResolveCarried(marker, 2, 1, 0, 9, 4);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Closes.Single().Heads[1], Is.EqualTo(6), "a re-claim head below the close head lowered it");
            Assert.That(Carried(marker), Is.Empty, "a close resolved at every partition stayed carried");
        });
    }

    [Test]
    public void A_re_claim_below_the_witness_resolves_nothing()
    {
        // G4 detector 3 (CarryResolvedBelowWitness).
        var marker = WitnessedClose(witness: 7);

        var changed = HandOffMarkerCore.ResolveCarried(marker, 2, 0, 0, 6, 20);

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.False, "a re-claim below the witness resolved the carriage");
            Assert.That(marker.Closes.Single().Heads, Is.EqualTo(new long[] { 5, 6 }));
            Assert.That(Carried(marker), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void A_witness_from_another_shard_root_stays_carried_and_asks_for_no_arm()
    {
        // A1: a reshard or fold between the close and the link; conservative, and the
        // backstop must not loop on it.
        var marker = WitnessedClose(admittedShard: 0, linkedShard: 1);

        var changed = HandOffMarkerCore.ResolveCarried(marker, 2, 0, 0, 9, 20);

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.False, "a re-claim resolved a close witnessed by another shard root");
            Assert.That(HandOffMarkerCore.TryGetCarriedWitness(marker, null, null, 0, out _, out _), Is.False, "a shard mismatch asked for an arm");
            Assert.That(Carried(marker), Has.Count.EqualTo(1), "a shard mismatch ended the carriage");
        });
    }
}

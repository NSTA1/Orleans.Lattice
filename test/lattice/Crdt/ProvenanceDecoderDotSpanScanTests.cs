using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Guards the dot-scan span walks in the observed-remove and remove-wins
/// provenance decoders. <c>OrSetDot</c> is a struct, so walking a
/// <c>List&lt;OrSetDot&gt;</c> by indexer copies it per read and re-reads the
/// mutable <c>Count</c> each iteration; the decoders now walk a span over the
/// list instead. The walks are read-only, so the length cannot change while the
/// span is alive, and the rewrite is required to be exactly answer-preserving.
/// <para>
/// The cases that matter are the ones a span rewrite could silently break: an
/// empty list, a single dot, a hit at each end of a long list, and the
/// counter-collision shape where two replicas share a counter - the case the
/// shared-replica precondition exists to reject, and the one a length or index
/// slip would turn into wrongly cancelling a live dot.
/// </para>
/// </summary>
[TestFixture]
public sealed class ProvenanceDecoderDotSpanScanTests
{
    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    private static string ElementKey(int index) => Convert.ToBase64String(BitConverter.GetBytes(index));

    private static List<OrSetDot> Dots(string replica, int count, long baseCounter = 1)
        => [.. Enumerable.Range(0, count).Select(i => new OrSetDot { ReplicaId = replica, Counter = baseCounter + i })];

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(8)]
    [TestCase(64)]
    public void OrSet_state_decode_is_stable_across_dot_list_lengths(int tombstones)
    {
        var set = new OrSet();
        var key = ElementKey(0);
        set.Adds[key] = Dots(ReplicaA, 3, baseCounter: 100);
        if (tombstones > 0) set.Tombstones[key] = Dots(ReplicaA, tombstones);

        var events = OrSetProvenanceDecoder.Instance.DecodeState(set);

        // Three real adds, plus one synthesized Added and one Removed for every
        // tombstoned dot whose exact dot the compacted add list no longer holds.
        Assert.Multiple(() =>
        {
            Assert.That(
                events.Count(e => e.Kind == CrdtMemberChangeKind.Removed),
                Is.EqualTo(tombstones));
            Assert.That(
                events.Count(e => e.Kind == CrdtMemberChangeKind.Added),
                Is.EqualTo(3 + tombstones));
            Assert.That(events.Select(e => e.Ordinal), Is.Ordered);
        });
    }

    [Test]
    public void OrSet_state_decode_does_not_synthesize_an_add_for_a_tombstone_already_in_the_add_list()
    {
        var set = new OrSet();
        var key = ElementKey(0);
        // Every tombstoned dot is still present in the add list, so the exact
        // containment scan must find each one and synthesize nothing.
        set.Adds[key] = Dots(ReplicaA, 16);
        set.Tombstones[key] = Dots(ReplicaA, 16);

        var events = OrSetProvenanceDecoder.Instance.DecodeState(set);

        Assert.Multiple(() =>
        {
            Assert.That(events.Count(e => e.Kind == CrdtMemberChangeKind.Added), Is.EqualTo(16));
            Assert.That(events.Count(e => e.Kind == CrdtMemberChangeKind.Removed), Is.EqualTo(16));
        });
    }

    [Test]
    public void OrSet_current_value_keeps_a_live_dot_whose_counter_collides_across_replicas()
    {
        var set = new OrSet();
        var key = ElementKey(0);
        // A long single-replica tombstone list clears the decoder's index
        // threshold, so the gated collapse runs; the live add is on another
        // replica at a counter the tombstones cover, which only the
        // shared-replica precondition keeps alive.
        set.Adds[key] = [new OrSetDot { ReplicaId = ReplicaB, Counter = 5 }];
        set.Tombstones[key] = Dots(ReplicaA, 32);

        var members = OrSetProvenanceDecoder.Instance.DecodeCurrentValue(set);

        Assert.Multiple(() =>
        {
            Assert.That(members, Has.Count.EqualTo(1));
            Assert.That(members[0].ReplicaId, Is.EqualTo(ReplicaB));
            Assert.That(members[0].Ordinal, Is.EqualTo(5));
        });
    }

    [Test]
    public void OrSet_current_value_drops_an_element_every_dot_of_which_is_covered()
    {
        var set = new OrSet();
        var key = ElementKey(0);
        set.Adds[key] = Dots(ReplicaA, 4);
        set.Tombstones[key] = Dots(ReplicaA, 32);

        Assert.That(OrSetProvenanceDecoder.Instance.DecodeCurrentValue(set), Is.Empty);
    }

    [Test]
    public void OrSet_current_value_keeps_the_highest_surviving_dot_past_a_long_tombstone_list()
    {
        var set = new OrSet();
        var key = ElementKey(0);
        set.Adds[key] = [.. Dots(ReplicaA, 4), new OrSetDot { ReplicaId = ReplicaA, Counter = 500 }];
        set.Tombstones[key] = Dots(ReplicaA, 32);

        var members = OrSetProvenanceDecoder.Instance.DecodeCurrentValue(set);

        Assert.Multiple(() =>
        {
            Assert.That(members, Has.Count.EqualTo(1));
            Assert.That(members[0].Ordinal, Is.EqualTo(500));
        });
    }

    [Test]
    public void RwSet_current_value_excludes_an_element_whose_remove_survives_a_long_tombstone_list()
    {
        var set = new RwSet();
        var key = ElementKey(0);
        set.Adds[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 1 }];
        // A remove above every tombstoned counter survives, so remove-wins
        // excludes the element.
        set.Removes[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 500 }];
        set.Tombstones[key] = Dots(ReplicaA, 32);

        Assert.That(RwSetProvenanceDecoder.Instance.DecodeCurrentValue(set), Is.Empty);
    }

    [Test]
    public void RwSet_current_value_includes_an_element_whose_removes_are_all_covered()
    {
        var set = new RwSet();
        var key = ElementKey(0);
        set.Adds[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 1 }];
        set.Removes[key] = [.. Dots(ReplicaA, 2)];
        set.Tombstones[key] = Dots(ReplicaA, 32);

        var members = RwSetProvenanceDecoder.Instance.DecodeCurrentValue(set);

        Assert.That(members, Has.Count.EqualTo(1));
    }

    [Test]
    public void RwSet_current_value_keeps_a_remove_whose_counter_collides_across_replicas()
    {
        var set = new RwSet();
        var key = ElementKey(0);
        set.Adds[key] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 1 }];
        // The remove is on replica B; a counter-only cancellation test would
        // wrongly cancel it against replica A's tombstones and wrongly keep the
        // element present.
        set.Removes[key] = [new OrSetDot { ReplicaId = ReplicaB, Counter = 5 }];
        set.Tombstones[key] = Dots(ReplicaA, 32);

        Assert.That(RwSetProvenanceDecoder.Instance.DecodeCurrentValue(set), Is.Empty);
    }

    [Test]
    public void A_tombstone_list_spanning_replicas_falls_back_to_the_per_dot_coverage_scan()
    {
        var set = new OrSet();
        var key = ElementKey(0);
        set.Adds[key] = [new OrSetDot { ReplicaId = ReplicaB, Counter = 40 }];
        // Long enough to clear the index threshold but spanning two replicas, so
        // the precondition fails and the scan is taken instead.
        set.Tombstones[key] = [.. Dots(ReplicaA, 32), new OrSetDot { ReplicaId = ReplicaB, Counter = 20 }];

        var members = OrSetProvenanceDecoder.Instance.DecodeCurrentValue(set);

        Assert.Multiple(() =>
        {
            Assert.That(members, Has.Count.EqualTo(1));
            Assert.That(members[0].ReplicaId, Is.EqualTo(ReplicaB));
        });
    }
}

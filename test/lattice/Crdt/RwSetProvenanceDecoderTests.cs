using System.Text;

namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class RwSetProvenanceDecoderTests
{
    private static RwSetProvenanceDecoder Decoder => RwSetProvenanceDecoder.Instance;

    private static byte[] E(string s) => Encoding.UTF8.GetBytes(s);

    private static OrSetDeltaDot Dot(string element, string replica, long counter) =>
        new() { Element = E(element), ReplicaId = replica, Counter = counter };

    private static RwSetDelta Delta(
        OrSetDeltaDot[]? adds = null,
        OrSetDeltaDot[]? removes = null,
        OrSetDeltaDot[]? tombstones = null) => new()
    {
        Adds = adds ?? Array.Empty<OrSetDeltaDot>(),
        Removes = removes ?? Array.Empty<OrSetDeltaDot>(),
        Tombstones = tombstones ?? Array.Empty<OrSetDeltaDot>(),
    };

    [Test]
    public void Mode_is_rwset()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.RwSet));
    }

    [Test]
    public void DecodeDeltas_null_throws()
    {
        Assert.That(() => Decoder.DecodeDeltas(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeState_null_throws()
    {
        Assert.That(() => Decoder.DecodeState(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => Decoder.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeDeltas_empty_sequence_yields_no_events()
    {
        Assert.That(Decoder.DecodeDeltas(Array.Empty<CrdtProvenanceDelta>()), Is.Empty);
    }

    [Test]
    public void DecodeState_empty_set_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new RwSet()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_empty_set_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new RwSet()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_add_yields_added_remove_yields_removed()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                adds: new[] { Dot("x", "r1", 2) },
                removes: new[] { Dot("y", "r2", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(2));
        var added = events.Single(e => e.Kind == CrdtMemberChangeKind.Added);
        var removed = events.Single(e => e.Kind == CrdtMemberChangeKind.Removed);
        Assert.Multiple(() =>
        {
            Assert.That(added.ReplicaId, Is.EqualTo("r1"));
            Assert.That(Encoding.UTF8.GetString(added.Element), Is.EqualTo("x"));
            Assert.That(removed.ReplicaId, Is.EqualTo("r2"));
            Assert.That(Encoding.UTF8.GetString(removed.Element), Is.EqualTo("y"));
        });
    }

    [Test]
    public void DecodeDeltas_does_not_emit_for_tombstones()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                adds: new[] { Dot("x", "r1", 2) },
                tombstones: new[] { Dot("x", "r2", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        // Only the add surfaces; the observed-add tombstone is bookkeeping.
        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
    }

    [Test]
    public void DecodeState_concurrent_remove_wins_but_both_events_present()
    {
        // Remove-wins: a concurrent remove the add has not observed keeps the
        // element out, but the provenance stream records both operations.
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);
        set.Removes["eA=="] = new List<OrSetDot> { new() { ReplicaId = "r2", Counter = 1 } };

        var events = Decoder.DecodeState(set);

        Assert.That(set.Contains(E("x")), Is.False);
        Assert.That(events.Select(e => (e.Kind, e.ReplicaId)), Is.EquivalentTo(new[]
        {
            (CrdtMemberChangeKind.Added, "r1"),
            (CrdtMemberChangeKind.Removed, "r2"),
        }));
    }

    [Test]
    public void DecodeState_emits_removed_for_every_remove_even_when_tombstoned()
    {
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);
        set.Remove(E("x"), "r2", 2);
        set.Add(E("x"), "r1", 3); // observes and tombstones (r2, 2)

        var events = Decoder.DecodeState(set);

        Assert.That(set.Contains(E("x")), Is.True);
        Assert.That(events.Count(e => e.Kind == CrdtMemberChangeKind.Removed), Is.EqualTo(1));
        Assert.That(events.Count(e => e.Kind == CrdtMemberChangeKind.Added), Is.EqualTo(1),
            "Compaction retains the latest same-replica add while preserving the remove event.");
    }

    [Test]
    public void DecodeState_order_is_deterministic()
    {
        var set = new RwSet();
        set.Add(E("b"), "rB", 1);
        set.Add(E("a"), "rA", 1);

        var first = Decoder.DecodeState(set);
        var second = Decoder.DecodeState(set);

        Assert.That(
            first.Select(e => Encoding.UTF8.GetString(e.Element)),
            Is.EqualTo(second.Select(e => Encoding.UTF8.GetString(e.Element))));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);

        Assert.That(Decoder.DecodeState(set).All(e => e.WallClock is null), Is.True);
    }

    [Test]
    public void DecodeDeltas_carries_wall_clock_stamp()
    {
        var stamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(adds: new[] { Dot("x", "r1", 1) }), stamp),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events[0].WallClock, Is.EqualTo(stamp));
    }

    [Test]
    public void DecodeCurrentValue_present_element_yields_member()
    {
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);

        var members = Decoder.DecodeCurrentValue(set);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(members[0].Element), Is.EqualTo("x"));
    }

    [Test]
    public void DecodeCurrentValue_removed_element_is_excluded()
    {
        // Remove-wins: a concurrent remove the add has not observed.
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);
        set.Removes["eA=="] = new List<OrSetDot> { new() { ReplicaId = "r2", Counter = 1 } };

        var members = Decoder.DecodeCurrentValue(set);

        Assert.That(members, Is.Empty);
    }
}

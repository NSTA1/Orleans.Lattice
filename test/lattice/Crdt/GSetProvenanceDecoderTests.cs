namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class GSetProvenanceDecoderTests
{
    private static GSetProvenanceDecoder Decoder => GSetProvenanceDecoder.Instance;

    private static GSetDelta Delta(params byte[][] adds) => new() { Adds = adds };

    [Test]
    public void Mode_is_gset()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.GSet));
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
        Assert.That(Decoder.DecodeState(new GSet()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_empty_set_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new GSet()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_add_yields_added_event_with_empty_replica_and_zero_ordinal()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(new byte[] { 1 })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(events[0].Element, Is.EqualTo(new byte[] { 1 }));
            Assert.That(events[0].ReplicaId, Is.EqualTo(string.Empty));
            Assert.That(events[0].Ordinal, Is.EqualTo(0L));
            Assert.That(events[0].WallClock, Is.Null);
        });
    }

    [Test]
    public void DecodeDeltas_propagates_wall_clock_stamp()
    {
        var stamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var deltas = new[] { new CrdtProvenanceDelta(Delta(new byte[] { 7 }), stamp) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events[0].WallClock, Is.EqualTo(stamp));
    }

    [Test]
    public void DecodeDeltas_preserves_operation_order_across_deltas()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(new byte[] { 1 }, new byte[] { 2 })),
            new CrdtProvenanceDelta(Delta(new byte[] { 3 })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(e => e.Element), Is.EqualTo(new[]
        {
            new byte[] { 1 },
            new byte[] { 2 },
            new byte[] { 3 },
        }));
    }

    [Test]
    public void DecodeState_yields_one_added_event_per_element()
    {
        var set = new GSet();
        set.Add(new byte[] { 1 });
        set.Add(new byte[] { 2 });

        var events = Decoder.DecodeState(set);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added), Is.True);
        Assert.That(events.All(e => e.ReplicaId == string.Empty && e.Ordinal == 0 && e.WallClock is null), Is.True);
    }

    [Test]
    public void DecodeCurrentValue_yields_one_member_per_live_element()
    {
        var set = new GSet();
        set.Add(new byte[] { 1 });
        set.Add(new byte[] { 2 });

        var members = Decoder.DecodeCurrentValue(set);

        Assert.That(members, Has.Count.EqualTo(2));
        Assert.That(members.All(m => m.ReplicaId == string.Empty && m.Ordinal == 0), Is.True);
    }
}

using System.Text;

namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class PnCounterProvenanceDecoderTests
{
    private static PnCounterProvenanceDecoder Decoder => PnCounterProvenanceDecoder.Instance;

    private static PnCounterDelta Delta(
        (string Replica, long Value)[]? increments = null,
        (string Replica, long Value)[]? decrements = null) => new()
    {
        Increments = ToDict(increments),
        Decrements = ToDict(decrements),
    };

    private static Dictionary<string, long> ToDict((string Replica, long Value)[]? pairs)
    {
        var d = new Dictionary<string, long>();
        if (pairs is not null)
        {
            foreach (var (r, v) in pairs) d[r] = v;
        }
        return d;
    }

    [Test]
    public void Mode_is_pncounter()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.PnCounter));
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
    public void DecodeDeltas_empty_sequence_yields_no_events()
    {
        Assert.That(Decoder.DecodeDeltas(Array.Empty<CrdtProvenanceDelta>()), Is.Empty);
    }

    [Test]
    public void DecodeState_empty_counter_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new PnCounter()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_positive_total_yields_added_with_replica_bytes_and_magnitude_ordinal()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(increments: new[] { ("r1", 7L) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(events[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(events[0].Element, Is.EqualTo(Encoding.UTF8.GetBytes("r1")));
            Assert.That(events[0].Ordinal, Is.EqualTo(7L)); // magnitude, not a dot
        });
    }

    [Test]
    public void DecodeState_positive_and_negative_both_represented()
    {
        var counter = new PnCounter();
        counter.Increment("r1", 5);
        counter.Decrement("r1", 2);

        var events = Decoder.DecodeState(counter);

        Assert.That(events, Has.Count.EqualTo(2));
        var added = events.Single(e => e.Kind == CrdtMemberChangeKind.Added);
        var removed = events.Single(e => e.Kind == CrdtMemberChangeKind.Removed);
        Assert.Multiple(() =>
        {
            Assert.That(added.Ordinal, Is.EqualTo(5L));
            Assert.That(removed.Ordinal, Is.EqualTo(2L));
        });
    }

    [Test]
    public void DecodeState_skips_zero_magnitude_entries()
    {
        var counter = new PnCounter();
        counter.Increment("r1", 0); // no-op, never stored
        counter.Increment("r2", 3);

        var events = Decoder.DecodeState(counter);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].ReplicaId, Is.EqualTo("r2"));
    }

    [Test]
    public void DecodeState_multiple_replicas_ordered_deterministically_by_replica()
    {
        var counter = new PnCounter();
        counter.Increment("rB", 1);
        counter.Increment("rA", 1);
        counter.Increment("rC", 1);

        var events = Decoder.DecodeState(counter);

        Assert.That(events.Select(e => e.ReplicaId), Is.EqualTo(new[] { "rA", "rB", "rC" }));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var counter = new PnCounter();
        counter.Increment("r1", 1);

        Assert.That(Decoder.DecodeState(counter).All(e => e.WallClock is null), Is.True);
    }

    [Test]
    public void DecodeDeltas_associates_wall_clock_when_supplied()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 10, Counter = 1 };
        var deltas = new[] { new CrdtProvenanceDelta(Delta(increments: new[] { ("r1", 1L) }), hlc) };

        Assert.That(Decoder.DecodeDeltas(deltas)[0].WallClock, Is.EqualTo(hlc));
    }

    // ---- current-value (net total) path ----

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => Decoder.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_empty_counter_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new PnCounter()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_yields_single_net_value_member()
    {
        var counter = new PnCounter();
        counter.Increment("r1", 5);
        counter.Decrement("r2", 2);

        var members = Decoder.DecodeCurrentValue(counter);

        // A single member carrying the net total (5 - 2 = 3), not per-replica
        // contributions, with no single authoring replica.
        Assert.That(members, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(members[0].Element), Is.EqualTo("3"));
            Assert.That(members[0].Ordinal, Is.EqualTo(3L));
            Assert.That(members[0].ReplicaId, Is.Empty);
        });
    }

    [Test]
    public void DecodeCurrentValue_negative_net_value_is_rendered()
    {
        var counter = new PnCounter();
        counter.Increment("r1", 1);
        counter.Decrement("r2", 4);

        var members = Decoder.DecodeCurrentValue(counter);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(members[0].Element), Is.EqualTo("-3"));
        Assert.That(members[0].Ordinal, Is.EqualTo(-3L));
    }
}

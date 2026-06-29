using System.Text;

namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class VersionVectorProvenanceDecoderTests
{
    private static VersionVectorProvenanceDecoder Decoder => VersionVectorProvenanceDecoder.Instance;

    private static VersionVectorDelta Delta(params (string Replica, HybridLogicalClock Clock)[] entries)
    {
        var d = new Dictionary<string, HybridLogicalClock>();
        foreach (var (r, c) in entries) d[r] = c;
        return new VersionVectorDelta { Entries = d };
    }

    private static HybridLogicalClock Hlc(long ticks, int counter) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void Mode_is_versionvector()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.VersionVector));
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
    public void DecodeState_empty_vector_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new VersionVector()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_entry_yields_added_with_replica_bytes_counter_ordinal_and_hlc()
    {
        var clock = Hlc(1000, 4);
        var deltas = new[] { new CrdtProvenanceDelta(Delta(("r1", clock))) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(events[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(events[0].Element, Is.EqualTo(Encoding.UTF8.GetBytes("r1")));
            Assert.That(events[0].Ordinal, Is.EqualTo(4L));
            Assert.That(events[0].WallClock, Is.EqualTo(clock));
        });
    }

    [Test]
    public void DecodeState_emits_only_added_events()
    {
        var vector = new VersionVector();
        vector.Tick("r1");
        vector.Tick("r2");

        var events = Decoder.DecodeState(vector);

        Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added), Is.True);
    }

    [Test]
    public void DecodeState_per_replica_entries_carry_their_own_clock()
    {
        var vector = new VersionVector();
        var c1 = vector.Tick("r1");
        var c2 = vector.Tick("r2");

        var events = Decoder.DecodeState(vector);

        var e1 = events.Single(e => e.ReplicaId == "r1");
        var e2 = events.Single(e => e.ReplicaId == "r2");
        Assert.Multiple(() =>
        {
            Assert.That(e1.WallClock, Is.EqualTo(c1));
            Assert.That(e2.WallClock, Is.EqualTo(c2));
        });
    }

    [Test]
    public void DecodeState_order_is_deterministic_by_replica()
    {
        var vector = new VersionVector();
        vector.Tick("rC");
        vector.Tick("rA");
        vector.Tick("rB");

        var events = Decoder.DecodeState(vector);

        Assert.That(events.Select(e => e.ReplicaId), Is.EqualTo(new[] { "rA", "rB", "rC" }));
    }

    // ---- current-value (frontier per replica) path ----

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => Decoder.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_empty_vector_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new VersionVector()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_yields_one_member_per_replica_ordered()
    {
        var vector = new VersionVector();
        vector.Tick("rC");
        vector.Tick("rA");
        vector.Tick("rB");

        var members = Decoder.DecodeCurrentValue(vector);

        Assert.That(members.Select(m => m.ReplicaId), Is.EqualTo(new[] { "rA", "rB", "rC" }));
        Assert.That(members.Select(m => Encoding.UTF8.GetString(m.Element)),
            Is.EqualTo(new[] { "rA", "rB", "rC" }));
    }
}

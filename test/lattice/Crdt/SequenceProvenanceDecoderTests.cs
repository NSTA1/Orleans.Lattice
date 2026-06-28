namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class SequenceProvenanceDecoderTests
{
    private static readonly byte[] A = "a"u8.ToArray();
    private static readonly byte[] B = "b"u8.ToArray();

    private static SequenceProvenanceDecoder Decoder => SequenceProvenanceDecoder.Instance;

    private static RgaDeltaNode Node(byte[] value, string replica, long counter) => new()
    {
        ReplicaId = replica,
        Counter = counter,
        ParentDot = Rga.Root,
        Value = value,
    };

    private static RgaDelta Delta(RgaDeltaNode[]? inserts = null, OrSetDot[]? tombstones = null) => new()
    {
        Inserts = inserts ?? Array.Empty<RgaDeltaNode>(),
        Tombstones = tombstones ?? Array.Empty<OrSetDot>(),
    };

    [Test]
    public void Mode_is_sequence()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.Sequence));
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
    public void DecodeState_empty_sequence_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new Rga()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_insert_yields_added_with_value_bytes()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(inserts: new[] { Node(A, "r1", 1) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Element, Is.EqualTo(A));
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(events[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(events[0].Ordinal, Is.EqualTo(1L));
        });
    }

    [Test]
    public void DecodeDeltas_tombstone_yields_removed_with_empty_element()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(tombstones: new[] { new OrSetDot { ReplicaId = "r1", Counter = 5 } })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
            Assert.That(events[0].Element, Is.Empty);
            Assert.That(events[0].Ordinal, Is.EqualTo(5L));
        });
    }

    [Test]
    public void DecodeDeltas_preserves_insert_then_remove_order()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(inserts: new[] { Node(A, "r1", 1) })),
            new CrdtProvenanceDelta(Delta(tombstones: new[] { new OrSetDot { ReplicaId = "r1", Counter = 1 } })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            CrdtMemberChangeKind.Added,
            CrdtMemberChangeKind.Removed,
        }));
    }

    [Test]
    public void DecodeDeltas_associates_wall_clock_when_supplied()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 99, Counter = 1 };
        var deltas = new[] { new CrdtProvenanceDelta(Delta(inserts: new[] { Node(A, "r1", 1) }), hlc) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events[0].WallClock, Is.EqualTo(hlc));
    }

    [Test]
    public void DecodeState_live_node_yields_added_tombstoned_yields_removed()
    {
        var rga = new Rga();
        var d1 = rga.InsertAfter(Rga.Root, "r1", A);
        rga.InsertAfter(d1, "r1", B);
        rga.Remove(d1);

        var events = Decoder.DecodeState(rga);

        Assert.That(events, Has.Count.EqualTo(2));
        var removed = events.Single(e => e.Kind == CrdtMemberChangeKind.Removed);
        var added = events.Single(e => e.Kind == CrdtMemberChangeKind.Added);
        Assert.Multiple(() =>
        {
            Assert.That(removed.Element, Is.EqualTo(A));
            Assert.That(added.Element, Is.EqualTo(B));
        });
    }

    [Test]
    public void DecodeState_concurrent_inserts_from_two_replicas_both_represented()
    {
        var rga = new Rga();
        rga.InsertAfter(Rga.Root, "r1", A);
        rga.InsertAfter(Rga.Root, "r2", B);

        var events = Decoder.DecodeState(rga);

        Assert.That(events.Where(e => e.Kind == CrdtMemberChangeKind.Added).Select(e => e.ReplicaId),
            Is.EquivalentTo(new[] { "r1", "r2" }));
    }

    [Test]
    public void DecodeState_order_is_deterministic_by_replica_then_ordinal()
    {
        var rga = new Rga();
        rga.InsertAfter(Rga.Root, "r2", B);
        rga.InsertAfter(Rga.Root, "r1", A);

        var first = Decoder.DecodeState(rga);
        var second = Decoder.DecodeState(rga);

        Assert.That(first.Select(e => (e.ReplicaId, e.Ordinal)),
            Is.EqualTo(second.Select(e => (e.ReplicaId, e.Ordinal))));
        Assert.That(first[0].ReplicaId, Is.EqualTo("r1"));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var rga = new Rga();
        rga.InsertAfter(Rga.Root, "r1", A);

        Assert.That(Decoder.DecodeState(rga).All(e => e.WallClock is null), Is.True);
    }
}

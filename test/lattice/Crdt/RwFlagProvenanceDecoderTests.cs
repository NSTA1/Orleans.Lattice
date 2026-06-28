namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class RwFlagProvenanceDecoderTests
{
    private static RwFlagProvenanceDecoder Decoder => RwFlagProvenanceDecoder.Instance;

    private static OrSetDot Dot(string replica, long counter) => new() { ReplicaId = replica, Counter = counter };

    private static RwFlagDelta Delta(
        OrSetDot[]? enables = null,
        OrSetDot[]? disables = null,
        OrSetDot[]? tombstones = null) => new()
    {
        Enables = enables ?? Array.Empty<OrSetDot>(),
        Disables = disables ?? Array.Empty<OrSetDot>(),
        Tombstones = tombstones ?? Array.Empty<OrSetDot>(),
    };

    [Test]
    public void Mode_is_rwflag()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.RwFlag));
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
    public void DecodeState_empty_flag_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new RwFlag()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_enable_yields_added_disable_yields_removed_both_empty_element()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                enables: new[] { Dot("r1", 2) },
                disables: new[] { Dot("r2", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.That(events.All(e => e.Element.Length == 0), Is.True);
        var added = events.Single(e => e.Kind == CrdtMemberChangeKind.Added);
        var removed = events.Single(e => e.Kind == CrdtMemberChangeKind.Removed);
        Assert.Multiple(() =>
        {
            Assert.That(added.ReplicaId, Is.EqualTo("r1"));
            Assert.That(removed.ReplicaId, Is.EqualTo("r2"));
        });
    }

    [Test]
    public void DecodeDeltas_does_not_emit_for_tombstones()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                enables: new[] { Dot("r1", 2) },
                tombstones: new[] { Dot("r2", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        // Only the enable surfaces; the observed-enable tombstone is bookkeeping.
        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
    }

    [Test]
    public void DecodeState_concurrent_disable_wins_but_both_events_present()
    {
        // Remove-wins: a concurrent disable the enable has not observed keeps
        // the flag off, but the provenance stream records both operations.
        var flag = new RwFlag();
        flag.Enable("r1", 1);
        flag.Disable("r2", 1);

        var events = Decoder.DecodeState(flag);

        Assert.That(flag.IsEnabled, Is.False);
        Assert.That(events.Select(e => (e.Kind, e.ReplicaId)), Is.EquivalentTo(new[]
        {
            (CrdtMemberChangeKind.Added, "r1"),
            (CrdtMemberChangeKind.Removed, "r2"),
        }));
    }

    [Test]
    public void DecodeState_emits_removed_for_every_disable_even_when_tombstoned()
    {
        var flag = new RwFlag();
        flag.Disable("r2", 1);
        flag.Enable("r1", 2); // observes and tombstones (r2, 1)

        var events = Decoder.DecodeState(flag);

        Assert.That(flag.IsEnabled, Is.True);
        Assert.That(events.Select(e => (e.Kind, e.ReplicaId)), Is.EquivalentTo(new[]
        {
            (CrdtMemberChangeKind.Added, "r1"),
            (CrdtMemberChangeKind.Removed, "r2"),
        }));
    }

    [Test]
    public void DecodeState_order_is_deterministic()
    {
        var flag = new RwFlag();
        flag.Enable("rB", 1);
        flag.Enable("rA", 1);

        var first = Decoder.DecodeState(flag);
        var second = Decoder.DecodeState(flag);

        Assert.That(first.Select(e => e.ReplicaId), Is.EqualTo(second.Select(e => e.ReplicaId)));
        Assert.That(first[0].ReplicaId, Is.EqualTo("rA"));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var flag = new RwFlag();
        flag.Enable("r1", 1);

        Assert.That(Decoder.DecodeState(flag).All(e => e.WallClock is null), Is.True);
    }
}

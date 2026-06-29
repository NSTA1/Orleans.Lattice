namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class MvRegisterProvenanceDecoderTests
{
    private static readonly byte[] X = "x"u8.ToArray();
    private static readonly byte[] Y = "y"u8.ToArray();

    private static MvRegisterProvenanceDecoder Decoder => MvRegisterProvenanceDecoder.Instance;

    private static MvRegisterEntry Entry(byte[] value, string replica, long counter) =>
        new() { ReplicaId = replica, Counter = counter, Value = value };

    private static MvRegisterDelta Delta(MvRegisterEntry[] entries, (string Replica, long Counter)[] context)
    {
        var ctx = new Dictionary<string, long>();
        foreach (var (r, c) in context) ctx[r] = c;
        return new MvRegisterDelta { Entries = entries, Context = ctx };
    }

    [Test]
    public void Mode_is_mvregister()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.MvRegister));
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
    public void DecodeState_empty_register_yields_no_events()
    {
        Assert.That(Decoder.DecodeState(new MvRegister()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_entry_yields_added_with_value_bytes_and_counter_ordinal()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                entries: new[] { Entry(X, "r1", 1) },
                context: new[] { ("r1", 1L) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        var added = events.Single(e => e.Kind == CrdtMemberChangeKind.Added);
        Assert.Multiple(() =>
        {
            Assert.That(added.Element, Is.EqualTo(X));
            Assert.That(added.ReplicaId, Is.EqualTo("r1"));
            Assert.That(added.Ordinal, Is.EqualTo(1L));
        });
    }

    [Test]
    public void DecodeDeltas_superseded_replica_yields_removed_with_empty_element()
    {
        // r2 wrote after observing r1; r1 has no surviving entry but is in the context.
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(
                entries: new[] { Entry(Y, "r2", 1) },
                context: new[] { ("r1", 1L), ("r2", 1L) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        var removed = events.Single(e => e.Kind == CrdtMemberChangeKind.Removed);
        Assert.Multiple(() =>
        {
            Assert.That(removed.ReplicaId, Is.EqualTo("r1"));
            Assert.That(removed.Element, Is.Empty);
            Assert.That(removed.Ordinal, Is.EqualTo(1L));
        });
    }

    [Test]
    public void DecodeState_concurrent_values_both_represented_as_added()
    {
        var a = new MvRegister();
        a.Set("r1", X);
        var b = new MvRegister();
        b.Set("r2", Y);
        a.MergeFrom(b);

        var events = Decoder.DecodeState(a);

        Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added), Is.True);
        Assert.That(events.Select(e => e.ReplicaId), Is.EquivalentTo(new[] { "r1", "r2" }));
    }

    [Test]
    public void DecodeState_superseded_value_surfaces_removed_with_empty_element()
    {
        var a = new MvRegister();
        a.Set("r1", X);          // (r1,1)
        var b = a.Clone();
        b.Set("r2", Y);          // observes (r1,1), drops it; entry (r2,1), context {r1:1, r2:1}

        var events = Decoder.DecodeState(b);

        var added = events.Single(e => e.Kind == CrdtMemberChangeKind.Added);
        var removed = events.Single(e => e.Kind == CrdtMemberChangeKind.Removed);
        Assert.Multiple(() =>
        {
            Assert.That(added.ReplicaId, Is.EqualTo("r2"));
            Assert.That(added.Element, Is.EqualTo(Y));
            Assert.That(removed.ReplicaId, Is.EqualTo("r1"));
            Assert.That(removed.Element, Is.Empty);
        });
    }

    [Test]
    public void DecodeState_order_is_deterministic_by_replica()
    {
        var a = new MvRegister();
        a.Set("rB", X);
        var b = new MvRegister();
        b.Set("rA", Y);
        a.MergeFrom(b);

        var first = Decoder.DecodeState(a);
        var second = Decoder.DecodeState(a);

        Assert.That(first.Select(e => e.ReplicaId), Is.EqualTo(second.Select(e => e.ReplicaId)));
        Assert.That(first[0].ReplicaId, Is.EqualTo("rA"));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var m = new MvRegister();
        m.Set("r1", X);

        Assert.That(Decoder.DecodeState(m).All(e => e.WallClock is null), Is.True);
    }

    // ---- current-value (live entries) path ----

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => Decoder.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_empty_register_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new MvRegister()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_single_value_yields_one_live_member()
    {
        var m = new MvRegister();
        m.Set("r1", X);

        var members = Decoder.DecodeCurrentValue(m);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].Element, Is.EqualTo(X));
        Assert.That(members[0].ReplicaId, Is.EqualTo("r1"));
    }

    [Test]
    public void DecodeCurrentValue_concurrent_values_both_present()
    {
        var a = new MvRegister();
        a.Set("r1", X);
        var b = new MvRegister();
        b.Set("r2", Y);
        a.MergeFrom(b);

        var members = Decoder.DecodeCurrentValue(a);

        // Both concurrent live values surface; superseded values do not (this is
        // the current value, not the provenance timeline).
        Assert.That(members.Select(e => e.ReplicaId), Is.EquivalentTo(new[] { "r1", "r2" }));
    }

    [Test]
    public void DecodeCurrentValue_superseded_value_is_excluded()
    {
        var a = new MvRegister();
        a.Set("r1", X);
        var b = a.Clone();
        b.Set("r2", Y); // observes and supersedes (r1,1)

        var members = Decoder.DecodeCurrentValue(b);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].ReplicaId, Is.EqualTo("r2"));
        Assert.That(members[0].Element, Is.EqualTo(Y));
    }
}

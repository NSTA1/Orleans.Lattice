namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class OrFlagProvenanceDecoderTests
{
    private static OrFlagProvenanceDecoder Decoder => OrFlagProvenanceDecoder.Instance;

    private static OrSetDot Dot(string replica, long counter) => new() { ReplicaId = replica, Counter = counter };

    private static OrFlagDelta Delta(OrSetDot[]? enables = null, OrSetDot[]? disables = null) => new()
    {
        Enables = enables ?? Array.Empty<OrSetDot>(),
        Disables = disables ?? Array.Empty<OrSetDot>(),
    };

    [Test]
    public void Mode_is_orflag()
    {
        Assert.That(Decoder.Mode, Is.EqualTo(LatticeMergeMode.OrFlag));
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
        Assert.That(Decoder.DecodeState(new OrFlag()), Is.Empty);
    }

    [Test]
    public void DecodeDeltas_enable_yields_added_with_empty_element()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(enables: new[] { Dot("r1", 1) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(events[0].Element, Is.Empty);
            Assert.That(events[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(events[0].Ordinal, Is.EqualTo(1L));
        });
    }

    [Test]
    public void DecodeDeltas_disable_yields_removed()
    {
        var deltas = new[] { new CrdtProvenanceDelta(Delta(disables: new[] { Dot("r1", 1) })) };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
    }

    [Test]
    public void DecodeDeltas_preserves_enable_then_disable_order()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Delta(enables: new[] { Dot("r1", 1) })),
            new CrdtProvenanceDelta(Delta(disables: new[] { Dot("r1", 1) })),
        };

        var events = Decoder.DecodeDeltas(deltas);

        Assert.That(events.Select(e => e.Kind), Is.EqualTo(new[]
        {
            CrdtMemberChangeKind.Added,
            CrdtMemberChangeKind.Removed,
        }));
    }

    [Test]
    public void DecodeState_concurrent_enables_both_represented()
    {
        var flag = new OrFlag();
        flag.Enable("r1", 1);
        flag.Enable("r2", 1);

        var events = Decoder.DecodeState(flag);

        Assert.That(events.Where(e => e.Kind == CrdtMemberChangeKind.Added).Select(e => e.ReplicaId),
            Is.EquivalentTo(new[] { "r1", "r2" }));
    }

    [Test]
    public void DecodeState_enable_then_disable_shows_both_events()
    {
        var flag = new OrFlag();
        flag.Enable("r1", 1);
        flag.Disable(); // tombstones (r1, 1)

        var events = Decoder.DecodeState(flag);

        Assert.That(events.Select(e => e.Kind), Is.EquivalentTo(new[]
        {
            CrdtMemberChangeKind.Added,
            CrdtMemberChangeKind.Removed,
        }));
    }

    [Test]
    public void DecodeState_order_is_deterministic()
    {
        var flag = new OrFlag();
        flag.Enable("r2", 1);
        flag.Enable("r1", 1);

        var first = Decoder.DecodeState(flag);
        var second = Decoder.DecodeState(flag);

        Assert.That(first.Select(e => e.ReplicaId), Is.EqualTo(second.Select(e => e.ReplicaId)));
        Assert.That(first[0].ReplicaId, Is.EqualTo("r1"));
    }

    [Test]
    public void DecodeState_wall_clock_is_always_null()
    {
        var flag = new OrFlag();
        flag.Enable("r1", 1);

        Assert.That(Decoder.DecodeState(flag).All(e => e.WallClock is null), Is.True);
    }

    // ---- current-value (boolean state) path ----

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => Decoder.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_untouched_flag_yields_no_members()
    {
        Assert.That(Decoder.DecodeCurrentValue(new OrFlag()), Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_enabled_flag_yields_single_enabled_member()
    {
        var flag = new OrFlag();
        flag.Enable("r1", 1);

        var members = Decoder.DecodeCurrentValue(flag);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(System.Text.Encoding.UTF8.GetString(members[0].Element), Is.EqualTo("enabled"));
    }

    [Test]
    public void DecodeCurrentValue_disabled_flag_yields_single_disabled_member()
    {
        var flag = new OrFlag();
        flag.Enable("r1", 1);
        flag.Disable();

        var members = Decoder.DecodeCurrentValue(flag);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(System.Text.Encoding.UTF8.GetString(members[0].Element), Is.EqualTo("disabled"));
    }
}

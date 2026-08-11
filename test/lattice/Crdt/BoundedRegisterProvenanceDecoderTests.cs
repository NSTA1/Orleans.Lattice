namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class BoundedRegisterProvenanceDecoderTests
{
    private static BoundedRegisterDelta Candidate(byte value) => new()
    {
        Value = new[] { value },
        OrderKey = new[] { value },
        HasValue = true,
    };

    [Test]
    public void Max_decoder_mode_is_max_register()
    {
        Assert.That(MaxRegisterProvenanceDecoder.Instance.Mode, Is.EqualTo(LatticeMergeMode.MaxRegister));
    }

    [Test]
    public void Min_decoder_mode_is_min_register()
    {
        Assert.That(MinRegisterProvenanceDecoder.Instance.Mode, Is.EqualTo(LatticeMergeMode.MinRegister));
    }

    [Test]
    public void DecodeDeltas_null_throws()
    {
        Assert.That(() => MaxRegisterProvenanceDecoder.Instance.DecodeDeltas(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeState_null_throws()
    {
        Assert.That(() => MaxRegisterProvenanceDecoder.Instance.DecodeState(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeCurrentValue_null_throws()
    {
        Assert.That(() => MaxRegisterProvenanceDecoder.Instance.DecodeCurrentValue(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void DecodeDeltas_empty_sequence_yields_no_events()
    {
        Assert.That(
            MaxRegisterProvenanceDecoder.Instance.DecodeDeltas(Array.Empty<CrdtProvenanceDelta>()),
            Is.Empty);
    }

    [Test]
    public void DecodeDeltas_each_candidate_yields_added_event_in_order()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(Candidate(0x02)),
            new CrdtProvenanceDelta(Candidate(0x09)),
        };

        var events = MaxRegisterProvenanceDecoder.Instance.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(events.All(e => e.Kind == CrdtMemberChangeKind.Added), Is.True);
            Assert.That(events[0].Element, Is.EqualTo(new byte[] { 0x02 }));
            Assert.That(events[1].Element, Is.EqualTo(new byte[] { 0x09 }));
            Assert.That(events.All(e => e.ReplicaId == string.Empty), Is.True);
        });
    }

    [Test]
    public void DecodeDeltas_no_op_candidate_is_skipped()
    {
        var deltas = new[]
        {
            new CrdtProvenanceDelta(BoundedRegisterDelta.Empty),
            new CrdtProvenanceDelta(Candidate(0x05)),
        };

        var events = MaxRegisterProvenanceDecoder.Instance.DecodeDeltas(deltas);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.That(events[0].Element, Is.EqualTo(new byte[] { 0x05 }));
    }

    [Test]
    public void DecodeState_untouched_register_yields_no_events()
    {
        Assert.That(
            MaxRegisterProvenanceDecoder.Instance.DecodeState(BoundedRegister.CreateEmpty(isMin: false)),
            Is.Empty);
    }

    [Test]
    public void DecodeState_written_register_yields_single_added_event()
    {
        var register = BoundedRegister.CreateEmpty(isMin: false);
        register.Set(new byte[] { 0x07 }, new byte[] { 0x07 });

        var events = MinRegisterProvenanceDecoder.Instance.DecodeState(register);

        Assert.That(events, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(events[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(events[0].Element, Is.EqualTo(new byte[] { 0x07 }));
            Assert.That(events[0].WallClock, Is.Null);
        });
    }

    [Test]
    public void DecodeCurrentValue_untouched_register_yields_no_members()
    {
        Assert.That(
            MaxRegisterProvenanceDecoder.Instance.DecodeCurrentValue(BoundedRegister.CreateEmpty(isMin: false)),
            Is.Empty);
    }

    [Test]
    public void DecodeCurrentValue_written_register_yields_single_member()
    {
        var register = BoundedRegister.CreateEmpty(isMin: true);
        register.Set(new byte[] { 0x04 }, new byte[] { 0x04 });

        var members = MinRegisterProvenanceDecoder.Instance.DecodeCurrentValue(register);

        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].Element, Is.EqualTo(new byte[] { 0x04 }));
    }
}

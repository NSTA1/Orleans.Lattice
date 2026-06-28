namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class CrdtProvenanceDecoderRegistryTests
{
    [Test]
    public void Default_resolves_orset_decoder_by_mode()
    {
        var ok = CrdtProvenanceDecoderRegistry.Default.TryGet(LatticeMergeMode.OrSet, out var decoder);

        Assert.That(ok, Is.True);
        Assert.That(decoder, Is.InstanceOf<OrSetProvenanceDecoder>());
    }

    [Test]
    public void Default_resolves_orset_decoder_by_shape_string()
    {
        var ok = CrdtProvenanceDecoderRegistry.Default.TryGet("OrSet", out var decoder);

        Assert.That(ok, Is.True);
        Assert.That(decoder.Mode, Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void Default_returns_false_for_mode_without_decoder()
    {
        var ok = CrdtProvenanceDecoderRegistry.Default.TryGet(LatticeMergeMode.LwwRegister, out var decoder);

        Assert.That(ok, Is.False);
        Assert.That(decoder, Is.Null);
    }

    private static readonly LatticeMergeMode[] TypedCrdtModes =
    {
        LatticeMergeMode.OrSet,
        LatticeMergeMode.PnCounter,
        LatticeMergeMode.VersionVector,
        LatticeMergeMode.MvRegister,
        LatticeMergeMode.OrMap,
        LatticeMergeMode.Sequence,
        LatticeMergeMode.OrFlag,
        LatticeMergeMode.RwFlag,
    };

    [Test]
    public void Default_resolves_a_decoder_for_every_typed_crdt_mode(
        [ValueSource(nameof(TypedCrdtModes))] LatticeMergeMode mode)
    {
        var ok = CrdtProvenanceDecoderRegistry.Default.TryGet(mode, out var decoder);

        Assert.That(ok, Is.True, $"no decoder registered for {mode}");
        Assert.That(decoder, Is.Not.Null);
        Assert.That(decoder.Mode, Is.EqualTo(mode));
    }

    [Test]
    public void Default_resolves_a_decoder_for_every_typed_crdt_shape_string(
        [ValueSource(nameof(TypedCrdtModes))] LatticeMergeMode mode)
    {
        var ok = CrdtProvenanceDecoderRegistry.Default.TryGet(mode.ToString(), out var decoder);

        Assert.That(ok, Is.True, $"no decoder registered for shape {mode}");
        Assert.That(decoder.Mode, Is.EqualTo(mode));
    }

    [Test]
    public void Default_resolves_no_decoder_for_lww_register()
    {
        var ok = CrdtProvenanceDecoderRegistry.Default.TryGet(LatticeMergeMode.LwwRegister, out var decoder);

        Assert.That(ok, Is.False);
        Assert.That(decoder, Is.Null);
    }

    [Test]
    public void TryGet_returns_false_for_null_shape()
    {
        var ok = CrdtProvenanceDecoderRegistry.Default.TryGet((string?)null, out var decoder);

        Assert.That(ok, Is.False);
        Assert.That(decoder, Is.Null);
    }

    [Test]
    public void TryGet_returns_false_for_unrecognised_shape()
    {
        var ok = CrdtProvenanceDecoderRegistry.Default.TryGet("NotAShape", out var decoder);

        Assert.That(ok, Is.False);
        Assert.That(decoder, Is.Null);
    }

    [Test]
    public void Constructor_null_decoders_throws()
    {
        Assert.That(() => new CrdtProvenanceDecoderRegistry(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_element_throws()
    {
        Assert.That(
            () => new CrdtProvenanceDecoderRegistry(new ICrdtProvenanceDecoder[] { null! }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_last_decoder_wins_for_duplicate_mode()
    {
        var first = new OrSetProvenanceDecoder();
        var second = new OrSetProvenanceDecoder();
        var registry = new CrdtProvenanceDecoderRegistry(new ICrdtProvenanceDecoder[] { first, second });

        registry.TryGet(LatticeMergeMode.OrSet, out var resolved);

        Assert.That(resolved, Is.SameAs(second));
    }
}

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexKeyCodecIdentity"/>: the stable string the
/// registry persists in place of the codec object, and one of the
/// drift-significant inputs to <see cref="GrainIndexFingerprint"/>.
/// </summary>
[TestFixture]
public sealed class GrainIndexKeyCodecIdentityTests
{
    [Test]
    public void Identity_of_a_built_in_codec_is_its_clr_type_name()
    {
        var codec = StringGrainKeyCodec<ITestStringKeyedGrain>.Instance;

        Assert.That(
            GrainIndexKeyCodecIdentity.For(codec),
            Is.EqualTo(typeof(StringGrainKeyCodec<ITestStringKeyedGrain>).FullName));
    }

    [Test]
    public void Two_codec_types_over_the_same_grain_have_different_identities()
    {
        Assert.That(
            GrainIndexKeyCodecIdentity.For(StringGrainKeyCodec<ITestStringKeyedGrain>.Instance),
            Is.Not.EqualTo(GrainIndexKeyCodecIdentity.For(GuidGrainKeyCodec<ITestGuidKeyedGrain>.Instance)),
            "Swapping the codec changes both the encoding and the ordering of every entry key, so "
            + "the identities must differ.");
    }

    [Test]
    public void The_same_codec_type_over_different_grains_has_different_identities()
    {
        Assert.That(
            GrainIndexKeyCodecIdentity.For(GuidGrainKeyCodec<ITestGuidKeyedGrain>.Instance),
            Is.Not.EqualTo(GrainIndexKeyCodecIdentity.For(GuidGrainKeyCodec<ITestAmbiguouslyKeyedGrain>.Instance)),
            "The generic argument is part of the name, so a codec closed over a different grain "
            + "is a different identity.");
    }

    [Test]
    public void Identity_is_stable_across_calls()
    {
        var codec = IntegerGrainKeyCodec<ITestIntegerKeyedGrain>.Instance;

        Assert.That(
            GrainIndexKeyCodecIdentity.For(codec),
            Is.EqualTo(GrainIndexKeyCodecIdentity.For(codec)),
            "The value is persisted and compared across process restarts, so it must not vary.");
    }

    [Test]
    public void A_custom_codec_gets_its_own_identity()
    {
        Assert.That(
            GrainIndexKeyCodecIdentity.For(new CustomKeyCodec()),
            Is.EqualTo(typeof(CustomKeyCodec).FullName));
    }

    [Test]
    public void A_null_codec_is_rejected()
    {
        Assert.That(() => GrainIndexKeyCodecIdentity.For(null!), Throws.ArgumentNullException);
    }

    /// <summary>
    /// A host-supplied codec, used to prove a custom implementation gets an
    /// identity of its own rather than being lumped in with a built-in one.
    /// </summary>
    private sealed class CustomKeyCodec : IGrainKeyCodec<ITestStringKeyedGrain>
    {
        public Type GrainInterfaceType => typeof(ITestStringKeyedGrain);

        public bool TryEncode(Runtime.GrainId grainId, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? encodedKey)
        {
            encodedKey = grainId.Key.ToString();
            return encodedKey is not null;
        }

        public string Encode(Runtime.GrainId grainId) => grainId.Key.ToString()!;

        public ITestStringKeyedGrain Resolve(IGrainFactory grainFactory, string encodedKey) =>
            grainFactory.GetGrain<ITestStringKeyedGrain>(encodedKey);

        IGrain IGrainKeyCodec.Resolve(IGrainFactory grainFactory, string encodedKey) =>
            Resolve(grainFactory, encodedKey);
    }
}

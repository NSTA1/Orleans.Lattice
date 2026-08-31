namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainKeyCodec"/>: which built-in codec is selected for each
/// supported key shape, and the typed failure raised for a grain whose key no
/// built-in codec can encode.
/// </summary>
[TestFixture]
public sealed class GrainKeyCodecTests
{
    [Test]
    public void Create_default_selects_the_string_codec_for_a_string_keyed_grain() =>
        Assert.That(
            GrainKeyCodec.CreateDefault<ITestStringKeyedGrain>(),
            Is.SameAs(StringGrainKeyCodec<ITestStringKeyedGrain>.Instance));

    [Test]
    public void Create_default_selects_the_guid_codec_for_a_guid_keyed_grain() =>
        Assert.That(
            GrainKeyCodec.CreateDefault<ITestGuidKeyedGrain>(),
            Is.SameAs(GuidGrainKeyCodec<ITestGuidKeyedGrain>.Instance));

    [Test]
    public void Create_default_selects_the_integer_codec_for_an_integer_keyed_grain() =>
        Assert.That(
            GrainKeyCodec.CreateDefault<ITestIntegerKeyedGrain>(),
            Is.SameAs(IntegerGrainKeyCodec<ITestIntegerKeyedGrain>.Instance));

    [Test]
    public void Try_create_default_reports_success_for_each_supported_key_shape()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainKeyCodec.TryCreateDefault<ITestStringKeyedGrain>(out _), Is.True);
            Assert.That(GrainKeyCodec.TryCreateDefault<ITestGuidKeyedGrain>(out _), Is.True);
            Assert.That(GrainKeyCodec.TryCreateDefault<ITestIntegerKeyedGrain>(out _), Is.True);
        });
    }

    [Test]
    public void Try_create_default_reports_failure_for_a_compound_keyed_grain()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainKeyCodec.TryCreateDefault<ITestCompoundKeyedGrain>(out var codec), Is.False);
            Assert.That(codec, Is.Null);
        });
    }

    [Test]
    public void Create_default_throws_a_typed_failure_for_a_compound_keyed_grain() =>
        Assert.That(
            GrainKeyCodec.CreateDefault<ITestCompoundKeyedGrain>,
            Throws.TypeOf<GrainIndexKeyEncodingException>()
                .With.Message.Contains("Compound-keyed grains")
                .And.Message.Contains(typeof(ITestCompoundKeyedGrain).FullName!),
            "A grain whose key cannot be encoded is not indexable, and that has to be a clear "
            + "typed failure rather than a silently skipped grain.");

    [Test]
    public void Try_create_default_reports_failure_for_an_ambiguously_keyed_grain() =>
        Assert.That(GrainKeyCodec.TryCreateDefault<ITestAmbiguouslyKeyedGrain>(out _), Is.False);

    [Test]
    public void Create_default_throws_a_typed_failure_naming_the_ambiguity() =>
        Assert.That(
            GrainKeyCodec.CreateDefault<ITestAmbiguouslyKeyedGrain>,
            Throws.TypeOf<GrainIndexKeyEncodingException>()
                .With.Message.Contains("more than one"));

    [Test]
    public void Try_create_default_yields_the_same_singleton_create_default_returns()
    {
        GrainKeyCodec.TryCreateDefault<ITestGuidKeyedGrain>(out var codec);

        Assert.That(codec, Is.SameAs(GrainKeyCodec.CreateDefault<ITestGuidKeyedGrain>()));
    }
}

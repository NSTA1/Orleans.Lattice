using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="StringGrainKeyCodec{TGrain}"/>: encoding a string-keyed
/// grain's identity, round-tripping it back through the grain factory, and the
/// keys it refuses.
/// </summary>
[TestFixture]
public sealed class StringGrainKeyCodecTests
{
    private static readonly GrainType TestGrainType = GrainType.Create("teststring");

    private static StringGrainKeyCodec<ITestStringKeyedGrain> Codec =>
        StringGrainKeyCodec<ITestStringKeyedGrain>.Instance;

    [Test]
    public void Instance_is_a_shared_singleton_so_the_projection_path_never_allocates_a_codec() =>
        Assert.That(
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            Is.SameAs(StringGrainKeyCodec<ITestStringKeyedGrain>.Instance));

    [Test]
    public void Grain_interface_type_reports_the_indexed_grain() =>
        Assert.That(Codec.GrainInterfaceType, Is.EqualTo(typeof(ITestStringKeyedGrain)));

    [TestCase("user-1")]
    [TestCase("a")]
    [TestCase("with/slash and space")]
    public void Encode_returns_the_string_key_verbatim(string key)
    {
        var grainId = GrainId.Create(TestGrainType, key);

        Assert.That(Codec.Encode(grainId), Is.EqualTo(key));
    }

    [Test]
    public void Try_encode_reports_success_and_yields_the_string_key()
    {
        var grainId = GrainId.Create(TestGrainType, "user-1");

        var encoded = Codec.TryEncode(grainId, out var value) ? value : null;

        Assert.That(encoded, Is.EqualTo("user-1"));
    }

    [Test]
    public void Try_encode_rejects_a_grain_id_with_an_empty_key()
    {
        var grainId = GrainId.Create(TestGrainType, IdSpan.Create(string.Empty));

        Assert.Multiple(() =>
        {
            Assert.That(Codec.TryEncode(grainId, out var encoded), Is.False);
            Assert.That(encoded, Is.Null);
        });
    }

    [Test]
    public void Try_encode_rejects_a_default_grain_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Codec.TryEncode(default, out var encoded), Is.False);
            Assert.That(encoded, Is.Null);
        });
    }

    [Test]
    public void Encode_throws_a_typed_failure_for_a_default_grain_id() =>
        Assert.That(
            () => Codec.Encode(default),
            Throws.TypeOf<GrainIndexKeyEncodingException>()
                .With.Message.Contains("not indexable"));

    [Test]
    public void Resolve_round_trips_an_encoded_key_back_to_the_same_grain()
    {
        var grainId = GrainId.Create(TestGrainType, "user-1");
        var encoded = Codec.Encode(grainId);
        var expected = Substitute.For<ITestStringKeyedGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITestStringKeyedGrain>("user-1").Returns(expected);

        var resolved = Codec.Resolve(factory, encoded);

        Assert.That(resolved, Is.SameAs(expected));
    }

    [Test]
    public void Resolve_through_the_non_generic_contract_round_trips_the_same_grain()
    {
        var expected = Substitute.For<ITestStringKeyedGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITestStringKeyedGrain>("user-1").Returns(expected);

        var resolved = ((IGrainKeyCodec)Codec).Resolve(factory, "user-1");

        Assert.That(resolved, Is.SameAs(expected));
    }

    [Test]
    public void Resolve_rejects_a_null_grain_factory() =>
        Assert.That(() => Codec.Resolve(null!, "user-1"), Throws.ArgumentNullException);

    [Test]
    public void Resolve_rejects_a_null_encoded_key() =>
        Assert.That(
            () => Codec.Resolve(Substitute.For<IGrainFactory>(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void Resolve_throws_a_typed_failure_for_an_empty_encoded_key() =>
        Assert.That(
            () => Codec.Resolve(Substitute.For<IGrainFactory>(), string.Empty),
            Throws.TypeOf<GrainIndexKeyEncodingException>());
}

using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GuidGrainKeyCodec{TGrain}"/>: the fixed-width hexadecimal
/// encoding, the round trip back through the grain factory, and the compound key
/// it refuses.
/// </summary>
[TestFixture]
public sealed class GuidGrainKeyCodecTests
{
    private static readonly GrainType TestGrainType = GrainType.Create("testguid");

    private static GuidGrainKeyCodec<ITestGuidKeyedGrain> Codec =>
        GuidGrainKeyCodec<ITestGuidKeyedGrain>.Instance;

    [Test]
    public void Instance_is_a_shared_singleton_so_the_projection_path_never_allocates_a_codec() =>
        Assert.That(
            GuidGrainKeyCodec<ITestGuidKeyedGrain>.Instance,
            Is.SameAs(GuidGrainKeyCodec<ITestGuidKeyedGrain>.Instance));

    [Test]
    public void Grain_interface_type_reports_the_indexed_grain() =>
        Assert.That(Codec.GrainInterfaceType, Is.EqualTo(typeof(ITestGuidKeyedGrain)));

    [TestCase("00000000-0000-0000-0000-000000000000")]
    [TestCase("1f8b3c2d-4e5a-6b7c-8d9e-0f1a2b3c4d5e")]
    [TestCase("ffffffff-ffff-ffff-ffff-ffffffffffff")]
    public void Encode_produces_the_fixed_width_hexadecimal_form(string guidText)
    {
        var key = Guid.Parse(guidText);
        var grainId = GrainId.Create(TestGrainType, GrainIdKeyExtensions.CreateGuidKey(key));

        var encoded = Codec.Encode(grainId);

        Assert.Multiple(() =>
        {
            Assert.That(encoded, Has.Length.EqualTo(32));
            Assert.That(encoded, Is.EqualTo(key.ToString("N")));
        });
    }

    [Test]
    public void Encode_then_resolve_round_trips_the_same_guid()
    {
        var key = Guid.Parse("1f8b3c2d-4e5a-6b7c-8d9e-0f1a2b3c4d5e");
        var grainId = GrainId.Create(TestGrainType, GrainIdKeyExtensions.CreateGuidKey(key));
        var expected = Substitute.For<ITestGuidKeyedGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITestGuidKeyedGrain>(key).Returns(expected);

        var resolved = Codec.Resolve(factory, Codec.Encode(grainId));

        Assert.That(resolved, Is.SameAs(expected));
    }

    [Test]
    public void Resolve_through_the_non_generic_contract_round_trips_the_same_grain()
    {
        var key = Guid.Parse("1f8b3c2d-4e5a-6b7c-8d9e-0f1a2b3c4d5e");
        var expected = Substitute.For<ITestGuidKeyedGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITestGuidKeyedGrain>(key).Returns(expected);

        var resolved = ((IGrainKeyCodec)Codec).Resolve(factory, key.ToString("N"));

        Assert.That(resolved, Is.SameAs(expected));
    }

    [Test]
    public void Try_encode_rejects_a_compound_guid_key_because_the_extension_would_be_lost()
    {
        var grainId = GrainId.Create(
            TestGrainType,
            GrainIdKeyExtensions.CreateGuidKey(Guid.NewGuid(), "tenant-a"));

        Assert.Multiple(() =>
        {
            Assert.That(Codec.TryEncode(grainId, out var encoded), Is.False);
            Assert.That(encoded, Is.Null);
        });
    }

    [Test]
    public void Encode_throws_a_typed_failure_for_a_compound_guid_key()
    {
        var grainId = GrainId.Create(
            TestGrainType,
            GrainIdKeyExtensions.CreateGuidKey(Guid.NewGuid(), "tenant-a"));

        Assert.That(
            () => Codec.Encode(grainId),
            Throws.TypeOf<GrainIndexKeyEncodingException>()
                .With.Message.Contains("not indexable"));
    }

    [Test]
    public void Try_encode_rejects_a_default_grain_id() =>
        Assert.That(Codec.TryEncode(default, out _), Is.False);

    [Test]
    public void Try_encode_rejects_a_grain_id_whose_key_is_not_a_guid()
    {
        var grainId = GrainId.Create(TestGrainType, "not-a-guid");

        Assert.That(Codec.TryEncode(grainId, out _), Is.False);
    }

    [Test]
    public void Resolve_rejects_a_null_grain_factory() =>
        Assert.That(() => Codec.Resolve(null!, Guid.Empty.ToString("N")), Throws.ArgumentNullException);

    [Test]
    public void Resolve_rejects_a_null_encoded_key() =>
        Assert.That(
            () => Codec.Resolve(Substitute.For<IGrainFactory>(), null!),
            Throws.ArgumentNullException);

    [TestCase("")]
    [TestCase("abc")]
    [TestCase("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")]
    [TestCase("1f8b3c2d-4e5a-6b7c-8d9e-0f1a2b3c4d5e")]
    public void Resolve_throws_a_typed_failure_for_a_key_it_did_not_produce(string encodedKey) =>
        Assert.That(
            () => Codec.Resolve(Substitute.For<IGrainFactory>(), encodedKey),
            Throws.TypeOf<GrainIndexKeyEncodingException>());
}

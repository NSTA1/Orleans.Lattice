using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="IntegerGrainKeyCodec{TGrain}"/>: the order-preserving
/// fixed-width encoding, the round trip back through the grain factory, and the
/// compound key it refuses.
/// </summary>
[TestFixture]
public sealed class IntegerGrainKeyCodecTests
{
    private static readonly GrainType TestGrainType = GrainType.Create("testinteger");

    private static IntegerGrainKeyCodec<ITestIntegerKeyedGrain> Codec =>
        IntegerGrainKeyCodec<ITestIntegerKeyedGrain>.Instance;

    private static GrainId IdFor(long key) =>
        GrainId.Create(TestGrainType, GrainIdKeyExtensions.CreateIntegerKey(key));

    [Test]
    public void Instance_is_a_shared_singleton_so_the_projection_path_never_allocates_a_codec() =>
        Assert.That(
            IntegerGrainKeyCodec<ITestIntegerKeyedGrain>.Instance,
            Is.SameAs(IntegerGrainKeyCodec<ITestIntegerKeyedGrain>.Instance));

    [Test]
    public void Grain_interface_type_reports_the_indexed_grain() =>
        Assert.That(Codec.GrainInterfaceType, Is.EqualTo(typeof(ITestIntegerKeyedGrain)));

    [TestCase(long.MinValue, "0000000000000000")]
    [TestCase(-1L, "7fffffffffffffff")]
    [TestCase(0L, "8000000000000000")]
    [TestCase(1L, "8000000000000001")]
    [TestCase(long.MaxValue, "ffffffffffffffff")]
    public void Encode_produces_the_fixed_width_sign_biased_hexadecimal_form(long key, string expected) =>
        Assert.That(Codec.Encode(IdFor(key)), Is.EqualTo(expected));

    [Test]
    public void Encoded_keys_sort_in_the_same_order_as_the_integers_they_encode()
    {
        long[] ascending = [long.MinValue, -1_000L, -1L, 0L, 1L, 1_000L, long.MaxValue];

        var encoded = new string[ascending.Length];
        for (var i = 0; i < ascending.Length; i++)
        {
            encoded[i] = Codec.Encode(IdFor(ascending[i]));
        }

        Assert.That(encoded, Is.Ordered.Using<string>(StringComparer.Ordinal),
            "Index entries are ordered by their encoded key, so a numeric ordering that did not "
            + "survive encoding would make an integer-keyed range scan wrong.");
    }

    [TestCase(long.MinValue)]
    [TestCase(-42L)]
    [TestCase(0L)]
    [TestCase(42L)]
    [TestCase(long.MaxValue)]
    public void Encode_then_resolve_round_trips_the_same_integer(long key)
    {
        var expected = Substitute.For<ITestIntegerKeyedGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITestIntegerKeyedGrain>(key).Returns(expected);

        var resolved = Codec.Resolve(factory, Codec.Encode(IdFor(key)));

        Assert.That(resolved, Is.SameAs(expected));
    }

    [Test]
    public void Resolve_through_the_non_generic_contract_round_trips_the_same_grain()
    {
        var expected = Substitute.For<ITestIntegerKeyedGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ITestIntegerKeyedGrain>(42L).Returns(expected);

        var resolved = ((IGrainKeyCodec)Codec).Resolve(factory, Codec.Encode(IdFor(42L)));

        Assert.That(resolved, Is.SameAs(expected));
    }

    [Test]
    public void Try_encode_rejects_a_compound_integer_key_because_the_extension_would_be_lost()
    {
        var grainId = GrainId.Create(
            TestGrainType,
            GrainIdKeyExtensions.CreateIntegerKey(7L, "tenant-a"));

        Assert.Multiple(() =>
        {
            Assert.That(Codec.TryEncode(grainId, out var encoded), Is.False);
            Assert.That(encoded, Is.Null);
        });
    }

    [Test]
    public void Encode_throws_a_typed_failure_for_a_compound_integer_key()
    {
        var grainId = GrainId.Create(
            TestGrainType,
            GrainIdKeyExtensions.CreateIntegerKey(7L, "tenant-a"));

        Assert.That(
            () => Codec.Encode(grainId),
            Throws.TypeOf<GrainIndexKeyEncodingException>()
                .With.Message.Contains("not indexable"));
    }

    [Test]
    public void Try_encode_rejects_a_default_grain_id() =>
        Assert.That(Codec.TryEncode(default, out _), Is.False);

    [Test]
    public void Try_encode_rejects_a_grain_id_whose_key_is_not_an_integer()
    {
        var grainId = GrainId.Create(TestGrainType, "not-an-integer");

        Assert.That(Codec.TryEncode(grainId, out _), Is.False);
    }

    [Test]
    public void Resolve_rejects_a_null_grain_factory() =>
        Assert.That(() => Codec.Resolve(null!, "8000000000000000"), Throws.ArgumentNullException);

    [Test]
    public void Resolve_rejects_a_null_encoded_key() =>
        Assert.That(
            () => Codec.Resolve(Substitute.For<IGrainFactory>(), null!),
            Throws.ArgumentNullException);

    [TestCase("")]
    [TestCase("42")]
    [TestCase("zzzzzzzzzzzzzzzz")]
    [TestCase("       800000000")]
    public void Resolve_throws_a_typed_failure_for_a_key_it_did_not_produce(string encodedKey) =>
        Assert.That(
            () => Codec.Resolve(Substitute.For<IGrainFactory>(), encodedKey),
            Throws.TypeOf<GrainIndexKeyEncodingException>());
}

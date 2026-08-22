using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Tests.Predicates;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit coverage for <see cref="PredicateRuntimeViewProjectionCodec"/>, the built-in
/// filter-only predicate projection payload codec.
/// </summary>
[TestFixture]
public class PredicateRuntimeViewProjectionCodecTests
{
    private ServiceProvider _services = null!;
    private PredicateRuntimeViewProjectionCodec _codec = null!;

    [SetUp]
    public void SetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _codec = new PredicateRuntimeViewProjectionCodec(
            _services.GetRequiredService<Serializer<LatticePredicateNode>>());
    }

    [TearDown]
    public void TearDown() => _services.Dispose();

    [Test]
    public void Encode_nullFilter_isSingleZeroByte()
    {
        var payload = _codec.Encode(null);

        Assert.That(payload, Is.EqualTo(new byte[] { 0 }));
    }

    [Test]
    public void RoundTrip_nullFilter_returnsNull()
    {
        var payload = _codec.Encode(null);

        Assert.That(_codec.Decode(payload), Is.Null);
    }

    [Test]
    public void RoundTrip_realFilter_preservesEvaluation()
    {
        var filter = LatticePredicateTranslator.Translate<PredicatePerson>(person => person.Age >= 18);

        var payload = _codec.Encode(filter);
        var decoded = _codec.Decode(payload);

        Assert.That(decoded, Is.Not.Null);
        var adult = JsonLatticeSerializer<PredicatePerson>.Default.Serialize(
            new PredicatePerson("Alice", 30, true, 0.9, null, null));
        var minor = JsonLatticeSerializer<PredicatePerson>.Default.Serialize(
            new PredicatePerson("Bob", 12, true, 0.5, null, null));
        Assert.Multiple(() =>
        {
            Assert.That(LatticePredicateEvaluator.Matches(adult, decoded!.Value), Is.True);
            Assert.That(LatticePredicateEvaluator.Matches(minor, decoded!.Value), Is.False);
        });
    }

    [Test]
    public void Encode_realFilter_isPrefixedWithOne()
    {
        var filter = LatticePredicateTranslator.Translate<PredicatePerson>(person => person.Active);

        var payload = _codec.Encode(filter);

        Assert.Multiple(() =>
        {
            Assert.That(payload, Has.Length.GreaterThan(1));
            Assert.That(payload[0], Is.EqualTo(1));
        });
    }

    [Test]
    public void Decode_emptyPayload_throws()
    {
        Assert.That(
            () => _codec.Decode(ReadOnlySpan<byte>.Empty),
            Throws.ArgumentException);
    }

    [Test]
    public void Decode_unknownPrefix_throws()
    {
        Assert.That(
            () => _codec.Decode(new byte[] { 2, 3, 4 }),
            Throws.ArgumentException);
    }

    [Test]
    public void Decode_presentPrefixWithoutBody_throws()
    {
        Assert.That(
            () => _codec.Decode(new byte[] { 1 }),
            Throws.ArgumentException);
    }
}

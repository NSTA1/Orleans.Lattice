using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Orleans serializer round-trip for <see cref="CrdtMemberValue"/>, the
/// wire-facing live-member projection the State API surfaces for a CRDT's current
/// folded state. Proves the codegen produces a working envelope and that every
/// id-tagged slot survives the round trip.
/// </summary>
[TestFixture]
public class CrdtMemberValueSerializerRoundTripTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        var bytes = serializer.SerializeToArray(value);
        return serializer.Deserialize(bytes);
    }

    [Test]
    public void Round_trips_all_fields()
    {
        var original = new CrdtMemberValue
        {
            Element = "apple"u8.ToArray(),
            ReplicaId = "r1",
            Ordinal = 42,
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Element, Is.EqualTo(original.Element));
            Assert.That(copy.ReplicaId, Is.EqualTo("r1"));
            Assert.That(copy.Ordinal, Is.EqualTo(42L));
        });
    }

    [Test]
    public void Round_trips_empty_replica_and_zero_ordinal()
    {
        var original = new CrdtMemberValue
        {
            Element = "3"u8.ToArray(),
            ReplicaId = string.Empty,
            Ordinal = 0,
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Element, Is.EqualTo(original.Element));
            Assert.That(copy.ReplicaId, Is.Empty);
            Assert.That(copy.Ordinal, Is.EqualTo(0L));
        });
    }
}

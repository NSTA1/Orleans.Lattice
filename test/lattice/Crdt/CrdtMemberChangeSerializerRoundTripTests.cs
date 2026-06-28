using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Orleans serializer round-trip for <see cref="CrdtMemberChange"/>, the
/// wire-facing provenance event the State API surfaces. Proves the codegen
/// produces a working envelope and that the nullable
/// <see cref="CrdtMemberChange.WallClock"/> slot survives both the present and
/// absent cases.
/// </summary>
[TestFixture]
public class CrdtMemberChangeSerializerRoundTripTests
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
    public void Round_trips_with_wall_clock()
    {
        var original = new CrdtMemberChange
        {
            Element = "apple"u8.ToArray(),
            Kind = CrdtMemberChangeKind.Added,
            ReplicaId = "r1",
            Ordinal = 42,
            WallClock = new HybridLogicalClock { WallClockTicks = 1234, Counter = 3 },
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Element, Is.EqualTo(original.Element));
            Assert.That(copy.Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(copy.ReplicaId, Is.EqualTo("r1"));
            Assert.That(copy.Ordinal, Is.EqualTo(42L));
            Assert.That(copy.WallClock, Is.EqualTo(original.WallClock));
        });
    }

    [Test]
    public void Round_trips_without_wall_clock()
    {
        var original = new CrdtMemberChange
        {
            Element = "banana"u8.ToArray(),
            Kind = CrdtMemberChangeKind.Removed,
            ReplicaId = "r2",
            Ordinal = 7,
            WallClock = null,
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
            Assert.That(copy.Ordinal, Is.EqualTo(7L));
            Assert.That(copy.WallClock, Is.Null);
        });
    }
}

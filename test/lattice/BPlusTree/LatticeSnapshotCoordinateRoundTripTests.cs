using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the Orleans-serializer wire shape for
/// <see cref="LatticeSnapshotCoordinate"/>, focusing on the
/// <c>[Id(5)] SnapshotBaselineToken</c> slot added for the frozen-baseline
/// snapshot-scan fix. A coordinate persisted before the slot existed (or one
/// whose payload leaves it unset) must decode to <see cref="Guid.Empty"/> so
/// the snapshot leaf falls back to the legacy from-zero replay path.
/// </summary>
[TestFixture]
public sealed class LatticeSnapshotCoordinateRoundTripTests
{
    private ServiceProvider _services = null!;
    private Serializer<LatticeSnapshotCoordinate> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeSnapshotCoordinate>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeSnapshotCoordinate RoundTrip(LatticeSnapshotCoordinate coordinate)
    {
        var bytes = _serializer.SerializeToArray(coordinate);
        return _serializer.Deserialize(bytes);
    }

    [Test]
    public void SnapshotBaselineToken_round_trips_a_non_empty_token()
    {
        var token = Guid.NewGuid();
        var coordinate = new LatticeSnapshotCoordinate(
            7,
            new Dictionary<int, long> { [0] = 12, [1] = 34 },
            HybridLogicalClock.Zero)
        {
            SnapshotBaselineToken = token,
        };

        var decoded = RoundTrip(coordinate);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.SnapshotBaselineToken, Is.EqualTo(token));
            Assert.That(decoded.TreeMapVersion, Is.EqualTo(7));
            Assert.That(decoded.PerShardWalOffsets[0], Is.EqualTo(12));
            Assert.That(decoded.PerShardWalOffsets[1], Is.EqualTo(34));
        });
    }

    [Test]
    public void SnapshotBaselineToken_defaults_to_empty_when_unset()
    {
        // A coordinate built without the token (the legacy shape) must decode
        // with Guid.Empty so the snapshot leaf selects the legacy replay path.
        var coordinate = new LatticeSnapshotCoordinate(
            1,
            new Dictionary<int, long> { [0] = 5 },
            HybridLogicalClock.Zero);

        var decoded = RoundTrip(coordinate);

        Assert.That(decoded.SnapshotBaselineToken, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public void SnapshotBaselineToken_is_independent_of_the_other_slots()
    {
        var token = Guid.NewGuid();
        var perPartition = new Dictionary<int, IReadOnlyList<long>>
        {
            [0] = new long[] { 3, 9 },
        };
        var coordinate = new LatticeSnapshotCoordinate(
            42,
            perPartition,
            HybridLogicalClock.Zero)
        {
            SnapshotBaselineToken = token,
        };

        var decoded = RoundTrip(coordinate);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.SnapshotBaselineToken, Is.EqualTo(token));
            Assert.That(decoded.PerShardPerPartitionWalOffsets, Is.Not.Null);
            Assert.That(decoded.PerShardPerPartitionWalOffsets![0], Is.EqualTo(new long[] { 3, 9 }));
            // The scalar companion carries the per-shard max for legacy readers.
            Assert.That(decoded.PerShardWalOffsets[0], Is.EqualTo(9));
        });
    }
}

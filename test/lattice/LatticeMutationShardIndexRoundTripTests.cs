using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Pins the Orleans-serializer wire-shape contract for the
/// <see cref="LatticeMutation.ShardIndex"/> slot: the field must
/// round-trip through serialize/deserialize verbatim, and a legacy
/// payload that leaves it unset must decode to zero. The slot is
/// stamped on every foreground commit by the leaf grain and
/// consulted at activation-time replay to filter out records
/// authored by a sibling chain shard sharing a WAL partition.
/// </summary>
[TestFixture]
public sealed class LatticeMutationShardIndexRoundTripTests
{
    private ServiceProvider _services = null!;
    private Serializer<LatticeMutation> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<LatticeMutation>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeMutation RoundTrip(LatticeMutation mutation)
    {
        var bytes = _serializer.SerializeToArray(mutation);
        return _serializer.Deserialize(bytes);
    }

    [Test]
    public void ShardIndex_round_trips_with_explicit_value()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            ShardIndex = 7,
        };

        var decoded = RoundTrip(mutation);

        Assert.That(decoded.ShardIndex, Is.EqualTo(7));
    }

    [Test]
    public void ShardIndex_round_trips_zero_for_legacy_decode()
    {
        // Wire-compat: a producer (e.g. a pre-Option-A persisted
        // observer payload) that never sets the slot emits the
        // default zero value; the serializer must decode the same
        // shape so a legacy payload upgraded in place reads as
        // shard 0 (the only shard in V1 single-shard topologies).
        var mutation = new LatticeMutation
        {
            TreeId = "tree",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1 },
        };

        var decoded = RoundTrip(mutation);

        Assert.That(decoded.ShardIndex, Is.EqualTo(0));
    }
}

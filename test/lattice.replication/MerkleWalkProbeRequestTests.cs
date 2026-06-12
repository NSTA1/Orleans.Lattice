using NUnit.Framework;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Tests for <see cref="MerkleWalkProbeRequest"/>.</summary>
[TestFixture]
public sealed class MerkleWalkProbeRequestTests
{
    [Test]
    public void Properties_round_trip_through_init()
    {
        var request = new MerkleWalkProbeRequest
        {
            TreeName = "orders",
            ShardIndex = 3,
            RangeStartKey = "a",
            RangeEndKey = "m",
            Depth = 2,
        };

        Assert.That(request.TreeName, Is.EqualTo("orders"));
        Assert.That(request.ShardIndex, Is.EqualTo(3));
        Assert.That(request.RangeStartKey, Is.EqualTo("a"));
        Assert.That(request.RangeEndKey, Is.EqualTo("m"));
        Assert.That(request.Depth, Is.EqualTo(2));
    }

    [Test]
    public void Default_has_null_range_bounds()
    {
        var request = default(MerkleWalkProbeRequest);

        Assert.That(request.RangeStartKey, Is.Null);
        Assert.That(request.RangeEndKey, Is.Null);
        Assert.That(request.Depth, Is.Zero);
    }
}

using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Tests for <see cref="PeerHighWaterMarkRequest"/>.</summary>
[TestFixture]
public sealed class PeerHighWaterMarkRequestTests
{
    [Test]
    public void Properties_round_trip_through_init()
    {
        var request = new PeerHighWaterMarkRequest
        {
            TreeName = "orders",
            OriginClusterId = "site-a",
        };

        Assert.That(request.TreeName, Is.EqualTo("orders"));
        Assert.That(request.OriginClusterId, Is.EqualTo("site-a"));
    }

    [Test]
    public void Default_has_null_members()
    {
        var request = default(PeerHighWaterMarkRequest);

        Assert.That(request.TreeName, Is.Null);
        Assert.That(request.OriginClusterId, Is.Null);
    }

    [Test]
    public void Value_equality_holds_for_identical_members()
    {
        var a = new PeerHighWaterMarkRequest { TreeName = "t", OriginClusterId = "o" };
        var b = new PeerHighWaterMarkRequest { TreeName = "t", OriginClusterId = "o" };

        Assert.That(a, Is.EqualTo(b));
    }
}

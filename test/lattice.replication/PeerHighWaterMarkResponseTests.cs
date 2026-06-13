using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Tests for <see cref="PeerHighWaterMarkResponse"/>.</summary>
[TestFixture]
public sealed class PeerHighWaterMarkResponseTests
{
    [Test]
    public void Clock_round_trips_through_init()
    {
        var clock = new HybridLogicalClock { WallClockTicks = 123, Counter = 4 };
        var response = new PeerHighWaterMarkResponse { Clock = clock };

        Assert.That(response.Clock, Is.EqualTo(clock));
    }

    [Test]
    public void Default_has_zero_clock()
    {
        var response = default(PeerHighWaterMarkResponse);

        Assert.That(response.Clock, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public void Value_equality_holds_for_identical_clocks()
    {
        var clock = new HybridLogicalClock { WallClockTicks = 7, Counter = 1 };
        var a = new PeerHighWaterMarkResponse { Clock = clock };
        var b = new PeerHighWaterMarkResponse { Clock = clock };

        Assert.That(a, Is.EqualTo(b));
    }
}

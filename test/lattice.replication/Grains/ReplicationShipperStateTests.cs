using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationShipperStateTests
{
    [Test]
    public void New_instance_has_zero_cursor()
    {
        var state = new ReplicationShipperState();
        Assert.That(state.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public void New_instance_has_zero_consecutive_failures()
    {
        var state = new ReplicationShipperState();
        Assert.That(state.ConsecutiveFailures, Is.EqualTo(0));
    }

    [Test]
    public void Cursor_is_settable()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 12345, Counter = 7 };
        var state = new ReplicationShipperState { Cursor = hlc };
        Assert.That(state.Cursor, Is.EqualTo(hlc));
    }

    [Test]
    public void ConsecutiveFailures_is_settable()
    {
        var state = new ReplicationShipperState { ConsecutiveFailures = 3 };
        Assert.That(state.ConsecutiveFailures, Is.EqualTo(3));
    }
}

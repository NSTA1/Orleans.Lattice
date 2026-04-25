using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationHighWaterMarkStateTests
{
    [Test]
    public void HighWaterMark_defaults_to_zero()
    {
        var state = new ReplicationHighWaterMarkState();

        Assert.That(state.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public void HighWaterMark_is_settable()
    {
        var state = new ReplicationHighWaterMarkState();
        var stamp = new HybridLogicalClock { WallClockTicks = 42, Counter = 1 };

        state.HighWaterMark = stamp;

        Assert.That(state.HighWaterMark, Is.EqualTo(stamp));
    }
}

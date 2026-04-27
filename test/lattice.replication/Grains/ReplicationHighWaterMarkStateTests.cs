using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationHighWaterMarkStateTests
{
    [Test]
    public void Vector_defaults_to_empty()
    {
        var state = new ReplicationHighWaterMarkState();

        Assert.That(state.Vector.Entries, Is.Empty);
    }

    [Test]
    public void Vector_is_settable()
    {
        var state = new ReplicationHighWaterMarkState();
        var vector = new VersionVector();
        vector.Entries["site-a"] = new HybridLogicalClock { WallClockTicks = 42, Counter = 1 };

        state.Vector = vector;

        Assert.That(state.Vector, Is.SameAs(vector));
    }
}

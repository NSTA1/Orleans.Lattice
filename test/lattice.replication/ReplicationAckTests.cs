using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationAckTests
{
    [Test]
    public void Default_value_is_unaccepted_with_zero_hlc()
    {
        var ack = default(ReplicationAck);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.False);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void Init_assigns_every_property()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 100, Counter = 7 };
        var ack = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(hlc));
        });
    }

    [Test]
    public void Equality_uses_value_semantics()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 10, Counter = 1 };
        var a = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };
        var b = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };
        var c = new ReplicationAck { Accepted = false, HighestAppliedHlc = hlc };

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a, Is.Not.EqualTo(c));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void With_expression_produces_modified_copy()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        var ack = new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc };

        var modified = ack with { Accepted = false };

        Assert.Multiple(() =>
        {
            Assert.That(modified.Accepted, Is.False);
            Assert.That(modified.HighestAppliedHlc, Is.EqualTo(hlc));
            Assert.That(ack.Accepted, Is.True);
        });
    }
}

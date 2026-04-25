using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ApplyResultTests
{
    [Test]
    public void Default_value_has_applied_false_and_zero_hwm()
    {
        var result = default(ApplyResult);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void Init_only_setters_round_trip()
    {
        var hwm = new HybridLogicalClock { WallClockTicks = 5, Counter = 1 };
        var result = new ApplyResult { Applied = true, HighWaterMark = hwm };

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(hwm));
        });
    }

    [Test]
    public void Equality_compares_by_value()
    {
        var hwm = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        var a = new ApplyResult { Applied = true, HighWaterMark = hwm };
        var b = new ApplyResult { Applied = true, HighWaterMark = hwm };

        Assert.That(a, Is.EqualTo(b));
    }
}

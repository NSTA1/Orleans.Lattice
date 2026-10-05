namespace Orleans.Lattice.Tests;

[TestFixture]
public sealed class LatticeIdempotencyKeyExpiredExceptionTests
{
    [Test]
    public void The_production_constructor_carries_the_key_and_the_floor()
    {
        var key = new HybridLogicalClock { WallClockTicks = 10 };
        var floor = new HybridLogicalClock { WallClockTicks = 20 };
        var inner = new InvalidOperationException("refused");

        var ex = new LatticeIdempotencyKeyExpiredException(key, floor, inner);

        Assert.Multiple(() =>
        {
            Assert.That(ex.KeyTimestamp, Is.EqualTo(key));
            Assert.That(ex.Floor, Is.EqualTo(floor));
            Assert.That(ex.InnerException, Is.SameAs(inner));
            Assert.That(ex.Message, Does.Contain(nameof(LatticeOptions.ReplicationClockFloorLag)));
        });
    }

    [Test]
    public void The_framework_constructors_leave_the_stamps_zero()
    {
        var inner = new InvalidOperationException("refused");
        var withInner = new LatticeIdempotencyKeyExpiredException("expired", inner);

        Assert.Multiple(() =>
        {
            Assert.That(new LatticeIdempotencyKeyExpiredException().KeyTimestamp, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(new LatticeIdempotencyKeyExpiredException("expired").Message, Is.EqualTo("expired"));
            Assert.That(withInner.InnerException, Is.SameAs(inner));
            Assert.That(withInner.Floor, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void It_is_not_a_transient_saturation_signal()
    {
        Assert.That(typeof(LatticeIdempotencyKeyExpiredException).BaseType, Is.EqualTo(typeof(Exception)),
            "derives directly from Exception: no same-silo copier, and no broad InvalidOperationException catch absorbs it");
    }
}

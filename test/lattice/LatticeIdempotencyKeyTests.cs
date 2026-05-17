using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the <see cref="LatticeIdempotencyKey"/> value type.
/// </summary>
[TestFixture]
public class LatticeIdempotencyKeyTests
{
    [Test]
    public void Default_value_has_zero_HLC()
    {
        var key = default(LatticeIdempotencyKey);
        Assert.That(key.Timestamp, Is.EqualTo(default(HybridLogicalClock)));
    }

    [Test]
    public void Init_assigns_timestamp()
    {
        var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var key = new LatticeIdempotencyKey { Timestamp = hlc };
        Assert.That(key.Timestamp, Is.EqualTo(hlc));
    }

    [Test]
    public void Equality_compares_timestamp()
    {
        var hlc1 = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var hlc2 = HybridLogicalClock.Tick(hlc1);
        var a = new LatticeIdempotencyKey { Timestamp = hlc1 };
        var b = new LatticeIdempotencyKey { Timestamp = hlc1 };
        var c = new LatticeIdempotencyKey { Timestamp = hlc2 };
        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Is.Not.EqualTo(c));
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
    }

    [Test]
    public void Fresh_returns_key_with_non_zero_timestamp()
    {
        var key = LatticeIdempotencyKey.Fresh();
        Assert.That(key.Timestamp, Is.Not.EqualTo(default(HybridLogicalClock)));
    }

    [Test]
    public void Two_Fresh_calls_produce_distinct_keys()
    {
        var a = LatticeIdempotencyKey.Fresh();
        var b = LatticeIdempotencyKey.Fresh();
        Assert.That(a, Is.Not.EqualTo(b),
            "Fresh() must advance HLC so consecutive calls do not collide.");
    }
}

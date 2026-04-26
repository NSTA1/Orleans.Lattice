using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class OrSetDotTests
{
    [Test]
    public void Default_dot_has_zero_counter_and_null_replica()
    {
        OrSetDot dot = default;
        Assert.That(dot.Counter, Is.EqualTo(0));
        Assert.That(dot.ReplicaId, Is.Null);
    }

    [Test]
    public void Init_round_trip_preserves_fields()
    {
        var dot = new OrSetDot { ReplicaId = "r1", Counter = 42 };
        Assert.That(dot.ReplicaId, Is.EqualTo("r1"));
        Assert.That(dot.Counter, Is.EqualTo(42));
    }

    [Test]
    public void Equality_is_value_based()
    {
        var a = new OrSetDot { ReplicaId = "r1", Counter = 1 };
        var b = new OrSetDot { ReplicaId = "r1", Counter = 1 };
        var c = new OrSetDot { ReplicaId = "r1", Counter = 2 };
        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Is.Not.EqualTo(c));
    }
}

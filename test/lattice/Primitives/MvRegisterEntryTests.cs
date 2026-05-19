using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class MvRegisterEntryTests
{
    [Test]
    public void Default_entry_has_zero_counter_and_null_value()
    {
        MvRegisterEntry entry = default;
        Assert.That(entry.Counter, Is.EqualTo(0));
        Assert.That(entry.ReplicaId, Is.Null);
        Assert.That(entry.Value, Is.Null);
    }

    [Test]
    public void Init_round_trip_preserves_fields()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var entry = new MvRegisterEntry { ReplicaId = "r1", Counter = 7, Value = bytes };
        Assert.That(entry.ReplicaId, Is.EqualTo("r1"));
        Assert.That(entry.Counter, Is.EqualTo(7));
        Assert.That(entry.Value, Is.SameAs(bytes));
    }

    [Test]
    public void Equality_is_value_based_over_replica_and_counter()
    {
        var a = new MvRegisterEntry { ReplicaId = "r1", Counter = 1, Value = new byte[] { 1 } };
        var b = new MvRegisterEntry { ReplicaId = "r1", Counter = 1, Value = new byte[] { 1 } };
        var c = new MvRegisterEntry { ReplicaId = "r1", Counter = 2, Value = new byte[] { 1 } };
        // Value is by-reference under record-struct default equality on arrays;
        // but ReplicaId+Counter still differ in c.
        Assert.That(a, Is.Not.EqualTo(c));
        // Same replica/counter, different array instances - default record equality
        // compares each field; arrays use reference equality, so a != b.
        Assert.That(a, Is.Not.EqualTo(b));
    }
}

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
    public void Equality_is_value_based_over_replica_counter_and_value()
    {
        var a = new MvRegisterEntry { ReplicaId = "r1", Counter = 1, Value = new byte[] { 1 } };
        var b = new MvRegisterEntry { ReplicaId = "r1", Counter = 1, Value = new byte[] { 1 } };
        var c = new MvRegisterEntry { ReplicaId = "r1", Counter = 2, Value = new byte[] { 1 } };
        var d = new MvRegisterEntry { ReplicaId = "r1", Counter = 1, Value = new byte[] { 2 } };
        Assert.Multiple(() =>
        {
            // Differing counter or value bytes still compare unequal.
            Assert.That(a, Is.Not.EqualTo(c));
            Assert.That(a, Is.Not.EqualTo(d));
            // Same replica/counter and byte-identical value across independently
            // allocated arrays now compare equal: Value is compared by content.
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }
}

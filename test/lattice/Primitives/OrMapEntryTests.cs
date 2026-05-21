using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class OrMapEntryTests
{
    [Test]
    public void Default_entry_has_initialised_value_and_empty_replica_id()
    {
        var entry = new OrMapEntry<OrSet>();
        Assert.That(entry.ReplicaId, Is.EqualTo(string.Empty));
        Assert.That(entry.Counter, Is.EqualTo(0));
        Assert.That(entry.Value, Is.Not.Null);
    }

    [Test]
    public void Properties_round_trip()
    {
        var value = new PnCounter();
        value.Increment("r1", 7);
        var entry = new OrMapEntry<PnCounter>
        {
            ReplicaId = "r1",
            Counter = 42,
            Value = value,
        };
        Assert.That(entry.ReplicaId, Is.EqualTo("r1"));
        Assert.That(entry.Counter, Is.EqualTo(42));
        Assert.That(entry.Value.Value, Is.EqualTo(7));
    }
}

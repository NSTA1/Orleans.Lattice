using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class OrMapDeltaEntryTests
{
    [Test]
    public void Default_instance_has_null_or_zero_fields()
    {
        var e = default(OrMapDeltaEntry<string, PnCounter>);
        Assert.Multiple(() =>
        {
            Assert.That(e.Key, Is.Null);
            Assert.That(e.ReplicaId, Is.Null);
            Assert.That(e.Counter, Is.EqualTo(0L));
            Assert.That(e.Value, Is.Null);
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var pc = new PnCounter();
        pc.Increment("r", 5);
        var e = new OrMapDeltaEntry<string, PnCounter>
        {
            Key = "k",
            ReplicaId = "r1",
            Counter = 42,
            Value = pc,
        };
        Assert.Multiple(() =>
        {
            Assert.That(e.Key, Is.EqualTo("k"));
            Assert.That(e.ReplicaId, Is.EqualTo("r1"));
            Assert.That(e.Counter, Is.EqualTo(42));
            Assert.That(e.Value, Is.SameAs(pc));
        });
    }
}

[TestFixture]
public class OrMapDeltaTombstoneTests
{
    [Test]
    public void Default_instance_has_null_or_zero_fields()
    {
        var t = default(OrMapDeltaTombstone<string>);
        Assert.Multiple(() =>
        {
            Assert.That(t.Key, Is.Null);
            Assert.That(t.ReplicaId, Is.Null);
            Assert.That(t.Counter, Is.EqualTo(0L));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var t = new OrMapDeltaTombstone<string> { Key = "k", ReplicaId = "r", Counter = 7 };
        Assert.Multiple(() =>
        {
            Assert.That(t.Key, Is.EqualTo("k"));
            Assert.That(t.ReplicaId, Is.EqualTo("r"));
            Assert.That(t.Counter, Is.EqualTo(7));
        });
    }
}

[TestFixture]
public class OrMapDeltaTests
{
    [Test]
    public void Default_instance_has_null_collections()
    {
        var d = default(OrMapDelta<string, PnCounter>);
        Assert.Multiple(() =>
        {
            Assert.That(d.Adds, Is.Null);
            Assert.That(d.Tombstones, Is.Null);
        });
    }

    [Test]
    public void Empty_factory_returns_non_null_empty_collections()
    {
        var d = OrMapDelta<string, PnCounter>.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(d.Adds, Is.Not.Null);
            Assert.That(d.Adds, Is.Empty);
            Assert.That(d.Tombstones, Is.Not.Null);
            Assert.That(d.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Empty_factory_does_not_allocate_on_repeated_access()
    {
        var first = OrMapDelta<string, PnCounter>.Empty;
        var second = OrMapDelta<string, PnCounter>.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(first.Adds, Is.SameAs(second.Adds));
            Assert.That(first.Tombstones, Is.SameAs(second.Tombstones));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var pc = new PnCounter();
        var add = new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r", Counter = 1, Value = pc };
        var tomb = new OrMapDeltaTombstone<string> { Key = "k", ReplicaId = "r", Counter = 1 };
        var d = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { add },
            Tombstones = new[] { tomb },
        };
        Assert.Multiple(() =>
        {
            Assert.That(d.Adds, Has.Count.EqualTo(1));
            Assert.That(d.Adds[0], Is.EqualTo(add));
            Assert.That(d.Tombstones, Has.Count.EqualTo(1));
            Assert.That(d.Tombstones[0], Is.EqualTo(tomb));
        });
    }
}
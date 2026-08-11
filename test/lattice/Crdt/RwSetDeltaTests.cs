namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class RwSetDeltaTests
{
    [Test]
    public void Default_instance_has_null_collections()
    {
        var delta = default(RwSetDelta);
        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Is.Null);
            Assert.That(delta.Removes, Is.Null);
            Assert.That(delta.Tombstones, Is.Null);
        });
    }

    [Test]
    public void Empty_factory_returns_non_null_empty_collections()
    {
        var delta = RwSetDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Is.Not.Null);
            Assert.That(delta.Adds, Is.Empty);
            Assert.That(delta.Removes, Is.Not.Null);
            Assert.That(delta.Removes, Is.Empty);
            Assert.That(delta.Tombstones, Is.Not.Null);
            Assert.That(delta.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Empty_factory_does_not_allocate_on_repeated_access()
    {
        var first = RwSetDelta.Empty;
        var second = RwSetDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(first.Adds, Is.SameAs(second.Adds));
            Assert.That(first.Removes, Is.SameAs(second.Removes));
            Assert.That(first.Tombstones, Is.SameAs(second.Tombstones));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var add = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r1", Counter = 3 };
        var remove = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r2", Counter = 7 };
        var tombstone = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r2", Counter = 5 };

        var delta = new RwSetDelta
        {
            Adds = new[] { add },
            Removes = new[] { remove },
            Tombstones = new[] { tombstone },
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Has.Count.EqualTo(1));
            Assert.That(delta.Adds[0], Is.EqualTo(add));
            Assert.That(delta.Removes, Has.Count.EqualTo(1));
            Assert.That(delta.Removes[0], Is.EqualTo(remove));
            Assert.That(delta.Tombstones, Has.Count.EqualTo(1));
            Assert.That(delta.Tombstones[0], Is.EqualTo(tombstone));
        });
    }

    [Test]
    public void Empty_collections_are_legal()
    {
        var delta = new RwSetDelta
        {
            Adds = Array.Empty<OrSetDeltaDot>(),
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Is.Empty);
            Assert.That(delta.Removes, Is.Empty);
            Assert.That(delta.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Equality_uses_value_equality_on_shared_collection_references()
    {
        var adds = new[] { new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r", Counter = 1 } };
        var removes = Array.Empty<OrSetDeltaDot>();
        var tombstones = Array.Empty<OrSetDeltaDot>();
        var a = new RwSetDelta { Adds = adds, Removes = removes, Tombstones = tombstones };
        var b = new RwSetDelta { Adds = adds, Removes = removes, Tombstones = tombstones };
        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Equality_uses_reference_equality_on_independently_allocated_collections()
    {
        // Documents the IReadOnlyList<> reference-equality caveat at the
        // delta level: two deltas built from independent arrays of equal
        // dots are NOT equal under record-struct equality.
        var dot = new OrSetDeltaDot { Element = new byte[] { 1 }, ReplicaId = "r", Counter = 1 };
        var a = new RwSetDelta { Adds = new[] { dot }, Removes = Array.Empty<OrSetDeltaDot>(), Tombstones = Array.Empty<OrSetDeltaDot>() };
        var b = new RwSetDelta { Adds = new[] { dot }, Removes = Array.Empty<OrSetDeltaDot>(), Tombstones = Array.Empty<OrSetDeltaDot>() };
        Assert.That(a, Is.Not.EqualTo(b));
    }
}

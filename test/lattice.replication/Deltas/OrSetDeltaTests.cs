using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Deltas;

[TestFixture]
public class OrSetDotTests
{
    [Test]
    public void Default_instance_has_null_or_zero_fields()
    {
        var dot = default(OrSetDot);
        Assert.Multiple(() =>
        {
            Assert.That(dot.Element, Is.Null);
            Assert.That(dot.ReplicaId, Is.Null);
            Assert.That(dot.Counter, Is.EqualTo(0L));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var bytes = new byte[] { 0xAB };
        var dot = new OrSetDot { Element = bytes, ReplicaId = "r1", Counter = 42L };

        Assert.Multiple(() =>
        {
            Assert.That(dot.Element, Is.SameAs(bytes));
            Assert.That(dot.ReplicaId, Is.EqualTo("r1"));
            Assert.That(dot.Counter, Is.EqualTo(42L));
        });
    }

    [Test]
    public void Equality_is_value_based_on_shared_element_reference()
    {
        var bytes = new byte[] { 1 };
        var a = new OrSetDot { Element = bytes, ReplicaId = "r", Counter = 1 };
        var b = new OrSetDot { Element = bytes, ReplicaId = "r", Counter = 1 };
        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Equality_uses_reference_equality_on_independently_allocated_element_arrays()
    {
        // Documents the byte[] reference-equality caveat: structurally
        // identical dots backed by independent arrays are NOT equal.
        var a = new OrSetDot { Element = new byte[] { 1 }, ReplicaId = "r", Counter = 1 };
        var b = new OrSetDot { Element = new byte[] { 1 }, ReplicaId = "r", Counter = 1 };
        Assert.That(a, Is.Not.EqualTo(b));
    }
}

[TestFixture]
public class OrSetDeltaTests
{
    [Test]
    public void Default_instance_has_null_collections()
    {
        var delta = default(OrSetDelta);
        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Is.Null);
            Assert.That(delta.Removes, Is.Null);
        });
    }

    [Test]
    public void Empty_factory_returns_non_null_empty_collections()
    {
        var delta = OrSetDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Is.Not.Null);
            Assert.That(delta.Adds, Is.Empty);
            Assert.That(delta.Removes, Is.Not.Null);
            Assert.That(delta.Removes, Is.Empty);
        });
    }

    [Test]
    public void Empty_factory_does_not_allocate_on_repeated_access()
    {
        var first = OrSetDelta.Empty;
        var second = OrSetDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(first.Adds, Is.SameAs(second.Adds));
            Assert.That(first.Removes, Is.SameAs(second.Removes));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var addOne = new OrSetDot { Element = new byte[] { 1 }, ReplicaId = "r1", Counter = 1 };
        var addTwo = new OrSetDot { Element = new byte[] { 2 }, ReplicaId = "r1", Counter = 2 };
        var remove = new OrSetDot { Element = new byte[] { 1 }, ReplicaId = "r2", Counter = 7 };

        var delta = new OrSetDelta
        {
            Adds = new[] { addOne, addTwo },
            Removes = new[] { remove },
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Has.Count.EqualTo(2));
            Assert.That(delta.Adds[0], Is.EqualTo(addOne));
            Assert.That(delta.Adds[1], Is.EqualTo(addTwo));
            Assert.That(delta.Removes, Has.Count.EqualTo(1));
            Assert.That(delta.Removes[0], Is.EqualTo(remove));
        });
    }

    [Test]
    public void Empty_collections_are_legal()
    {
        var delta = new OrSetDelta
        {
            Adds = Array.Empty<OrSetDot>(),
            Removes = Array.Empty<OrSetDot>(),
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds, Is.Empty);
            Assert.That(delta.Removes, Is.Empty);
        });
    }

    [Test]
    public void Equality_uses_reference_equality_on_independently_allocated_collections()
    {
        // Documents the IReadOnlyList<> reference-equality caveat at the
        // delta level: two deltas built from independent arrays of equal
        // dots are NOT equal under record-struct equality.
        var dot = new OrSetDot { Element = new byte[] { 1 }, ReplicaId = "r", Counter = 1 };
        var a = new OrSetDelta { Adds = new[] { dot }, Removes = Array.Empty<OrSetDot>() };
        var b = new OrSetDelta { Adds = new[] { dot }, Removes = Array.Empty<OrSetDot>() };
        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Equality_uses_value_equality_on_shared_collection_references()
    {
        var adds = new[] { new OrSetDot { Element = new byte[] { 1 }, ReplicaId = "r", Counter = 1 } };
        var removes = Array.Empty<OrSetDot>();
        var a = new OrSetDelta { Adds = adds, Removes = removes };
        var b = new OrSetDelta { Adds = adds, Removes = removes };
        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Adds_and_removes_can_target_the_same_element_with_different_dots()
    {
        var bytes = new byte[] { 9 };
        var add = new OrSetDot { Element = bytes, ReplicaId = "r1", Counter = 1 };
        var remove = new OrSetDot { Element = bytes, ReplicaId = "r2", Counter = 1 };

        var delta = new OrSetDelta
        {
            Adds = new[] { add },
            Removes = new[] { remove },
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Adds[0].ReplicaId, Is.EqualTo("r1"));
            Assert.That(delta.Removes[0].ReplicaId, Is.EqualTo("r2"));
            Assert.That(delta.Adds[0].Element, Is.SameAs(delta.Removes[0].Element));
        });
    }

    [Test]
    public void Zero_length_element_is_legal()
    {
        var dot = new OrSetDot { Element = Array.Empty<byte>(), ReplicaId = "r", Counter = 1 };
        Assert.That(dot.Element, Is.Empty);
    }
}

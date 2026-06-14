namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class RwFlagDeltaTests
{
    [Test]
    public void Default_instance_has_null_collections()
    {
        var delta = default(RwFlagDelta);
        Assert.Multiple(() =>
        {
            Assert.That(delta.Enables, Is.Null);
            Assert.That(delta.Disables, Is.Null);
            Assert.That(delta.Tombstones, Is.Null);
        });
    }

    [Test]
    public void Empty_factory_returns_non_null_empty_collections()
    {
        var delta = RwFlagDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(delta.Enables, Is.Not.Null);
            Assert.That(delta.Enables, Is.Empty);
            Assert.That(delta.Disables, Is.Not.Null);
            Assert.That(delta.Disables, Is.Empty);
            Assert.That(delta.Tombstones, Is.Not.Null);
            Assert.That(delta.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Empty_factory_does_not_allocate_on_repeated_access()
    {
        var first = RwFlagDelta.Empty;
        var second = RwFlagDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(first.Enables, Is.SameAs(second.Enables));
            Assert.That(first.Disables, Is.SameAs(second.Disables));
            Assert.That(first.Tombstones, Is.SameAs(second.Tombstones));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var enable = new OrSetDot { ReplicaId = "r1", Counter = 3 };
        var disable = new OrSetDot { ReplicaId = "r2", Counter = 7 };
        var tombstone = new OrSetDot { ReplicaId = "r2", Counter = 5 };

        var delta = new RwFlagDelta
        {
            Enables = new[] { enable },
            Disables = new[] { disable },
            Tombstones = new[] { tombstone },
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Enables, Has.Count.EqualTo(1));
            Assert.That(delta.Enables[0], Is.EqualTo(enable));
            Assert.That(delta.Disables, Has.Count.EqualTo(1));
            Assert.That(delta.Disables[0], Is.EqualTo(disable));
            Assert.That(delta.Tombstones, Has.Count.EqualTo(1));
            Assert.That(delta.Tombstones[0], Is.EqualTo(tombstone));
        });
    }

    [Test]
    public void Empty_collections_are_legal()
    {
        var delta = new RwFlagDelta
        {
            Enables = Array.Empty<OrSetDot>(),
            Disables = Array.Empty<OrSetDot>(),
            Tombstones = Array.Empty<OrSetDot>(),
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Enables, Is.Empty);
            Assert.That(delta.Disables, Is.Empty);
            Assert.That(delta.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Equality_uses_value_equality_on_shared_collection_references()
    {
        var enables = new[] { new OrSetDot { ReplicaId = "r", Counter = 1 } };
        var disables = Array.Empty<OrSetDot>();
        var tombstones = Array.Empty<OrSetDot>();
        var a = new RwFlagDelta { Enables = enables, Disables = disables, Tombstones = tombstones };
        var b = new RwFlagDelta { Enables = enables, Disables = disables, Tombstones = tombstones };
        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Equality_uses_reference_equality_on_independently_allocated_collections()
    {
        // Documents the IReadOnlyList<> reference-equality caveat at the
        // delta level: two deltas built from independent arrays of equal
        // dots are NOT equal under record-struct equality.
        var dot = new OrSetDot { ReplicaId = "r", Counter = 1 };
        var a = new RwFlagDelta { Enables = new[] { dot }, Disables = Array.Empty<OrSetDot>(), Tombstones = Array.Empty<OrSetDot>() };
        var b = new RwFlagDelta { Enables = new[] { dot }, Disables = Array.Empty<OrSetDot>(), Tombstones = Array.Empty<OrSetDot>() };
        Assert.That(a, Is.Not.EqualTo(b));
    }
}

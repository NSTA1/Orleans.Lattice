namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class OrFlagDeltaTests
{
    [Test]
    public void Default_instance_has_null_collections()
    {
        var delta = default(OrFlagDelta);
        Assert.Multiple(() =>
        {
            Assert.That(delta.Enables, Is.Null);
            Assert.That(delta.Disables, Is.Null);
        });
    }

    [Test]
    public void Empty_factory_returns_non_null_empty_collections()
    {
        var delta = OrFlagDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(delta.Enables, Is.Not.Null);
            Assert.That(delta.Enables, Is.Empty);
            Assert.That(delta.Disables, Is.Not.Null);
            Assert.That(delta.Disables, Is.Empty);
        });
    }

    [Test]
    public void Empty_factory_does_not_allocate_on_repeated_access()
    {
        var first = OrFlagDelta.Empty;
        var second = OrFlagDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(first.Enables, Is.SameAs(second.Enables));
            Assert.That(first.Disables, Is.SameAs(second.Disables));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var enableOne = new OrSetDot { ReplicaId = "r1", Counter = 1 };
        var enableTwo = new OrSetDot { ReplicaId = "r1", Counter = 2 };
        var disable = new OrSetDot { ReplicaId = "r2", Counter = 7 };

        var delta = new OrFlagDelta
        {
            Enables = new[] { enableOne, enableTwo },
            Disables = new[] { disable },
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Enables, Has.Count.EqualTo(2));
            Assert.That(delta.Enables[0], Is.EqualTo(enableOne));
            Assert.That(delta.Enables[1], Is.EqualTo(enableTwo));
            Assert.That(delta.Disables, Has.Count.EqualTo(1));
            Assert.That(delta.Disables[0], Is.EqualTo(disable));
        });
    }

    [Test]
    public void Empty_collections_are_legal()
    {
        var delta = new OrFlagDelta
        {
            Enables = Array.Empty<OrSetDot>(),
            Disables = Array.Empty<OrSetDot>(),
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Enables, Is.Empty);
            Assert.That(delta.Disables, Is.Empty);
        });
    }

    [Test]
    public void Equality_uses_value_equality_on_shared_collection_references()
    {
        var enables = new[] { new OrSetDot { ReplicaId = "r", Counter = 1 } };
        var disables = Array.Empty<OrSetDot>();
        var a = new OrFlagDelta { Enables = enables, Disables = disables };
        var b = new OrFlagDelta { Enables = enables, Disables = disables };
        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Equality_is_value_based_on_independently_allocated_collections()
    {
        // Two deltas built from independent arrays of equal dots now compare
        // equal: Enables and Disables are compared element-by-element using
        // the value equality of OrSetDot, not by collection reference, so a
        // delta and its post-serialization self compare equal.
        var dot = new OrSetDot { ReplicaId = "r", Counter = 1 };
        var a = new OrFlagDelta { Enables = new[] { dot }, Disables = Array.Empty<OrSetDot>() };
        var b = new OrFlagDelta { Enables = new[] { dot }, Disables = Array.Empty<OrSetDot>() };
        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }
}

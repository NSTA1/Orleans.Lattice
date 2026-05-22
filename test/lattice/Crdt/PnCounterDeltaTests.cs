
namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class PnCounterDeltaTests
{
    [Test]
    public void Default_instance_has_null_dictionaries()
    {
        var delta = default(PnCounterDelta);
        Assert.Multiple(() =>
        {
            Assert.That(delta.Increments, Is.Null);
            Assert.That(delta.Decrements, Is.Null);
        });
    }

    [Test]
    public void Empty_factory_returns_non_null_empty_dictionaries()
    {
        var delta = PnCounterDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(delta.Increments, Is.Not.Null);
            Assert.That(delta.Increments, Is.Empty);
            Assert.That(delta.Decrements, Is.Not.Null);
            Assert.That(delta.Decrements, Is.Empty);
        });
    }

    [Test]
    public void Empty_factory_does_not_allocate_on_repeated_access()
    {
        var first = PnCounterDelta.Empty;
        var second = PnCounterDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(first.Increments, Is.SameAs(second.Increments));
            Assert.That(first.Decrements, Is.SameAs(second.Decrements));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var inc = new Dictionary<string, long> { ["r1"] = 5, ["r2"] = 3 };
        var dec = new Dictionary<string, long> { ["r1"] = 1 };
        var delta = new PnCounterDelta { Increments = inc, Decrements = dec };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Increments, Has.Count.EqualTo(2));
            Assert.That(delta.Increments["r1"], Is.EqualTo(5L));
            Assert.That(delta.Increments["r2"], Is.EqualTo(3L));
            Assert.That(delta.Decrements, Has.Count.EqualTo(1));
            Assert.That(delta.Decrements["r1"], Is.EqualTo(1L));
        });
    }

    [Test]
    public void Empty_dictionaries_are_legal()
    {
        var delta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(),
            Decrements = new Dictionary<string, long>(),
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Increments, Is.Empty);
            Assert.That(delta.Decrements, Is.Empty);
        });
    }

    [Test]
    public void Equality_uses_reference_equality_on_independently_allocated_dictionaries()
    {
        var a = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["r"] = 1 },
            Decrements = new Dictionary<string, long>(),
        };
        var b = new PnCounterDelta
        {
            Increments = new Dictionary<string, long> { ["r"] = 1 },
            Decrements = new Dictionary<string, long>(),
        };
        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Equality_uses_value_equality_on_shared_dictionary_references()
    {
        var inc = new Dictionary<string, long> { ["r"] = 1 };
        var dec = new Dictionary<string, long>();
        var a = new PnCounterDelta { Increments = inc, Decrements = dec };
        var b = new PnCounterDelta { Increments = inc, Decrements = dec };
        Assert.That(a, Is.EqualTo(b));
    }
}

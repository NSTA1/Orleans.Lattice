using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Deltas;

[TestFixture]
public class VersionVectorDeltaTests
{
    [Test]
    public void Default_instance_has_null_entries()
    {
        var delta = default(VersionVectorDelta);
        Assert.That(delta.Entries, Is.Null);
    }

    [Test]
    public void Empty_factory_returns_non_null_empty_entries()
    {
        var delta = VersionVectorDelta.Empty;
        Assert.Multiple(() =>
        {
            Assert.That(delta.Entries, Is.Not.Null);
            Assert.That(delta.Entries, Is.Empty);
        });
    }

    [Test]
    public void Empty_factory_does_not_allocate_on_repeated_access()
    {
        var first = VersionVectorDelta.Empty;
        var second = VersionVectorDelta.Empty;
        Assert.That(first.Entries, Is.SameAs(second.Entries));
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var clockOne = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var clockTwo = HybridLogicalClock.Tick(clockOne);
        var entries = new Dictionary<string, HybridLogicalClock>
        {
            ["r1"] = clockOne,
            ["r2"] = clockTwo,
        };
        var delta = new VersionVectorDelta { Entries = entries };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Entries, Has.Count.EqualTo(2));
            Assert.That(delta.Entries["r1"], Is.EqualTo(clockOne));
            Assert.That(delta.Entries["r2"], Is.EqualTo(clockTwo));
        });
    }

    [Test]
    public void Empty_dictionary_represents_a_noop_delta()
    {
        var delta = new VersionVectorDelta { Entries = new Dictionary<string, HybridLogicalClock>() };
        Assert.That(delta.Entries, Is.Empty);
    }

    [Test]
    public void Equality_uses_reference_equality_on_independently_allocated_entries()
    {
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var a = new VersionVectorDelta { Entries = new Dictionary<string, HybridLogicalClock> { ["r"] = clock } };
        var b = new VersionVectorDelta { Entries = new Dictionary<string, HybridLogicalClock> { ["r"] = clock } };
        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Equality_uses_value_equality_on_shared_dictionary_reference()
    {
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var entries = new Dictionary<string, HybridLogicalClock> { ["r"] = clock };
        var a = new VersionVectorDelta { Entries = entries };
        var b = new VersionVectorDelta { Entries = entries };
        Assert.That(a, Is.EqualTo(b));
    }
}

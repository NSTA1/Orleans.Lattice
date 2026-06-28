namespace Orleans.Lattice.Tests.Crdt;

[TestFixture]
public class CrdtProvenanceDeltaTests
{
    [Test]
    public void Constructor_null_delta_throws()
    {
        Assert.That(() => new CrdtProvenanceDelta(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_without_wall_clock_leaves_it_null()
    {
        var entry = new CrdtProvenanceDelta(OrSetDelta.Empty);

        Assert.Multiple(() =>
        {
            Assert.That((OrSetDelta)entry.Delta, Is.EqualTo(OrSetDelta.Empty));
            Assert.That(entry.WallClock, Is.Null);
        });
    }

    [Test]
    public void Constructor_with_wall_clock_carries_it()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 99, Counter = 1 };

        var entry = new CrdtProvenanceDelta(OrSetDelta.Empty, hlc);

        Assert.That(entry.WallClock, Is.EqualTo(hlc));
    }
}

[TestFixture]
public class CrdtMemberChangeTests
{
    [Test]
    public void Default_instance_has_null_or_zero_fields()
    {
        var change = default(CrdtMemberChange);

        Assert.Multiple(() =>
        {
            Assert.That(change.Element, Is.Null);
            Assert.That(change.Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
            Assert.That(change.ReplicaId, Is.Null);
            Assert.That(change.Ordinal, Is.EqualTo(0L));
            Assert.That(change.WallClock, Is.Null);
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var element = "x"u8.ToArray();
        var hlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 1 };

        var change = new CrdtMemberChange
        {
            Element = element,
            Kind = CrdtMemberChangeKind.Removed,
            ReplicaId = "r1",
            Ordinal = 9,
            WallClock = hlc,
        };

        Assert.Multiple(() =>
        {
            Assert.That(change.Element, Is.SameAs(element));
            Assert.That(change.Kind, Is.EqualTo(CrdtMemberChangeKind.Removed));
            Assert.That(change.ReplicaId, Is.EqualTo("r1"));
            Assert.That(change.Ordinal, Is.EqualTo(9L));
            Assert.That(change.WallClock, Is.EqualTo(hlc));
        });
    }
}

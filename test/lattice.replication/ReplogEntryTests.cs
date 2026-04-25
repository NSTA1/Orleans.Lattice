using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplogEntryTests
{
    [Test]
    public void Default_instance_has_empty_or_zero_fields()
    {
        var entry = default(ReplogEntry);
        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.Null);
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.Set));
            Assert.That(entry.Key, Is.Null);
            Assert.That(entry.EndExclusiveKey, Is.Null);
            Assert.That(entry.Value, Is.Null);
            Assert.That(entry.Timestamp, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(entry.IsTombstone, Is.False);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(0L));
            Assert.That(entry.OriginClusterId, Is.Null);
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var bytes = new byte[] { 1, 2 };
        var entry = new ReplogEntry
        {
            TreeId = "tree",
            Op = ReplogOp.Delete,
            Key = "k",
            EndExclusiveKey = "z",
            Value = bytes,
            Timestamp = ts,
            IsTombstone = true,
            ExpiresAtTicks = 42L,
            OriginClusterId = "site-a",
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.TreeId, Is.EqualTo("tree"));
            Assert.That(entry.Op, Is.EqualTo(ReplogOp.Delete));
            Assert.That(entry.Key, Is.EqualTo("k"));
            Assert.That(entry.EndExclusiveKey, Is.EqualTo("z"));
            Assert.That(entry.Value, Is.SameAs(bytes));
            Assert.That(entry.Timestamp, Is.EqualTo(ts));
            Assert.That(entry.IsTombstone, Is.True);
            Assert.That(entry.ExpiresAtTicks, Is.EqualTo(42L));
            Assert.That(entry.OriginClusterId, Is.EqualTo("site-a"));
        });
    }

    [Test]
    public void Equality_uses_value_semantics_on_shared_byte_references()
    {
        var bytes = new byte[] { 9 };
        var c = new ReplogEntry { TreeId = "t", Key = "k", Value = bytes };
        var d = new ReplogEntry { TreeId = "t", Key = "k", Value = bytes };
        Assert.That(c, Is.EqualTo(d));
    }
}

[TestFixture]
public class ReplogOpTests
{
    [Test]
    public void Underlying_values_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)ReplogOp.Set, Is.EqualTo(0));
            Assert.That((int)ReplogOp.Delete, Is.EqualTo(1));
            Assert.That((int)ReplogOp.DeleteRange, Is.EqualTo(2));
        });
    }
}

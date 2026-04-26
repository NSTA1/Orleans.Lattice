using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Deltas;

[TestFixture]
public class LwwRegisterDeltaTests
{
    [Test]
    public void Default_instance_has_null_or_zero_fields()
    {
        var delta = default(LwwRegisterDelta);
        Assert.Multiple(() =>
        {
            Assert.That(delta.Value, Is.Null);
            Assert.That(delta.Timestamp, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(delta.IsTombstone, Is.False);
            Assert.That(delta.OriginClusterId, Is.Null);
            Assert.That(delta.ExpiresAtTicks, Is.EqualTo(0L));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var bytes = new byte[] { 1, 2, 3 };
        var delta = new LwwRegisterDelta
        {
            Value = bytes,
            Timestamp = ts,
            IsTombstone = false,
            OriginClusterId = "site-a",
            ExpiresAtTicks = 1234L,
        };

        Assert.Multiple(() =>
        {
            Assert.That(delta.Value, Is.SameAs(bytes));
            Assert.That(delta.Timestamp, Is.EqualTo(ts));
            Assert.That(delta.IsTombstone, Is.False);
            Assert.That(delta.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(delta.ExpiresAtTicks, Is.EqualTo(1234L));
        });
    }

    [Test]
    public void Tombstone_factory_produces_value_null_and_is_tombstone_true()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var delta = LwwRegisterDelta.Tombstone(ts, "site-b");

        Assert.Multiple(() =>
        {
            Assert.That(delta.IsTombstone, Is.True);
            Assert.That(delta.Value, Is.Null);
            Assert.That(delta.Timestamp, Is.EqualTo(ts));
            Assert.That(delta.OriginClusterId, Is.EqualTo("site-b"));
            Assert.That(delta.ExpiresAtTicks, Is.EqualTo(0L));
        });
    }

    [Test]
    public void Tombstone_factory_default_origin_is_null()
    {
        var delta = LwwRegisterDelta.Tombstone(HybridLogicalClock.Zero);
        Assert.That(delta.OriginClusterId, Is.Null);
    }

    [Test]
    public void Equality_is_value_based_on_shared_value_reference()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var bytes = new byte[] { 7, 8 };
        var a = new LwwRegisterDelta { Value = bytes, Timestamp = ts, OriginClusterId = "x" };
        var b = new LwwRegisterDelta { Value = bytes, Timestamp = ts, OriginClusterId = "x" };
        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Equality_distinguishes_origin_cluster_id()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var a = new LwwRegisterDelta { Timestamp = ts, OriginClusterId = "x" };
        var b = new LwwRegisterDelta { Timestamp = ts, OriginClusterId = "y" };
        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Equality_distinguishes_expires_at_ticks()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var a = new LwwRegisterDelta { Timestamp = ts, ExpiresAtTicks = 100L };
        var b = new LwwRegisterDelta { Timestamp = ts, ExpiresAtTicks = 200L };
        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Equality_distinguishes_tombstone_flag()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var a = new LwwRegisterDelta { Timestamp = ts, IsTombstone = false };
        var b = new LwwRegisterDelta { Timestamp = ts, IsTombstone = true };
        Assert.That(a, Is.Not.EqualTo(b));
    }
}

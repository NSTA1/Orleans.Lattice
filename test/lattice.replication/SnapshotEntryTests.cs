using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class SnapshotEntryTests
{
    [Test]
    public void Default_instance_has_empty_or_zero_fields()
    {
        var entry = default(SnapshotEntry);
        Assert.Multiple(() =>
        {
            Assert.That(entry.Key, Is.Null);
            Assert.That(entry.Value, Is.Null);
            Assert.That(entry.Timestamp, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public void Properties_are_settable_via_object_initialiser()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var bytes = new byte[] { 9, 9 };
        var entry = new SnapshotEntry
        {
            Key = "k",
            Value = bytes,
            Timestamp = ts,
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.Key, Is.EqualTo("k"));
            Assert.That(entry.Value, Is.SameAs(bytes));
            Assert.That(entry.Timestamp, Is.EqualTo(ts));
        });
    }

    [Test]
    public void Equality_is_value_based_on_key_and_timestamp_and_reference_value()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var bytes = new byte[] { 1 };
        var a = new SnapshotEntry { Key = "k", Value = bytes, Timestamp = ts };
        var b = new SnapshotEntry { Key = "k", Value = bytes, Timestamp = ts };
        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Different_keys_are_not_equal()
    {
        var ts = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var a = new SnapshotEntry { Key = "k1", Value = new byte[] { 1 }, Timestamp = ts };
        var b = a with { Key = "k2" };
        Assert.That(a, Is.Not.EqualTo(b));
    }
}

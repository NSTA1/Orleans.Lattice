using Orleans.Lattice.BPlusTree.Grains;
namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class DeadLetterEntryTests
{
    [Test]
    public void Default_value_has_zero_id_and_null_failure_reason()
    {
        var entry = default(DeadLetterEntry);
        Assert.Multiple(() =>
        {
            Assert.That(entry.EntryId, Is.EqualTo(0L));
            Assert.That(entry.RetryCount, Is.EqualTo(0));
            Assert.That(entry.EnqueuedAtTicks, Is.EqualTo(0L));
            Assert.That(entry.FailureReason, Is.Null);
        });
    }

    [Test]
    public void With_initialiser_round_trips_every_property()
    {
        var inner = new WalRecord { TreeId = "t", Key = "k", OriginClusterId = "o" };
        var entry = new DeadLetterEntry
        {
            EntryId = 42,
            Entry = inner,
            FailureReason = "boom",
            RetryCount = 3,
            EnqueuedAtTicks = 12345,
        };

        Assert.Multiple(() =>
        {
            Assert.That(entry.EntryId, Is.EqualTo(42L));
            Assert.That(entry.Entry.TreeId, Is.EqualTo("t"));
            Assert.That(entry.Entry.Key, Is.EqualTo("k"));
            Assert.That(entry.Entry.OriginClusterId, Is.EqualTo("o"));
            Assert.That(entry.FailureReason, Is.EqualTo("boom"));
            Assert.That(entry.RetryCount, Is.EqualTo(3));
            Assert.That(entry.EnqueuedAtTicks, Is.EqualTo(12345L));
        });
    }

    [Test]
    public void Two_entries_with_the_same_payload_are_equal()
    {
        var inner = new WalRecord { TreeId = "t", Key = "k", OriginClusterId = "o" };
        var a = new DeadLetterEntry { EntryId = 1, Entry = inner, FailureReason = "x", RetryCount = 1, EnqueuedAtTicks = 0 };
        var b = new DeadLetterEntry { EntryId = 1, Entry = inner, FailureReason = "x", RetryCount = 1, EnqueuedAtTicks = 0 };

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
    }
}

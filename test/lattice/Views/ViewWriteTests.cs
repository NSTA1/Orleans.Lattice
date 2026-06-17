namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for the <see cref="ViewWrite"/> value type and its factories.</summary>
[TestFixture]
public class ViewWriteTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public void Upsert_sets_kind_key_value_and_timestamp()
    {
        var value = new byte[] { 1, 2, 3 };
        var write = ViewWrite.Upsert("k", value, Clock(10));

        Assert.That(write.Kind, Is.EqualTo(ViewWriteKind.Upsert));
        Assert.That(write.Key, Is.EqualTo("k"));
        Assert.That(write.Value, Is.SameAs(value));
        Assert.That(write.Timestamp, Is.EqualTo(Clock(10)));
        Assert.That(write.ExpiresAtTicks, Is.EqualTo(0));
    }

    [Test]
    public void Upsert_with_expiry_sets_expires_at_ticks()
    {
        var write = ViewWrite.Upsert("k", [9], Clock(10), expiresAtTicks: 12345);
        Assert.That(write.ExpiresAtTicks, Is.EqualTo(12345));
    }

    [Test]
    public void Upsert_null_key_throws()
    {
        Assert.That(() => ViewWrite.Upsert(null!, [1], Clock(1)), Throws.ArgumentNullException);
    }

    [Test]
    public void Upsert_null_value_throws()
    {
        Assert.That(() => ViewWrite.Upsert("k", null!, Clock(1)), Throws.ArgumentNullException);
    }

    [Test]
    public void Delete_sets_kind_and_null_value()
    {
        var write = ViewWrite.Delete("k", Clock(7));

        Assert.That(write.Kind, Is.EqualTo(ViewWriteKind.Delete));
        Assert.That(write.Key, Is.EqualTo("k"));
        Assert.That(write.Value, Is.Null);
        Assert.That(write.Timestamp, Is.EqualTo(Clock(7)));
    }

    [Test]
    public void Delete_null_key_throws()
    {
        Assert.That(() => ViewWrite.Delete(null!, Clock(1)), Throws.ArgumentNullException);
    }
}

namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for <see cref="ViewWriteCoalescer"/> last-writer-wins coalescing.</summary>
[TestFixture]
public class ViewWriteCoalescerTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public void Coalesce_null_throws()
    {
        Assert.That(() => ViewWriteCoalescer.Coalesce(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Coalesce_empty_returns_empty()
    {
        Assert.That(ViewWriteCoalescer.Coalesce([]), Is.Empty);
    }

    [Test]
    public void Coalesce_distinct_keys_all_survive_in_first_seen_order()
    {
        var result = ViewWriteCoalescer.Coalesce(
        [
            ViewWrite.Upsert("b", [1], Clock(1)),
            ViewWrite.Upsert("a", [2], Clock(1)),
            ViewWrite.Upsert("c", [3], Clock(1)),
        ]);

        Assert.That(result.Select(w => w.Key), Is.EqualTo(new[] { "b", "a", "c" }));
    }

    [Test]
    public void Coalesce_keeps_highest_timestamp_per_key()
    {
        var result = ViewWriteCoalescer.Coalesce(
        [
            ViewWrite.Upsert("k", [1], Clock(1)),
            ViewWrite.Upsert("k", [2], Clock(3)),
            ViewWrite.Upsert("k", [3], Clock(2)),
        ]);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0].Value, Is.EqualTo(new byte[] { 2 }));
        Assert.That(result[0].Timestamp, Is.EqualTo(Clock(3)));
    }

    [Test]
    public void Coalesce_tie_keeps_first_seen()
    {
        var result = ViewWriteCoalescer.Coalesce(
        [
            ViewWrite.Upsert("k", [1], Clock(5)),
            ViewWrite.Upsert("k", [2], Clock(5)),
        ]);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0].Value, Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public void Coalesce_later_delete_with_higher_timestamp_wins_over_upsert()
    {
        var result = ViewWriteCoalescer.Coalesce(
        [
            ViewWrite.Upsert("k", [1], Clock(1)),
            ViewWrite.Delete("k", Clock(2)),
        ]);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0].Kind, Is.EqualTo(ViewWriteKind.Delete));
    }

    [Test]
    public void Coalesce_earlier_delete_loses_to_later_upsert()
    {
        var result = ViewWriteCoalescer.Coalesce(
        [
            ViewWrite.Delete("k", Clock(1)),
            ViewWrite.Upsert("k", [9], Clock(2)),
        ]);

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0].Kind, Is.EqualTo(ViewWriteKind.Upsert));
        Assert.That(result[0].Value, Is.EqualTo(new byte[] { 9 }));
    }
}

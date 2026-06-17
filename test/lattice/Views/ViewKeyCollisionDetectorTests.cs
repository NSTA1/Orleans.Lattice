namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for <see cref="ViewKeyCollisionDetector"/>.</summary>
[TestFixture]
public class ViewKeyCollisionDetectorTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    [Test]
    public void Detect_no_writes_returns_empty()
    {
        Assert.That(ViewKeyCollisionDetector.Detect([]), Is.Empty);
    }

    [Test]
    public void Detect_distinct_view_keys_returns_empty()
    {
        var writes = new[]
        {
            ViewWrite.Upsert("v1", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Upsert("v2", [2], Clock(2), sourceKey: "b"),
        };

        Assert.That(ViewKeyCollisionDetector.Detect(writes), Is.Empty);
    }

    [Test]
    public void Detect_same_source_key_repeated_is_not_a_collision()
    {
        // Two updates to the same source key map to the same view key; that is an
        // update stream, not a collision.
        var writes = new[]
        {
            ViewWrite.Upsert("v", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Upsert("v", [2], Clock(2), sourceKey: "a"),
        };

        Assert.That(ViewKeyCollisionDetector.Detect(writes), Is.Empty);
    }

    [Test]
    public void Detect_two_distinct_source_keys_one_view_key_is_a_collision()
    {
        var writes = new[]
        {
            ViewWrite.Upsert("v", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Upsert("v", [2], Clock(2), sourceKey: "b"),
        };

        Assert.That(ViewKeyCollisionDetector.Detect(writes), Is.EqualTo(new[] { "v" }));
    }

    [Test]
    public void Detect_reports_each_colliding_view_key_once()
    {
        var writes = new[]
        {
            ViewWrite.Upsert("v", [1], Clock(1), sourceKey: "a"),
            ViewWrite.Upsert("v", [2], Clock(2), sourceKey: "b"),
            ViewWrite.Upsert("v", [3], Clock(3), sourceKey: "c"),
        };

        Assert.That(ViewKeyCollisionDetector.Detect(writes), Is.EqualTo(new[] { "v" }));
    }

    [Test]
    public void Detect_ignores_writes_without_source_key()
    {
        var writes = new[]
        {
            ViewWrite.Upsert("v", [1], Clock(1)),
            ViewWrite.Upsert("v", [2], Clock(2)),
        };

        Assert.That(ViewKeyCollisionDetector.Detect(writes), Is.Empty);
    }

    [Test]
    public void Detect_null_writes_throws()
    {
        Assert.That(() => ViewKeyCollisionDetector.Detect(null!), Throws.ArgumentNullException);
    }
}

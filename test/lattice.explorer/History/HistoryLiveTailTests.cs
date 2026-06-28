using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Tests.History;

[TestFixture]
public class HistoryLiveTailTests
{
    [Test]
    public void TryAccept_MatchingKey_YieldsLiveRow()
    {
        var tail = new HistoryLiveTail("k");

        var accepted = tail.TryAccept(NotificationFactory.Set("k", 10), out var row);

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.True);
            Assert.That(row, Is.Not.Null);
            Assert.That(row!.IsLiveTail, Is.True);
            Assert.That(row.Hlc.WallClockTicks, Is.EqualTo(10));
        });
    }

    [Test]
    public void TryAccept_DifferentKey_FilteredOut()
    {
        var tail = new HistoryLiveTail("k");

        var accepted = tail.TryAccept(NotificationFactory.Set("other", 10), out var row);

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False);
            Assert.That(row, Is.Null);
            Assert.That(tail.SeenCount, Is.Zero, "a filtered notification must not touch the seen-set");
        });
    }

    [Test]
    public void TryAccept_DuplicateClock_DeDuplicated()
    {
        var tail = new HistoryLiveTail("k");

        Assert.That(tail.TryAccept(NotificationFactory.Set("k", 10, position: "p1"), out _), Is.True);
        // Same clock, different opaque position (an at-least-once redelivery).
        var second = tail.TryAccept(NotificationFactory.Set("k", 10, position: "p2"), out var row);

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.False);
            Assert.That(row, Is.Null);
            Assert.That(tail.SeenCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void TryAccept_OverlapWithLoadedPage_DeDuplicated()
    {
        // Seed with an already-loaded revision at clock 10; the live tail must not
        // re-surface it where the retrospective page and live tail overlap.
        var loaded = new[] { HistoryRevisionRow.From(RevisionFactory.Set(10, value: "v")) };
        var tail = new HistoryLiveTail("k", loaded);

        var overlap = tail.TryAccept(NotificationFactory.Set("k", 10), out var overlapRow);
        var fresh = tail.TryAccept(NotificationFactory.Set("k", 20), out var freshRow);

        Assert.Multiple(() =>
        {
            Assert.That(overlap, Is.False, "clock 10 is already on the loaded page");
            Assert.That(overlapRow, Is.Null);
            Assert.That(fresh, Is.True, "clock 20 is past the loaded page");
            Assert.That(freshRow, Is.Not.Null);
        });
    }

    [Test]
    public void Covers_RangeDeleteSpanningKey_Matches()
    {
        var inside = NotificationFactory.DeleteRange("a", "m", 5);
        var outside = NotificationFactory.DeleteRange("a", "k", 6);

        Assert.Multiple(() =>
        {
            Assert.That(HistoryLiveTail.Covers(inside, "key"), Is.True, "key is within [a, m)");
            Assert.That(HistoryLiveTail.Covers(outside, "key"), Is.False, "key is outside [a, k)");
        });
    }

    [Test]
    public void Constructor_NullKey_Throws()
    {
        Assert.That(() => new HistoryLiveTail(null!), Throws.ArgumentNullException);
    }
}

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class LwwValueTests
{
    [Test]
    public void Merge_keeps_value_with_higher_timestamp()
    {
        var older = LwwValue<string>.Create("old", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var newer = LwwValue<string>.Create("new", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        Assert.That(LwwValue<string>.Merge(older, newer).Value, Is.EqualTo("new"));
        Assert.That(LwwValue<string>.Merge(newer, older).Value, Is.EqualTo("new"));
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = LwwValue<int>.Create(1, new HybridLogicalClock { WallClockTicks = 10, Counter = 0 });
        var b = LwwValue<int>.Create(2, new HybridLogicalClock { WallClockTicks = 20, Counter = 0 });

        Assert.That(LwwValue<int>.Merge(b, a), Is.EqualTo(LwwValue<int>.Merge(a, b)));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 5, Counter = 0 });
        Assert.That(LwwValue<string>.Merge(v, v), Is.EqualTo(v));
    }

    [Test]
    public void Tombstone_wins_when_timestamp_is_higher()
    {
        var live = LwwValue<string>.Create("alive", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var dead = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        var result = LwwValue<string>.Merge(live, dead);
        Assert.That(result.IsTombstone, Is.True);
    }

    [Test]
    public void Live_value_wins_when_timestamp_is_higher_than_tombstone()
    {
        var dead = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var live = LwwValue<string>.Create("resurrected", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        var result = LwwValue<string>.Merge(dead, live);
        Assert.That(result.IsTombstone, Is.False);
        Assert.That(result.Value, Is.EqualTo("resurrected"));
    }

    // --- TTL / ExpiresAtTicks ---

    [Test]
    public void Create_defaults_ExpiresAtTicks_to_zero()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(v.ExpiresAtTicks, Is.EqualTo(0L));
    }

    [Test]
    public void CreateWithExpiry_sets_ExpiresAtTicks()
    {
        var v = LwwValue<string>.CreateWithExpiry(
            "x",
            new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            expiresAtTicks: 12345L);
        Assert.That(v.ExpiresAtTicks, Is.EqualTo(12345L));
        Assert.That(v.Value, Is.EqualTo("x"));
        Assert.That(v.IsTombstone, Is.False);
    }

    [Test]
    public void IsExpired_returns_false_for_non_expiring_entry()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(v.IsExpired(long.MaxValue), Is.False);
    }

    [Test]
    public void IsExpired_returns_true_when_now_passes_expiry()
    {
        var v = LwwValue<string>.CreateWithExpiry(
            "x",
            new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            expiresAtTicks: 1000L);
        Assert.That(v.IsExpired(999L), Is.False);
        Assert.That(v.IsExpired(1000L), Is.True);
        Assert.That(v.IsExpired(1001L), Is.True);
    }

    [Test]
    public void IsExpired_returns_false_for_tombstones_even_with_expiry_set()
    {
        // Tombstones are already "deleted" — expiry is only meaningful for live values.
        var tomb = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(tomb.IsExpired(long.MaxValue), Is.False);
    }

    [Test]
    public void Merge_preserves_expiry_of_winning_value()
    {
        var older = LwwValue<string>.Create("old", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var newerWithExpiry = LwwValue<string>.CreateWithExpiry(
            "new",
            new HybridLogicalClock { WallClockTicks = 2, Counter = 0 },
            expiresAtTicks: 999L);

        var winner = LwwValue<string>.Merge(older, newerWithExpiry);
        Assert.That(winner.Value, Is.EqualTo("new"));
        Assert.That(winner.ExpiresAtTicks, Is.EqualTo(999L));
    }
}

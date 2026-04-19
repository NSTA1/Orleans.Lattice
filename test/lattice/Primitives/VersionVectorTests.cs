using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class VersionVectorTests
{
    [Test]
    public void GetClock_returns_zero_for_unknown_replica()
    {
        var vv = new VersionVector();
        Assert.That(vv.GetClock("unknown"), Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public void Tick_advances_clock_for_replica()
    {
        var vv = new VersionVector();
        var first = vv.Tick("r1");
        var second = vv.Tick("r1");
        Assert.That(second > first, Is.True);
        Assert.That(vv.GetClock("r1"), Is.EqualTo(second));
    }

    [Test]
    public void Tick_is_independent_per_replica()
    {
        var vv = new VersionVector();
        vv.Tick("r1");
        vv.Tick("r1");
        var r2Clock = vv.Tick("r2");

        Assert.That(vv.GetClock("r1") > HybridLogicalClock.Zero, Is.True);
        Assert.That(r2Clock > HybridLogicalClock.Zero, Is.True);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new VersionVector();
        a.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };
        a.Entries["r2"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        var b = new VersionVector();
        b.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 15, Counter = 0 };
        b.Entries["r3"] = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };

        var ab = VersionVector.Merge(a, b);
        var ba = VersionVector.Merge(b, a);

        Assert.That(ba.GetClock("r1"), Is.EqualTo(ab.GetClock("r1")));
        Assert.That(ba.GetClock("r2"), Is.EqualTo(ab.GetClock("r2")));
        Assert.That(ba.GetClock("r3"), Is.EqualTo(ab.GetClock("r3")));
    }

    [Test]
    public void Merge_takes_pointwise_max()
    {
        var a = new VersionVector();
        a.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var b = new VersionVector();
        b.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        var merged = VersionVector.Merge(a, b);
        Assert.That(merged.GetClock("r1").WallClockTicks, Is.EqualTo(20));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new VersionVector();
        a.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 5 };

        var merged = VersionVector.Merge(a, a);
        Assert.That(merged.GetClock("r1"), Is.EqualTo(a.GetClock("r1")));
    }

    [Test]
    public void Merge_unions_disjoint_replicas()
    {
        var a = new VersionVector();
        a.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var b = new VersionVector();
        b.Entries["r2"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        var merged = VersionVector.Merge(a, b);
        Assert.That(merged.GetClock("r1").WallClockTicks, Is.EqualTo(10));
        Assert.That(merged.GetClock("r2").WallClockTicks, Is.EqualTo(20));
    }

    [Test]
    public void DominatesOrEquals_returns_true_when_all_entries_are_covered()
    {
        var local = new VersionVector();
        local.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };
        local.Entries["r2"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var other = new VersionVector();
        other.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 15, Counter = 0 };

        Assert.That(local.DominatesOrEquals(other), Is.True);
    }

    [Test]
    public void DominatesOrEquals_returns_false_when_other_has_newer_entry()
    {
        var local = new VersionVector();
        local.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var other = new VersionVector();
        other.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        Assert.That(local.DominatesOrEquals(other), Is.False);
    }

    [Test]
    public void IsNewerThan_detects_advancement()
    {
        var newer = new VersionVector();
        newer.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        var older = new VersionVector();
        older.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        Assert.That(newer.IsNewerThan(older), Is.True);
        Assert.That(older.IsNewerThan(newer), Is.False);
    }

    [Test]
    public void Clone_produces_independent_copy()
    {
        var original = new VersionVector();
        original.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var clone = original.Clone();
        clone.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 99, Counter = 0 };

        Assert.That(original.GetClock("r1").WallClockTicks, Is.EqualTo(10));
        Assert.That(clone.GetClock("r1").WallClockTicks, Is.EqualTo(99));
    }

    [Test]
    public void PruneOlderThan_removes_entries_below_cutoff()
    {
        var vv = new VersionVector();
        vv.Entries["old"] = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        vv.Entries["mid"] = new HybridLogicalClock { WallClockTicks = 500, Counter = 0 };
        vv.Entries["new"] = new HybridLogicalClock { WallClockTicks = 1_000, Counter = 0 };

        var removed = vv.PruneOlderThan(500);

        Assert.That(removed, Is.EqualTo(1));
        Assert.That(vv.Entries.ContainsKey("old"), Is.False);
        Assert.That(vv.Entries.ContainsKey("mid"), Is.True);
        Assert.That(vv.Entries.ContainsKey("new"), Is.True);
    }

    [Test]
    public void PruneOlderThan_returns_zero_on_empty_vector()
    {
        var vv = new VersionVector();
        Assert.That(vv.PruneOlderThan(long.MaxValue), Is.EqualTo(0));
    }

    [Test]
    public void PruneOlderThan_keeps_entries_at_cutoff()
    {
        var vv = new VersionVector();
        vv.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 500, Counter = 0 };

        var removed = vv.PruneOlderThan(500);

        Assert.That(removed, Is.EqualTo(0));
        Assert.That(vv.Entries.ContainsKey("r1"), Is.True);
    }

    [Test]
    public void PruneOlderThan_drops_all_when_cutoff_high()
    {
        var vv = new VersionVector();
        vv.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        vv.Entries["r2"] = new HybridLogicalClock { WallClockTicks = 2, Counter = 0 };

        var removed = vv.PruneOlderThan(long.MaxValue);

        Assert.That(removed, Is.EqualTo(2));
        Assert.That(vv.Entries, Is.Empty);
    }
}

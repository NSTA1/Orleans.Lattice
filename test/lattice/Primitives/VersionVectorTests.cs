using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class VersionVectorTests
{
    [Fact]
    public void GetClock_returns_zero_for_unknown_replica()
    {
        var vv = new VersionVector();
        Assert.Equal(HybridLogicalClock.Zero, vv.GetClock("unknown"));
    }

    [Fact]
    public void Tick_advances_clock_for_replica()
    {
        var vv = new VersionVector();
        var first = vv.Tick("r1");
        var second = vv.Tick("r1");
        Assert.True(second > first);
        Assert.Equal(second, vv.GetClock("r1"));
    }

    [Fact]
    public void Tick_is_independent_per_replica()
    {
        var vv = new VersionVector();
        vv.Tick("r1");
        vv.Tick("r1");
        var r2Clock = vv.Tick("r2");

        Assert.True(vv.GetClock("r1") > HybridLogicalClock.Zero);
        Assert.True(r2Clock > HybridLogicalClock.Zero);
    }

    [Fact]
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

        Assert.Equal(ab.GetClock("r1"), ba.GetClock("r1"));
        Assert.Equal(ab.GetClock("r2"), ba.GetClock("r2"));
        Assert.Equal(ab.GetClock("r3"), ba.GetClock("r3"));
    }

    [Fact]
    public void Merge_takes_pointwise_max()
    {
        var a = new VersionVector();
        a.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var b = new VersionVector();
        b.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        var merged = VersionVector.Merge(a, b);
        Assert.Equal(20, merged.GetClock("r1").WallClockTicks);
    }

    [Fact]
    public void Merge_is_idempotent()
    {
        var a = new VersionVector();
        a.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 5 };

        var merged = VersionVector.Merge(a, a);
        Assert.Equal(a.GetClock("r1"), merged.GetClock("r1"));
    }

    [Fact]
    public void Merge_unions_disjoint_replicas()
    {
        var a = new VersionVector();
        a.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var b = new VersionVector();
        b.Entries["r2"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        var merged = VersionVector.Merge(a, b);
        Assert.Equal(10, merged.GetClock("r1").WallClockTicks);
        Assert.Equal(20, merged.GetClock("r2").WallClockTicks);
    }

    [Fact]
    public void DominatesOrEquals_returns_true_when_all_entries_are_covered()
    {
        var local = new VersionVector();
        local.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };
        local.Entries["r2"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var other = new VersionVector();
        other.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 15, Counter = 0 };

        Assert.True(local.DominatesOrEquals(other));
    }

    [Fact]
    public void DominatesOrEquals_returns_false_when_other_has_newer_entry()
    {
        var local = new VersionVector();
        local.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var other = new VersionVector();
        other.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        Assert.False(local.DominatesOrEquals(other));
    }

    [Fact]
    public void IsNewerThan_detects_advancement()
    {
        var newer = new VersionVector();
        newer.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };

        var older = new VersionVector();
        older.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        Assert.True(newer.IsNewerThan(older));
        Assert.False(older.IsNewerThan(newer));
    }

    [Fact]
    public void Clone_produces_independent_copy()
    {
        var original = new VersionVector();
        original.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };

        var clone = original.Clone();
        clone.Entries["r1"] = new HybridLogicalClock { WallClockTicks = 99, Counter = 0 };

        Assert.Equal(10, original.GetClock("r1").WallClockTicks);
        Assert.Equal(99, clone.GetClock("r1").WallClockTicks);
    }
}

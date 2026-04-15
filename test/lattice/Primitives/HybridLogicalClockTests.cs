using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class HybridLogicalClockTests
{
    [Fact]
    public void Tick_advances_beyond_previous()
    {
        var a = HybridLogicalClock.Zero;
        var b = HybridLogicalClock.Tick(a);
        Assert.True(b > a);
    }

    [Fact]
    public void Tick_is_monotonic_across_multiple_calls()
    {
        var clock = HybridLogicalClock.Zero;
        for (int i = 0; i < 100; i++)
        {
            var next = HybridLogicalClock.Tick(clock);
            Assert.True(next > clock);
            clock = next;
        }
    }

    [Fact]
    public void Merge_returns_value_greater_than_both_inputs()
    {
        var a = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var b = HybridLogicalClock.Tick(a);
        var merged = HybridLogicalClock.Merge(a, b);
        Assert.True(merged > a);
        Assert.True(merged > b);
    }

    [Fact]
    public void Merge_is_commutative()
    {
        // Use wall clock values far in the future so DateTimeOffset.UtcNow
        // does not dominate and the merge is purely input-driven.
        var a = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = 3 };
        var b = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = 7 };
        var ab = HybridLogicalClock.Merge(a, b);
        var ba = HybridLogicalClock.Merge(b, a);
        Assert.Equal(ab, ba);
    }

    [Fact]
    public void CompareTo_orders_by_wall_clock_then_counter()
    {
        var a = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 };
        var b = new HybridLogicalClock { WallClockTicks = 100, Counter = 2 };
        var c = new HybridLogicalClock { WallClockTicks = 200, Counter = 0 };

        Assert.True(a < b);
        Assert.True(b < c);
        Assert.True(a < c);
    }

    [Fact]
    public void Zero_is_default()
    {
        var zero = HybridLogicalClock.Zero;
        Assert.Equal(0, zero.WallClockTicks);
        Assert.Equal(0, zero.Counter);
    }
}

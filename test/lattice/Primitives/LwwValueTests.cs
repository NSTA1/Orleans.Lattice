using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class LwwValueTests
{
    [Fact]
    public void Merge_keeps_value_with_higher_timestamp()
    {
        var older = LwwValue<string>.Create("old", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var newer = LwwValue<string>.Create("new", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        Assert.Equal("new", LwwValue<string>.Merge(older, newer).Value);
        Assert.Equal("new", LwwValue<string>.Merge(newer, older).Value);
    }

    [Fact]
    public void Merge_is_commutative()
    {
        var a = LwwValue<int>.Create(1, new HybridLogicalClock { WallClockTicks = 10, Counter = 0 });
        var b = LwwValue<int>.Create(2, new HybridLogicalClock { WallClockTicks = 20, Counter = 0 });

        Assert.Equal(LwwValue<int>.Merge(a, b), LwwValue<int>.Merge(b, a));
    }

    [Fact]
    public void Merge_is_idempotent()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 5, Counter = 0 });
        Assert.Equal(v, LwwValue<string>.Merge(v, v));
    }

    [Fact]
    public void Tombstone_wins_when_timestamp_is_higher()
    {
        var live = LwwValue<string>.Create("alive", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var dead = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        var result = LwwValue<string>.Merge(live, dead);
        Assert.True(result.IsTombstone);
    }

    [Fact]
    public void Live_value_wins_when_timestamp_is_higher_than_tombstone()
    {
        var dead = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var live = LwwValue<string>.Create("resurrected", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        var result = LwwValue<string>.Merge(dead, live);
        Assert.False(result.IsTombstone);
        Assert.Equal("resurrected", result.Value);
    }
}

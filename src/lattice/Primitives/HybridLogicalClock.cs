using Orleans.Lattice;

namespace Orleans.Lattice.Primitives;

/// <summary>
/// A simple hybrid logical clock (HLC) that combines wall-clock time with a
/// monotonic counter to produce totally-ordered, conflict-free timestamps.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.HybridLogicalClock)]
[Immutable]
public readonly record struct HybridLogicalClock
{
    [Id(0)] public long WallClockTicks { get; init; }
    [Id(1)] public int Counter { get; init; }

    public static HybridLogicalClock Zero => default;

    /// <summary>
    /// Advances the clock for a local event. The returned value is guaranteed
    /// to be strictly greater than <paramref name="previous"/>.
    /// </summary>
    public static HybridLogicalClock Tick(HybridLogicalClock previous)
    {
        var now = DateTimeOffset.UtcNow.Ticks;
        if (now > previous.WallClockTicks)
        {
            return new HybridLogicalClock { WallClockTicks = now, Counter = 0 };
        }

        return new HybridLogicalClock
        {
            WallClockTicks = previous.WallClockTicks,
            Counter = previous.Counter + 1
        };
    }

    /// <summary>
    /// Merges two clock values, returning a value strictly greater than both.
    /// The result is deterministic given the same inputs (commutative, associative,
    /// idempotent on ordering). Wall-clock time is incorporated to keep the
    /// clock advancing, but the merge itself is symmetric.
    /// </summary>
    public static HybridLogicalClock Merge(HybridLogicalClock local, HybridLogicalClock remote)
    {
        var now = DateTimeOffset.UtcNow.Ticks;
        var maxInput = Math.Max(local.WallClockTicks, remote.WallClockTicks);
        var maxWall = Math.Max(now, maxInput);

        int counter;
        if (maxWall > maxInput)
        {
            // Wall clock jumped ahead of both inputs — reset counter.
            counter = 0;
        }
        else if (local.WallClockTicks == remote.WallClockTicks)
        {
            // Both share the winning wall clock — bump past the higher counter.
            counter = Math.Max(local.Counter, remote.Counter) + 1;
        }
        else if (local.WallClockTicks > remote.WallClockTicks)
        {
            counter = local.Counter + 1;
        }
        else
        {
            counter = remote.Counter + 1;
        }

        return new HybridLogicalClock { WallClockTicks = maxWall, Counter = counter };
    }

    public int CompareTo(HybridLogicalClock other)
    {
        var cmp = WallClockTicks.CompareTo(other.WallClockTicks);
        return cmp != 0 ? cmp : Counter.CompareTo(other.Counter);
    }

    public static bool operator <(HybridLogicalClock left, HybridLogicalClock right) => left.CompareTo(right) < 0;
    public static bool operator >(HybridLogicalClock left, HybridLogicalClock right) => left.CompareTo(right) > 0;
    public static bool operator <=(HybridLogicalClock left, HybridLogicalClock right) => left.CompareTo(right) <= 0;
    public static bool operator >=(HybridLogicalClock left, HybridLogicalClock right) => left.CompareTo(right) >= 0;

    public override string ToString() => $"HLC({WallClockTicks}:{Counter})";
}

using System.ComponentModel;

namespace Orleans.Lattice;

/// <summary>
/// A simple hybrid logical clock (HLC) that combines wall-clock time with a
/// monotonic counter to produce totally-ordered, conflict-free timestamps.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.HybridLogicalClock)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public readonly record struct HybridLogicalClock
{
    [Id(0)] public long WallClockTicks { get; init; }
    [Id(1)] public int Counter { get; init; }

    public static HybridLogicalClock Zero => default;

    /// <summary>
    /// Advances the clock for a local event. The returned value is guaranteed
    /// to be strictly greater than <paramref name="previous"/>, except at the
    /// <see cref="int.MaxValue"/> counter ceiling where it saturates (see
    /// <see cref="BumpCounter"/>).
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
            Counter = BumpCounter(previous.Counter)
        };
    }

    /// <summary>
    /// Advances a counter by one, saturating at <see cref="int.MaxValue"/>.
    /// <para>
    /// An unchecked <c>+ 1</c> at the ceiling wraps to <see cref="int.MinValue"/>,
    /// which makes the successor compare strictly <em>less</em> than its own
    /// input and permanently inverts causality for that wall-clock tick: every
    /// value authored after the wrap is ordered before every value authored
    /// before it, so an LWW merge resurrects stale writes. Saturation keeps the
    /// order non-decreasing. The ceiling is only reachable while the wall clock
    /// is not advancing (a clock pinned at or beyond <c>DateTimeOffset.UtcNow</c>,
    /// e.g. a hand-authored or corrupted timestamp arriving over the wire), since
    /// any wall-clock advance resets the counter to zero.
    /// </para>
    /// </summary>
    private static int BumpCounter(int counter) =>
        counter == int.MaxValue ? int.MaxValue : counter + 1;

    /// <summary>
    /// Merges two clock values, returning a value greater than or equal to both
    /// and strictly greater than both except at the <see cref="int.MaxValue"/>
    /// counter ceiling, where it saturates.
    /// <para>
    /// The result is deterministic given the same inputs and the merge itself is
    /// commutative. It is deliberately <em>not</em> a join: it advances the
    /// counter past the winning input so the merged clock is a successor of both,
    /// which means it is neither idempotent (<c>Merge(a, a) != a</c>) nor
    /// associative (grouping changes how many bumps are applied). Wall-clock time
    /// is incorporated to keep the clock advancing.
    /// </para>
    /// </summary>
    public static HybridLogicalClock Merge(HybridLogicalClock local, HybridLogicalClock remote)
    {
        var now = DateTimeOffset.UtcNow.Ticks;
        var maxInput = Math.Max(local.WallClockTicks, remote.WallClockTicks);
        var maxWall = Math.Max(now, maxInput);

        int counter;
        if (maxWall > maxInput)
        {
            // Wall clock jumped ahead of both inputs - reset counter.
            counter = 0;
        }
        else if (local.WallClockTicks == remote.WallClockTicks)
        {
            // Both share the winning wall clock - bump past the higher counter.
            counter = BumpCounter(Math.Max(local.Counter, remote.Counter));
        }
        else if (local.WallClockTicks > remote.WallClockTicks)
        {
            counter = BumpCounter(local.Counter);
        }
        else
        {
            counter = BumpCounter(remote.Counter);
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

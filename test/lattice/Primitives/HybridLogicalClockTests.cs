using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class HybridLogicalClockTests
{
    [Test]
    public void Tick_advances_beyond_previous()
    {
        var a = HybridLogicalClock.Zero;
        var b = HybridLogicalClock.Tick(a);
        Assert.That(b > a, Is.True);
    }

    [Test]
    public void Tick_is_monotonic_across_multiple_calls()
    {
        var clock = HybridLogicalClock.Zero;
        for (int i = 0; i < 100; i++)
        {
            var next = HybridLogicalClock.Tick(clock);
            Assert.That(next > clock, Is.True);
            clock = next;
        }
    }

    [Test]
    public void Merge_returns_value_greater_than_both_inputs()
    {
        var a = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var b = HybridLogicalClock.Tick(a);
        var merged = HybridLogicalClock.Merge(a, b);
        Assert.That(merged > a, Is.True);
        Assert.That(merged > b, Is.True);
    }

    [Test]
    public void Merge_is_commutative()
    {
        // Use wall clock values far in the future so DateTimeOffset.UtcNow
        // does not dominate and the merge is purely input-driven.
        var a = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = 3 };
        var b = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = 7 };
        var ab = HybridLogicalClock.Merge(a, b);
        var ba = HybridLogicalClock.Merge(b, a);
        Assert.That(ba, Is.EqualTo(ab));
    }

    [Test]
    public void CompareTo_orders_by_wall_clock_then_counter()
    {
        var a = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 };
        var b = new HybridLogicalClock { WallClockTicks = 100, Counter = 2 };
        var c = new HybridLogicalClock { WallClockTicks = 200, Counter = 0 };

        Assert.That(a < b, Is.True);
        Assert.That(b < c, Is.True);
        Assert.That(a < c, Is.True);
    }

    [Test]
    public void Zero_is_default()
    {
        var zero = HybridLogicalClock.Zero;
        Assert.That(zero.WallClockTicks, Is.EqualTo(0));
        Assert.That(zero.Counter, Is.EqualTo(0));
    }

    // Counter is an int and every bump is an unchecked `+ 1`. A clock that
    // reaches int.MaxValue wraps to int.MinValue, which makes the "strictly
    // greater than previous" guarantee Tick documents collapse and inverts
    // every CompareTo against the pre-wrap value - a silent, permanent
    // causality inversion for that wall-clock tick. Saturating at the ceiling
    // keeps the order non-decreasing.

    [Test]
    public void Tick_at_the_counter_ceiling_does_not_wrap_negative()
    {
        // Wall clock far in the future so DateTimeOffset.UtcNow cannot dominate
        // and reset the counter.
        var previous = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = int.MaxValue };

        var next = HybridLogicalClock.Tick(previous);

        Assert.That(next.Counter, Is.GreaterThanOrEqualTo(0),
            "an unchecked counter bump at int.MaxValue wraps to int.MinValue and inverts causality");
    }

    [Test]
    public void Tick_at_the_counter_ceiling_is_not_ordered_before_its_input()
    {
        var previous = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = int.MaxValue };

        var next = HybridLogicalClock.Tick(previous);

        Assert.That(next >= previous, Is.True,
            "Tick documents a value strictly greater than its input; wrapping produces a strictly lesser one");
    }

    [Test]
    public void Merge_at_the_counter_ceiling_does_not_wrap_negative()
    {
        var a = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = int.MaxValue };
        var b = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = 5 };

        var merged = HybridLogicalClock.Merge(a, b);

        Assert.That(merged.Counter, Is.GreaterThanOrEqualTo(0),
            "Merge documents a value strictly greater than both inputs; wrapping produces a lesser one");
    }

    [Test]
    public void Merge_at_the_counter_ceiling_is_not_ordered_before_either_input()
    {
        var a = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = int.MaxValue };
        var b = new HybridLogicalClock { WallClockTicks = long.MaxValue - 200, Counter = int.MaxValue };

        var merged = HybridLogicalClock.Merge(a, b);

        Assert.Multiple(() =>
        {
            Assert.That(merged >= a, Is.True);
            Assert.That(merged >= b, Is.True);
        });
    }

    [Test]
    public void Merge_saturates_rather_than_advancing_past_the_counter_ceiling()
    {
        var a = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = int.MaxValue };

        Assert.That(HybridLogicalClock.Merge(a, a).Counter, Is.EqualTo(int.MaxValue));
    }

    // Merge is commutative but is NOT a join: it bumps the counter past the
    // winning input, so it is neither idempotent (Merge(a, a) != a) nor
    // associative. Both properties are pinned here so the XML doc and the
    // behaviour stay in step.

    [Test]
    public void Merge_is_not_idempotent_because_it_advances_the_counter()
    {
        var a = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = 3 };

        Assert.That(HybridLogicalClock.Merge(a, a), Is.Not.EqualTo(a),
            "Merge advances the clock by construction; it is a strictly-greater successor, not a join");
    }

    [Test]
    public void Merge_is_not_associative_because_it_advances_the_counter()
    {
        var a = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = 0 };
        var b = new HybridLogicalClock { WallClockTicks = long.MaxValue - 100, Counter = 5 };
        var c = new HybridLogicalClock { WallClockTicks = long.MaxValue - 50, Counter = 0 };

        var leftAssociated = HybridLogicalClock.Merge(HybridLogicalClock.Merge(a, b), c);
        var rightAssociated = HybridLogicalClock.Merge(a, HybridLogicalClock.Merge(b, c));

        Assert.That(rightAssociated, Is.Not.EqualTo(leftAssociated),
            "grouping changes how many counter bumps are applied, so the result differs");
    }
}

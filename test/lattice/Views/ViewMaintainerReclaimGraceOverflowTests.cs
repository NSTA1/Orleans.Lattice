using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression coverage for the swapped-out-generation reclaim-eligibility tick
/// arithmetic (a sibling of the expiry-overflow class fixed in #2221). An
/// unbounded <see cref="LatticeViewOptions.OldGenerationReclaimGrace"/> added
/// directly to <c>DateTime.UtcNow.Ticks</c> silently wraps past
/// <c>long.MaxValue</c> to a negative (past) instant, which flips the reclaim
/// gate from "wait out the grace" to "eligible immediately" and deletes the
/// swapped-out generation tree while it may still be serving live readers - the
/// exact inverse of the option's intent.
/// </summary>
[TestFixture]
public class ViewMaintainerReclaimGraceOverflowTests
{
    [Test]
    public void SaturatingReclaimEligibleAtTicks_saturates_instead_of_wrapping_negative()
    {
        var now = DateTime.UtcNow.Ticks;

        var eligible = ViewMaintainerGrain.SaturatingReclaimEligibleAtTicks(now, TimeSpan.MaxValue);

        // The unguarded arithmetic this replaces wraps to a past instant.
        Assert.That(
            unchecked(now + TimeSpan.MaxValue.Ticks),
            Is.LessThan(now),
            "precondition: the raw tick sum overflows negative for an extreme grace");
        Assert.Multiple(() =>
        {
            Assert.That(eligible, Is.EqualTo(DateTime.MaxValue.Ticks));
            Assert.That(
                eligible,
                Is.GreaterThan(now),
                "an extreme grace must defer reclaim, never make it immediately eligible");
        });
    }

    [Test]
    public void SaturatingReclaimEligibleAtTicks_is_exact_for_ordinary_grace()
    {
        var now = DateTime.UtcNow.Ticks;
        var grace = TimeSpan.FromMinutes(5);

        var eligible = ViewMaintainerGrain.SaturatingReclaimEligibleAtTicks(now, grace);

        Assert.That(eligible, Is.EqualTo(now + grace.Ticks));
    }
}

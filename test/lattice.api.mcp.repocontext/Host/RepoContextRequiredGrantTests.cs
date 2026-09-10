using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers <see cref="RepoContextShutdownBudget.RequiredGrantFor"/>, the inverse of
/// the budget derivation added for issue #2598.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2598 turns on a trap worth stating in a test: the shipped 90s budget is
/// <b>derived from</b> an assumed 120s grant, so declaring
/// <c>stop_grace_period: 120s</c> changes nothing at all. The reflex remedy is
/// therefore a no-op, and the only way to escape that trap is to work backwards from
/// a measured drain to the grant that would have covered it. That is what this
/// member does, and these tests pin the round-trip property that makes its answer
/// trustworthy: a grant it reports must derive a budget that actually covers the
/// drain it was asked about.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextRequiredGrantTests
{
    [Test]
    public void The_grant_required_for_the_gate_run_two_drain_is_well_past_the_value_already_assumed()
    {
        // 102.1s is the drain measured on gate run 2, against a 90s budget derived
        // from an assumed 120s grant. If this came back at or under 120s the whole
        // change would be circular.
        var required = RepoContextShutdownBudget.RequiredGrantFor(TimeSpan.FromSeconds(102.1));

        Assert.Multiple(() =>
        {
            Assert.That(required.TotalSeconds, Is.EqualTo(137d));
            Assert.That(
                required,
                Is.GreaterThan(RepoContextShutdownBudget.DefaultStopGracePeriod),
                "declaring the grant the derivation already assumes is the no-op #2598 warns about");
        });
    }

    [Test]
    [TestCase(0.5)]
    [TestCase(1)]
    [TestCase(10)]
    [TestCase(67.2)]
    [TestCase(102.1)]
    [TestCase(600)]
    public void A_grant_this_reports_survives_the_F0_rendering_it_is_reported_through(double seconds)
    {
        // The property that actually protects the operator, and it is stronger than
        // the round-trip below. This figure is reported through a ":F0" format and
        // copied out of the log into a compose file, so the number that has to cover
        // the drain is the RENDERED one, not the TimeSpan. The exact real-valued
        // inverse of 102.1s is 136.1333s, which renders as "136" and derives a 102s
        // budget - short of the very drain that produced it. Rounding up to a whole
        // second is what closes that gap, and this asserts the closure at the point
        // it is observed rather than at the point it is computed.
        var drain = TimeSpan.FromSeconds(seconds);

        var rendered = double.Parse(
            RepoContextShutdownBudget.RequiredGrantFor(drain).TotalSeconds.ToString("F0"),
            System.Globalization.CultureInfo.InvariantCulture);

        Assert.That(
            RepoContextShutdownBudget.Derive(TimeSpan.FromSeconds(rendered)),
            Is.GreaterThanOrEqualTo(drain),
            "an operator who copies the printed figure exactly must get a budget that covers the drain");
    }

    [Test]
    [TestCase(0.5)]
    [TestCase(1)]
    [TestCase(10)]
    [TestCase(67.2)]
    [TestCase(102.1)]
    [TestCase(600)]
    public void A_grant_this_reports_always_derives_a_budget_that_covers_the_drain_it_was_asked_about(double seconds)
    {
        // The property that makes the answer usable rather than merely plausible.
        // Derive() is min(0.75g, g - 2s), so a naive inverse of only the first term
        // would under-report for short drains where the reserve dominates.
        var drain = TimeSpan.FromSeconds(seconds);

        var required = RepoContextShutdownBudget.RequiredGrantFor(drain);

        Assert.That(
            RepoContextShutdownBudget.Derive(required),
            Is.GreaterThanOrEqualTo(drain),
            "a grant that does not derive a budget covering the drain is worthless advice");
    }

    [Test]
    public void A_reported_grant_is_always_a_whole_number_of_seconds()
    {
        // Guards the rounding itself rather than only its consequence, so that a
        // change reintroducing the raw quotient fails here with a message naming the
        // cause instead of only in the rendering property above.
        var required = RepoContextShutdownBudget.RequiredGrantFor(TimeSpan.FromSeconds(102.1));

        Assert.That(
            required.TotalSeconds % 1d,
            Is.Zero,
            "the figure is printed with :F0 and copied into a compose file, so a fractional "
            + "answer is one that renders as a different number from the one it computed");
    }

    [Test]
    public void A_short_drain_is_covered_by_the_unwind_reserve_term_rather_than_the_fraction()
    {
        // Below 8s the reserve term dominates: 0.75 * g leaves more room than g - 2s.
        // Reporting max() of the two arms is what keeps the round-trip property above
        // holding across the crossover.
        var required = RepoContextShutdownBudget.RequiredGrantFor(TimeSpan.FromSeconds(1));

        Assert.That(required, Is.EqualTo(TimeSpan.FromSeconds(3)));
    }

    [Test]
    public void A_long_drain_is_covered_by_the_fraction_term_rather_than_the_reserve()
    {
        Assert.That(
            RepoContextShutdownBudget.RequiredGrantFor(TimeSpan.FromSeconds(90)),
            Is.EqualTo(TimeSpan.FromSeconds(120)),
            "90s is exactly the budget the shipped 120s assumption derives, so it must invert to 120s");
    }

    [Test]
    public void A_zero_drain_needs_no_grant_beyond_the_reserve()
    {
        Assert.That(
            RepoContextShutdownBudget.RequiredGrantFor(TimeSpan.Zero),
            Is.EqualTo(RepoContextShutdownBudget.UnwindReserve));
    }

    [Test]
    public void A_negative_drain_is_rejected_rather_than_inverted_into_a_negative_grant()
    {
        Assert.That(
            () => RepoContextShutdownBudget.RequiredGrantFor(TimeSpan.FromSeconds(-1)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}

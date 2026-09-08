using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextExactScanBudget"/>, which decides how
/// large a corpus the exact kNN gather may visit before the scan is no longer
/// worth starting.
/// <para>
/// The property under test is that the threshold is <b>derived</b>: it is a
/// projection of the two scan-page options the vector-metadata tree is configured
/// with, so retuning either moves it, and no row count is written down anywhere.
/// The second property is that every case it cannot compute fails <b>open</b>, so
/// the only behaviour it removes is a scan the configuration itself says cannot
/// complete.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextExactScanBudgetTests
{
    private const int PageSize = RepoContextPortability.DefaultPageSize;

    private static RepoContextExactScanBudget Budget(
        LatticeOptions options, TimeSpan? responseTimeout = null)
        => RepoContextExactScanBudgets.From(options, responseTimeout);

    [Test]
    public void The_shipped_defaults_afford_the_pages_the_stall_ceiling_covers()
    {
        var budget = Budget(new LatticeOptions());

        Assert.That(budget.AffordableVectorCount, Is.EqualTo(5 * PageSize),
            "The Orleans default 30s response timeout derives a 25s stall ceiling, which covers five 5s "
            + "cooperative page budgets. The measured field ceiling was exactly 00:00:25.");
    }

    [Test]
    public void Relaxing_the_stall_ceiling_raises_the_threshold_proportionally()
    {
        var tighter = Budget(new LatticeOptions { MaxScanPageStallDuration = TimeSpan.FromSeconds(25) });
        var looser = Budget(new LatticeOptions { MaxScanPageStallDuration = TimeSpan.FromSeconds(50) });

        Assert.Multiple(() =>
        {
            Assert.That(tighter.AffordableVectorCount, Is.EqualTo(5 * PageSize));
            Assert.That(looser.AffordableVectorCount, Is.EqualTo(10 * PageSize),
                "The threshold has to track the configured budget. A row count written into the source would "
                + "not, and would go quietly wrong the first time an operator retuned the ceiling.");
        });
    }

    [Test]
    public void Raising_the_nominal_page_budget_lowers_the_threshold()
    {
        var budget = Budget(new LatticeOptions
        {
            MaxScanPageStallDuration = TimeSpan.FromSeconds(25),
            MaxScanPageDuration = TimeSpan.FromSeconds(12.5),
        });

        Assert.That(budget.AffordableVectorCount, Is.EqualTo(2 * PageSize),
            "A page fill the store expects to cost more buys fewer of them inside the same ceiling.");
    }

    [Test]
    public void An_explicit_stall_ceiling_overrides_the_response_timeout_derivation()
    {
        var budget = Budget(
            new LatticeOptions { MaxScanPageStallDuration = TimeSpan.FromSeconds(25) },
            responseTimeout: TimeSpan.FromMinutes(10));

        Assert.That(budget.AffordableVectorCount, Is.EqualTo(5 * PageSize),
            "An explicitly configured ceiling is the effective ceiling; the derivation is only for the unset case.");
    }

    [Test]
    public void An_unset_ceiling_is_derived_from_the_configured_response_timeout()
    {
        var budget = Budget(new LatticeOptions(), responseTimeout: TimeSpan.FromSeconds(20));

        Assert.That(budget.AffordableVectorCount, Is.EqualTo(3 * PageSize),
            "20s less the 5s headroom is a 15s ceiling, which covers three 5s page budgets. A deployment that "
            + "tightens its response timeout tightens this with it, with no second knob to remember.");
    }

    [Test]
    public void A_derived_ceiling_is_floored_at_the_cooperative_page_budget()
    {
        var options = new LatticeOptions { MaxScanPageDuration = TimeSpan.FromSeconds(5) };
        var budget = Budget(options, responseTimeout: TimeSpan.FromSeconds(6));

        Assert.Multiple(() =>
        {
            Assert.That(budget.EffectiveStallCeiling(options), Is.EqualTo(TimeSpan.FromSeconds(5)),
                "6s less the 5s headroom would derive a 1s ceiling that fires instantly; the core resolver "
                + "floors it at the graceful budget and this mirror must agree.");
            Assert.That(budget.AffordableVectorCount, Is.EqualTo(PageSize),
                "One page is the floor: a threshold of zero would skip every gather, including the small ones "
                + "the exact path exists to serve.");
        });
    }

    [Test]
    public void A_disabled_stall_ceiling_is_unbounded()
    {
        var budget = Budget(new LatticeOptions { MaxScanPageStallDuration = Timeout.InfiniteTimeSpan });

        Assert.That(budget.AffordableVectorCount, Is.EqualTo(RepoContextExactScanBudget.Unbounded),
            "A deployment that deliberately restored the unbounded wait has no ceiling for a gather to trip, "
            + "so there is nothing to protect it from.");
    }

    [Test]
    public void An_infinite_response_timeout_with_no_explicit_ceiling_is_unbounded()
    {
        var budget = Budget(new LatticeOptions(), responseTimeout: Timeout.InfiniteTimeSpan);

        Assert.That(budget.AffordableVectorCount, Is.EqualTo(RepoContextExactScanBudget.Unbounded),
            "No RPC deadline to stay ahead of means no ceiling to derive, exactly as the core resolver decides.");
    }

    [Test]
    public void A_disabled_cooperative_page_budget_is_unbounded()
    {
        var budget = Budget(new LatticeOptions
        {
            MaxScanPageStallDuration = TimeSpan.FromSeconds(25),
            MaxScanPageDuration = TimeSpan.Zero,
        });

        Assert.That(budget.AffordableVectorCount, Is.EqualTo(RepoContextExactScanBudget.Unbounded),
            "With the cooperative bound switched off the store publishes no nominal page cost, so there is "
            + "nothing to project a gather duration from. Guessing one would be the magic constant this "
            + "derivation exists to avoid.");
    }

    [Test]
    public void The_projection_is_a_pure_function_of_the_two_bounds()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextExactScanBudget.AffordableVectors(
                    TimeSpan.FromSeconds(25), TimeSpan.FromSeconds(5), 100),
                Is.EqualTo(500));
            Assert.That(
                RepoContextExactScanBudget.AffordableVectors(
                    TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(5), 100),
                Is.EqualTo(100),
                "Floored at one page even when the pair is misconfigured the wrong way round.");
            Assert.That(
                RepoContextExactScanBudget.AffordableVectors(
                    TimeSpan.FromDays(400), TimeSpan.FromMilliseconds(1), 1000),
                Is.EqualTo(RepoContextExactScanBudget.Unbounded),
                "A projection that would overflow an int saturates rather than wrapping negative.");
            Assert.That(
                RepoContextExactScanBudget.AffordableVectors(
                    TimeSpan.Zero, TimeSpan.FromSeconds(5), 100),
                Is.EqualTo(RepoContextExactScanBudget.Unbounded));
        });
    }

    [Test]
    public void Invalid_arguments_are_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextExactScanBudget(null!), Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextExactScanBudget.AffordableVectors(
                    TimeSpan.FromSeconds(25), TimeSpan.FromSeconds(5), 0),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => RepoContextExactScanBudgets.Default().EffectiveStallCeiling(null!),
                Throws.ArgumentNullException);
        });
    }
}

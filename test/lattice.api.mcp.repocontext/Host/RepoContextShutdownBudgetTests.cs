using Microsoft.Extensions.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the derivation of the host's shutdown budget from the container grant
/// the deployment declares (issue #2402).
/// </summary>
/// <remarks>
/// The property under test is not "the number is large enough" - no number is,
/// because drain time scales with a resident activation set that has no observed
/// ceiling. It is that the budget is <b>always strictly inside the grant</b>, so a
/// budget above the container's <c>stop_grace_period</c> cannot be expressed. That
/// matters because such a budget is not merely unreachable: it arms the
/// drain-abandoned alarm for an instant the process never lives to reach, silencing
/// the only evidence that a drain was cut short.
/// </remarks>
[TestFixture]
public sealed class RepoContextShutdownBudgetTests
{
    private static IConfiguration Configuration(params (string Key, string Value)[] values)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value)))
            .Build();

    [Test]
    public void An_undeclared_grant_yields_the_shipped_budget_unchanged()
    {
        var resolved = RepoContextShutdownBudget.Resolve(Configuration());

        Assert.Multiple(() =>
        {
            // The safety property that makes this landable in a bucket: a deployment
            // that sets nothing new derives exactly the budget it already ran with.
            Assert.That(resolved.ShutdownBudget, Is.EqualTo(TimeSpan.FromSeconds(90)));
            Assert.That(resolved.StopGracePeriod, Is.EqualTo(TimeSpan.FromSeconds(120)));
            Assert.That(resolved.GrantWasDeclared, Is.False);
        });
    }

    [Test]
    public void A_declared_grant_is_reported_as_declared()
    {
        var resolved = RepoContextShutdownBudget.Resolve(
            Configuration((RepoContextShutdownBudget.StopGracePeriodKey, "240s")));

        Assert.Multiple(() =>
        {
            Assert.That(resolved.StopGracePeriod, Is.EqualTo(TimeSpan.FromSeconds(240)));
            Assert.That(resolved.ShutdownBudget, Is.EqualTo(TimeSpan.FromSeconds(180)));

            // Carried so the startup log can distinguish a stated grant from an
            // assumed one. A stale declaration is undetectable at run time, so which
            // of the two the process believed has to be visible.
            Assert.That(resolved.GrantWasDeclared, Is.True);
        });
    }

    [Test]
    public void The_derivation_reproduces_the_shipped_pair_exactly()
    {
        // The whole claim that this change moves no deployed number. If it fails, the
        // container's behaviour has silently changed.
        Assert.That(
            RepoContextShutdownBudget.Derive(TimeSpan.FromSeconds(120)),
            Is.EqualTo(TimeSpan.FromSeconds(90)));
    }

    [Test]
    public void The_budget_is_strictly_inside_the_grant_at_every_scale()
    {
        // Swept rather than spot-checked, because the failure this guards against is
        // a scaling one: the two rules cross over at 8s, so a case list that happened
        // to sample one side only would pass while the other silenced the alarm.
        foreach (var seconds in new[] { 2.5, 3, 4, 6, 8, 10, 30, 90, 120, 300, 600, 3600 })
        {
            var grant = TimeSpan.FromSeconds(seconds);
            var budget = RepoContextShutdownBudget.Derive(grant);

            Assert.That(
                budget,
                Is.GreaterThan(TimeSpan.Zero),
                $"a {seconds}s grant produced a non-positive budget");
            Assert.That(
                budget,
                Is.LessThan(grant),
                $"a {seconds}s grant produced a budget that is not strictly inside it, so the host would be "
                + "killed at or before its own budget expired and the drain-abandoned line would never be emitted");
        }
    }

    [Test]
    public void A_small_grant_reserves_a_constant_rather_than_a_proportion()
    {
        // The correction that makes this derivation sound. A pure percentage reserves
        // a PROPORTION for an unwind whose cost is roughly CONSTANT, so at a 4s grant
        // it would leave 1s to log and flush before SIGKILL - not reliably enough, and
        // silently so, landing on whoever configured the tightest grace period.
        var budget = RepoContextShutdownBudget.Derive(TimeSpan.FromSeconds(4));

        Assert.That(budget, Is.EqualTo(TimeSpan.FromSeconds(2)));
        Assert.That(
            TimeSpan.FromSeconds(4) - budget,
            Is.EqualTo(RepoContextShutdownBudget.UnwindReserve),
            "below the crossover the constant reserve must bind, not the fraction");
    }

    [Test]
    public void A_large_grant_reserves_a_proportion_rather_than_the_constant()
    {
        var budget = RepoContextShutdownBudget.Derive(TimeSpan.FromSeconds(600));

        Assert.That(budget, Is.EqualTo(TimeSpan.FromSeconds(450)));
        Assert.That(
            TimeSpan.FromSeconds(600) - budget,
            Is.GreaterThan(RepoContextShutdownBudget.UnwindReserve),
            "above the crossover the fraction must bind, not the constant reserve");
    }

    [Test]
    public void The_two_reserve_rules_cross_over_where_the_documentation_says_they_do()
    {
        // UnwindReserve / (1 - BudgetFractionOfGrant) = 8s. Asserted so the comment
        // that states the intended operating range cannot quietly become false.
        var crossover = TimeSpan.FromSeconds(
            RepoContextShutdownBudget.UnwindReserve.TotalSeconds
            / (1d - RepoContextShutdownBudget.BudgetFractionOfGrant));

        Assert.That(crossover, Is.EqualTo(TimeSpan.FromSeconds(8)));
        Assert.That(
            RepoContextShutdownBudget.Derive(crossover),
            Is.EqualTo(crossover * RepoContextShutdownBudget.BudgetFractionOfGrant));
        Assert.That(
            RepoContextShutdownBudget.Derive(crossover),
            Is.EqualTo(crossover - RepoContextShutdownBudget.UnwindReserve));
    }

    [Test]
    public void A_grant_too_small_to_leave_a_budget_is_refused()
    {
        // Refused rather than clamped to something nominal: a budget of zero disables
        // the graceful drain while still reading as configured, which is the shape of
        // defect this class exists to remove.
        Assert.Throws<ArgumentOutOfRangeException>(
            () => RepoContextShutdownBudget.Derive(RepoContextShutdownBudget.UnwindReserve));
    }

    [Test]
    public void A_non_positive_grant_is_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => RepoContextShutdownBudget.Derive(TimeSpan.Zero));
            Assert.Throws<ArgumentOutOfRangeException>(
                () => RepoContextShutdownBudget.Derive(TimeSpan.FromSeconds(-1)));
        });
    }

    [Test]
    public void A_grant_is_accepted_with_or_without_the_seconds_suffix()
    {
        // Both spellings, so the value can be written identically to the
        // stop_grace_period it declares without that being a trap.
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextShutdownBudget.ParseStopGracePeriod("120"),
                Is.EqualTo(TimeSpan.FromSeconds(120)));
            Assert.That(
                RepoContextShutdownBudget.ParseStopGracePeriod("120s"),
                Is.EqualTo(TimeSpan.FromSeconds(120)));
            Assert.That(
                RepoContextShutdownBudget.ParseStopGracePeriod("  120s  "),
                Is.EqualTo(TimeSpan.FromSeconds(120)));
        });
    }

    [Test]
    public void A_compound_duration_is_refused_by_name_rather_than_misread()
    {
        // Compose's own grammar admits 1m30s, so an operator copying the value across
        // can legitimately arrive with one. Being told beats being silently misread as
        // 1 second or 30 seconds.
        var ex = Assert.Throws<InvalidOperationException>(
            () => RepoContextShutdownBudget.ParseStopGracePeriod("1m30s"));

        Assert.That(ex!.Message, Does.Contain("1m30s"));
        Assert.That(ex.Message, Does.Contain(RepoContextShutdownBudget.StopGracePeriodKey));
    }

    [Test]
    public void An_unusable_declared_grant_refuses_startup_rather_than_being_ignored()
    {
        // Silently ignoring an operator's intent is the failure this plumbing exists
        // to remove: nothing parses an unrecognised value, so there would be no error,
        // no warning, and no signal distinguishing "declared" from "discarded".
        foreach (var value in new[] { "abc", "0", "-5", "99999999", "" })
        {
            var config = Configuration((RepoContextShutdownBudget.StopGracePeriodKey, value));

            if (value.Length == 0)
            {
                // Blank is absence, not a bad value, and defaults rather than throwing.
                Assert.That(RepoContextShutdownBudget.Resolve(config).GrantWasDeclared, Is.False);
                continue;
            }

            Assert.Throws<InvalidOperationException>(
                () => RepoContextShutdownBudget.Resolve(config),
                $"'{value}' should have been refused");
        }
    }

    [Test]
    public void Resolve_rejects_a_null_configuration()
    {
        Assert.Throws<ArgumentNullException>(() => RepoContextShutdownBudget.Resolve(null!));
    }

    [Test]
    public void ParseStopGracePeriod_rejects_a_null_value()
    {
        Assert.Throws<ArgumentNullException>(() => RepoContextShutdownBudget.ParseStopGracePeriod(null!));
    }

    [Test]
    public void The_resolution_record_carries_what_the_startup_line_reports()
    {
        var resolution = new RepoContextShutdownBudgetResolution(
            TimeSpan.FromSeconds(120),
            TimeSpan.FromSeconds(90),
            GrantWasDeclared: true);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.StopGracePeriod, Is.EqualTo(TimeSpan.FromSeconds(120)));
            Assert.That(resolution.ShutdownBudget, Is.EqualTo(TimeSpan.FromSeconds(90)));
            Assert.That(resolution.GrantWasDeclared, Is.True);
        });
    }
}

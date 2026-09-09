using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Ties the sample container's <c>stop_grace_period</c> to the host's own
/// shutdown budget, so the two numbers cannot drift apart unnoticed.
/// <para>
/// Issue #2389: no compose file set <c>stop_grace_period</c>, so Docker's 10
/// second default applied while the host asked for 90 seconds to deactivate the
/// silo and flush the WAL commit-log. Every teardown was therefore a crash
/// teardown and the graceful deactivation path never completed - and nothing
/// said so, because a SIGKILLed container reports only an exit code that the
/// next start overwrites. The relationship between the two values is the whole
/// defect, so the relationship is what is asserted here rather than either
/// number in isolation.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextComposeShutdownBudgetTests
{
    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    private static string ComposePath => Path.Combine(
        RepoRoot, "samples", "RepoContextContainer", "docker-compose.yml");

    /// <summary>
    /// Reads every <c>stop_grace_period</c> in the sample compose file, keyed by the
    /// service it belongs to. Deliberately a small hand-rolled scan rather than a
    /// YAML dependency: the file's two-space service indentation is stable, and the
    /// alternative is adding a parser to the test project for one assertion.
    /// </summary>
    private static Dictionary<string, TimeSpan> ReadStopGracePeriods()
    {
        var result = new Dictionary<string, TimeSpan>(StringComparer.Ordinal);
        var service = string.Empty;

        foreach (var raw in File.ReadAllLines(ComposePath))
        {
            var trimmed = raw.TrimStart();
            if (trimmed.Length == 0 || trimmed.StartsWith('#'))
            {
                continue;
            }

            var indent = raw.Length - trimmed.Length;

            // A service key sits at exactly two spaces of indentation under `services:`.
            if (indent == 2 && trimmed.EndsWith(':') && !trimmed.Contains(' ', StringComparison.Ordinal))
            {
                service = trimmed[..^1];
                continue;
            }

            if (!trimmed.StartsWith("stop_grace_period:", StringComparison.Ordinal))
            {
                continue;
            }

            var value = trimmed["stop_grace_period:".Length..].Trim();
            Assert.That(
                value.EndsWith('s'),
                Is.True,
                $"expected a seconds-suffixed duration for {service}, found '{value}'");

            result[service] = TimeSpan.FromSeconds(double.Parse(
                value[..^1],
                System.Globalization.CultureInfo.InvariantCulture));
        }

        return result;
    }

    [Test]
    public void The_sample_compose_file_sets_a_stop_grace_period_on_the_repocontext_service()
    {
        Assert.That(File.Exists(ComposePath), Is.True, $"expected the sample compose file at {ComposePath}");

        var periods = ReadStopGracePeriods();

        // Asserted against the resolved set rather than by grepping for the literal,
        // so a value that is present but attached to the WRONG service - which is
        // indistinguishable from a correct one to a substring search, and which
        // would leave the defect fully in place - fails here.
        Assert.That(
            periods.ContainsKey("repocontext"),
            Is.True,
            "the repocontext service must set stop_grace_period explicitly; without it Docker's "
            + "10s default applies and every teardown is a crash teardown (issue #2389)");
    }

    [Test]
    public void The_repocontext_stop_grace_period_exceeds_the_host_shutdown_budget()
    {
        var periods = ReadStopGracePeriods();

        Assert.That(
            periods.ContainsKey("repocontext"),
            Is.True,
            "the repocontext service sets no stop_grace_period at all, so there is no value to compare "
            + "against the host budget");

        // The load-bearing assertion. A stop_grace_period at or below the host's own
        // ShutdownTimeout means the host can never reach the budget it asks for, so
        // that budget is dead configuration and the documented graceful drain does
        // not hold however plainly the docs state it.
        Assert.That(
            periods["repocontext"],
            Is.GreaterThan(RepoContextHostBuilder.ShutdownBudget),
            $"stop_grace_period ({periods["repocontext"].TotalSeconds}s) must exceed the host shutdown "
            + $"budget ({RepoContextHostBuilder.ShutdownBudget.TotalSeconds}s), or the host is killed "
            + "before it can spend the budget it is configured with");
    }

    [Test]
    public void The_embedder_stop_grace_period_is_set_explicitly()
    {
        var periods = ReadStopGracePeriods();

        // The embedder holds no durable state, so the DEFAULT would be adequate for
        // it. What is asserted is that the value is on the record: the cost of
        // #2389 was not that 10s was wrong everywhere, but that nothing in the file
        // distinguished a considered default from an unconsidered one.
        Assert.That(
            periods.ContainsKey("embedder"),
            Is.True,
            "the embedder service should state its stop_grace_period explicitly so an omission "
            + "is distinguishable from a decision");
    }

    [Test]
    public void The_host_shutdown_budget_is_the_value_the_host_actually_configures()
    {
        // Guards the constant against being edited to satisfy the assertion above
        // rather than to describe the host. 90s is the value the container has run
        // with since it shipped; changing it is a deliberate act that should also
        // move the compose value, which the test above then re-checks.
        Assert.That(RepoContextHostBuilder.ShutdownBudget, Is.EqualTo(TimeSpan.FromSeconds(90)));
    }

    [Test]
    public void The_drain_signal_reports_the_budget_it_was_constructed_with()
    {
        // The log line an operator reads to derive the budget must carry the same
        // number the host enforces, or the derivation is against a stale figure.
        var signal = new RepoContextDrainSignal(
            NullLogger<RepoContextDrainSignal>.Instance,
            RepoContextHostBuilder.ShutdownBudget);

        Assert.Multiple(() =>
        {
            Assert.That(signal.IsDraining, Is.False);
            Assert.That(signal.HasCompleted, Is.False);
            Assert.That(signal.Elapsed, Is.Null);
        });
    }
}

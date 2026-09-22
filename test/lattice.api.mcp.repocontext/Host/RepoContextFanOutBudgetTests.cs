using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the batch-write fan-out budget wiring (issues #3348, #3386):
/// the host's armed default against the library's unbounded one, the environment
/// override and its fail-fast validation, the explicit zero that restores the
/// unbounded library behaviour, and - the load-bearing one - that the zero
/// rollback resolves to <see cref="Timeout.InfiniteTimeSpan"/> rather than
/// <see cref="TimeSpan.Zero"/>.
/// </summary>
[TestFixture]
public sealed class RepoContextFanOutBudgetTests
{
    private static IConfiguration Configuration(string? raw)
    {
        var dict = new Dictionary<string, string?>();
        if (raw is not null)
        {
            dict[RepoContextFanOutBudget.FanOutBudgetKey] = raw;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Test]
    public void The_host_default_is_armed_where_the_library_default_is_not()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextFanOutBudget.DefaultFanOutBudgetSeconds, Is.GreaterThan(0),
                "the library ships this unbounded so no existing caller regresses on upgrade "
                + "(#3386), which is right for a library and wrong for one known deployment "
                + "whose fan-out collapse was measured");
            Assert.That(LatticeOptions.DefaultSetManyFanOutBudget, Is.EqualTo(Timeout.InfiniteTimeSpan),
                "if the library default ever becomes finite, this host wiring stops being the "
                + "thing that arms the bound and its rationale must be re-read rather than "
                + "silently inherited");
            Assert.That(
                RepoContextFanOutBudget.DefaultFanOutBudgetSeconds,
                Is.LessThanOrEqualTo(RepoContextFanOutBudget.MaxFanOutBudgetSeconds));
        });

    [Test]
    public void An_absent_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextFanOutBudget.ResolveBudgetSeconds(Configuration(null)),
            Is.EqualTo(RepoContextFanOutBudget.DefaultFanOutBudgetSeconds));

    [Test]
    public void A_blank_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextFanOutBudget.ResolveBudgetSeconds(Configuration("   ")),
            Is.EqualTo(RepoContextFanOutBudget.DefaultFanOutBudgetSeconds));

    [Test]
    public void An_explicit_value_wins_over_the_default()
        => Assert.That(RepoContextFanOutBudget.ResolveBudgetSeconds(Configuration(" 45 ")), Is.EqualTo(45));

    [Test]
    public void Zero_is_accepted_as_the_explicit_rollback_to_the_unbounded_library_behaviour()
        => Assert.That(
            RepoContextFanOutBudget.ResolveBudgetSeconds(Configuration("0")),
            Is.Zero,
            "rollback must be expressible as a VALUE rather than by unsetting the variable - "
            + "unsetting resolves the armed default, so an operator who wanted the library's "
            + "unbounded behaviour back would otherwise have no way to ask for it");

    [TestCase("-1")]
    [TestCase("thirty seconds")]
    [TestCase("3601")]
    public void An_out_of_range_or_unparseable_value_fails_the_host_fast(string raw)
        => Assert.That(
            () => RepoContextFanOutBudget.ResolveBudgetSeconds(Configuration(raw)),
            Throws.InvalidOperationException.With.Message.Contains(RepoContextFanOutBudget.FanOutBudgetKey),
            "the container must refuse to start rather than silently ignore the operator's intent; "
            + "a typo that fell back to the default would leave the deployment enforcing a budget "
            + "nobody chose");

    [Test]
    public void ResolveBudgetSeconds_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextFanOutBudget.ResolveBudgetSeconds(null!),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextFanOutBudget_rejects_a_null_silo()
        => Assert.That(
            () => RepoContextFanOutBudget.ConfigureRepoContextFanOutBudget(null!, Configuration(null)),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextFanOutBudget_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextFanOutBudget.ConfigureRepoContextFanOutBudget(new FakeSiloBuilder(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_option_lands_on_the_unnamed_instance_and_on_named_ones()
    {
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextFanOutBudget(Configuration("30"));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.Get(string.Empty).SetManyFanOutBudget, Is.EqualTo(TimeSpan.FromSeconds(30)),
                "the fan-out is a property of the batch-write call shape rather than of any one "
                + "tree, so the budget must be present on the unnamed instance");
            Assert.That(monitor.Get(RepoContextHostTrees.VectorMetadata).SetManyFanOutBudget, Is.EqualTo(TimeSpan.FromSeconds(30)),
                "ConfigureAll applies to every name, so a caller batching across trees is policed "
                + "identically whichever tree it happens to name");
        });
    }

    [Test]
    public void An_explicit_zero_resolves_to_infinite_rather_than_to_TimeSpan_Zero()
    {
        // This is the load-bearing test. TimeSpan.Zero and "unbounded" must not be
        // conflated at the option layer, and here they are actively opposite: a zero
        // budget would refuse EVERY fan-out immediately, failing every batch write on
        // the deployment, and the library's own options validator rejects zero for
        // exactly that reason. An operator typing the documented rollback value must
        // get the library's unbounded behaviour back, not a total outage.
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextFanOutBudget(Configuration("0"));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.That(
            monitor.Get(string.Empty).SetManyFanOutBudget,
            Is.EqualTo(Timeout.InfiniteTimeSpan),
            "the rollback value must restore the library default exactly; resolving it to "
            + "TimeSpan.Zero would turn an operator's rollback into an outage");
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> that exposes only its service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}

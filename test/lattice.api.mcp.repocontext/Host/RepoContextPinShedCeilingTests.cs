using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the pin-shed ceiling wiring (issue #3310): the host's armed
/// default, the environment override and its fail-fast validation, the explicit
/// zero that disarms it, and the fact that the option lands on the
/// <b>unnamed</b> options instance - the one <c>LeafCursorReporter</c> actually
/// reads when it resolves the ceiling.
/// </summary>
[TestFixture]
public sealed class RepoContextPinShedCeilingTests
{
    private static IConfiguration Configuration(string? raw)
    {
        var dict = new Dictionary<string, string?>();
        if (raw is not null)
        {
            dict[RepoContextPinShedCeiling.PinShedCeilingKey] = raw;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Test]
    public void The_host_default_is_armed()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextPinShedCeiling.DefaultPinShedCeilingSeconds, Is.GreaterThan(0),
                "the library ships this disarmed; the entire point of the host wiring is that "
                + "THIS deployment, where issue #3310 was measured, arms it");
            Assert.That(
                RepoContextPinShedCeiling.DefaultPinShedCeilingSeconds,
                Is.LessThanOrEqualTo(RepoContextPinShedCeiling.MaxPinShedCeilingSeconds));
        });

    [Test]
    public void An_absent_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextPinShedCeiling.ResolveCeilingSeconds(Configuration(null)),
            Is.EqualTo(RepoContextPinShedCeiling.DefaultPinShedCeilingSeconds));

    [Test]
    public void A_blank_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextPinShedCeiling.ResolveCeilingSeconds(Configuration("   ")),
            Is.EqualTo(RepoContextPinShedCeiling.DefaultPinShedCeilingSeconds));

    [Test]
    public void An_explicit_value_wins_over_the_default()
        => Assert.That(RepoContextPinShedCeiling.ResolveCeilingSeconds(Configuration(" 45 ")), Is.EqualTo(45));

    [Test]
    public void Zero_is_accepted_as_the_explicit_rollback_to_the_pre_fix_behaviour()
        => Assert.That(
            RepoContextPinShedCeiling.ResolveCeilingSeconds(Configuration("0")),
            Is.Zero,
            "rollback must be expressible, and it must be expressible as a VALUE rather than by "
            + "unsetting the variable - unsetting resolves the armed default, so an operator who "
            + "wanted the old behaviour back would otherwise have no way to ask for it");

    [TestCase("-1")]
    [TestCase("two minutes")]
    [TestCase("3601")]
    public void An_out_of_range_or_unparseable_value_fails_the_host_fast(string raw)
        => Assert.That(
            () => RepoContextPinShedCeiling.ResolveCeilingSeconds(Configuration(raw)),
            Throws.InvalidOperationException.With.Message.Contains(RepoContextPinShedCeiling.PinShedCeilingKey),
            "the container must refuse to start rather than silently ignore the operator's intent; "
            + "a typo that fell back to the default would leave the deployment reporting a ceiling "
            + "nobody chose");

    [Test]
    public void ResolveCeilingSeconds_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextPinShedCeiling.ResolveCeilingSeconds(null!),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextPinShedCeiling_rejects_a_null_silo()
        => Assert.That(
            () => RepoContextPinShedCeiling.ConfigureRepoContextPinShedCeiling(null!, Configuration(null)),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextPinShedCeiling_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextPinShedCeiling.ConfigureRepoContextPinShedCeiling(new FakeSiloBuilder(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_option_lands_on_the_unnamed_instance_the_reporter_reads()
    {
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextPinShedCeiling(Configuration("30"));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.Get(string.Empty).WalMaterialiserPinShedCeiling, Is.EqualTo(TimeSpan.FromSeconds(30)),
                "LeafCursorReporter resolves the ceiling from the unnamed options instance, so a "
                + "per-tree registration would silently do nothing - the shed window is keyed by "
                + "pin shard, and a pin shard spans trees");
            Assert.That(monitor.Get(RepoContextHostTrees.VectorMetadata).WalMaterialiserPinShedCeiling, Is.EqualTo(TimeSpan.FromSeconds(30)),
                "ConfigureAll applies to every name, so a named read agrees with the unnamed one");
        });
    }

    [Test]
    public void An_explicit_zero_leaves_the_option_null_rather_than_zero()
    {
        // Zero seconds and "disarmed" must not be conflated at the option layer. A
        // TimeSpan.Zero ceiling would compare as "the run is already older than the
        // ceiling" on the very first shed and force EVERY report through, disabling
        // the issue #2014 back-pressure outright and re-saturating the pin queue.
        // That would turn an operator's rollback into an outage, so the rollback
        // value must resolve to null (no ceiling) and not to a zero-length one.
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextPinShedCeiling(Configuration("0"));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.That(monitor.Get(string.Empty).WalMaterialiserPinShedCeiling, Is.Null);
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> that exposes only its service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}

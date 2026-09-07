using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the named-lock lease ceiling wiring: the host's opt-in default,
/// the environment override and its fail-fast validation, and the fact that the
/// option lands on the <b>unnamed</b> options instance (the one the lock grain
/// actually clamps against).
/// </summary>
/// <remarks>
/// The load-bearing assertion in this fixture is
/// <see cref="The_host_ceiling_exceeds_the_library_ceiling_it_exists_to_raise"/>.
/// Every other test here would still pass if the wiring were reverted to the
/// library default, because they only check internal consistency. That one fails,
/// and it fails with a message naming the condition rather than a bare number.
/// </remarks>
[TestFixture]
public sealed class RepoContextClaimLeasesTests
{
    private static IConfiguration Configuration(string? raw)
    {
        var dict = new Dictionary<string, string?>();
        if (raw is not null)
        {
            dict[RepoContextClaimLeases.MaxLockLeaseSecondsKey] = raw;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Test]
    public void The_host_ceiling_exceeds_the_library_ceiling_it_exists_to_raise()
        => Assert.That(
            RepoContextClaimLeases.DefaultMaxLockLeaseSeconds,
            Is.GreaterThan(LatticeOptions.MaxLockLeaseDurationValue.TotalSeconds),
            "the entire purpose of this wiring is to raise the library's 5-minute cap, which is "
            + "shorter than a build-and-test cycle and therefore expires a backlog claim mid-work; "
            + "if this fails, the host has silently reverted to evicting working agents");

    [Test]
    public void The_host_ceiling_covers_a_build_and_test_cycle()
        => Assert.That(
            RepoContextClaimLeases.DefaultMaxLockLeaseSeconds,
            Is.GreaterThanOrEqualTo(900),
            "a claim held across a build, a targeted test run and a push needs materially more "
            + "than the fifteen minutes asserted here; this is the floor, not the target");

    [Test]
    public void The_minimum_respects_the_library_default_lease()
        => Assert.That(
            RepoContextClaimLeases.MinMaxLockLeaseSeconds,
            Is.GreaterThanOrEqualTo(LatticeOptions.DefaultLockLeaseDurationValue.TotalSeconds),
            "MaxLockLeaseDuration must be at least DefaultLockLeaseDuration, or every "
            + "defer-to-default acquisition would be silently clamped below the default");

    [Test]
    public void The_default_sits_inside_the_accepted_range()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextClaimLeases.DefaultMaxLockLeaseSeconds, Is.GreaterThanOrEqualTo(RepoContextClaimLeases.MinMaxLockLeaseSeconds));
            Assert.That(RepoContextClaimLeases.DefaultMaxLockLeaseSeconds, Is.LessThanOrEqualTo(RepoContextClaimLeases.MaxMaxLockLeaseSeconds));
        });

    [Test]
    public void An_absent_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextClaimLeases.ResolveMaxLeaseSeconds(Configuration(null)),
            Is.EqualTo(RepoContextClaimLeases.DefaultMaxLockLeaseSeconds));

    [Test]
    public void A_blank_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextClaimLeases.ResolveMaxLeaseSeconds(Configuration("   ")),
            Is.EqualTo(RepoContextClaimLeases.DefaultMaxLockLeaseSeconds));

    [Test]
    public void An_explicit_value_wins_over_the_default()
        => Assert.That(RepoContextClaimLeases.ResolveMaxLeaseSeconds(Configuration(" 2400 ")), Is.EqualTo(2400));

    [Test]
    public void The_library_ceiling_remains_expressible_so_an_operator_can_revert()
        => Assert.That(
            RepoContextClaimLeases.ResolveMaxLeaseSeconds(Configuration("300")),
            Is.EqualTo(300),
            "an operator must be able to restore the library's behaviour without rebuilding the image");

    [TestCase("0")]
    [TestCase("-1")]
    [TestCase("29")]
    [TestCase("7201")]
    [TestCase("thirty minutes")]
    public void An_out_of_range_or_unparseable_value_fails_the_host_fast(string raw)
        => Assert.That(
            () => RepoContextClaimLeases.ResolveMaxLeaseSeconds(Configuration(raw)),
            Throws.InvalidOperationException.With.Message.Contains(RepoContextClaimLeases.MaxLockLeaseSecondsKey),
            "the container must refuse to start rather than silently ignore the operator's intent");

    [Test]
    public void ResolveMaxLeaseSeconds_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextClaimLeases.ResolveMaxLeaseSeconds(null!),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextClaimLeases_rejects_a_null_silo()
        => Assert.That(
            () => RepoContextClaimLeases.ConfigureRepoContextClaimLeases(null!, Configuration(null)),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextClaimLeases_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextClaimLeases.ConfigureRepoContextClaimLeases(new FakeSiloBuilder(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_option_lands_on_the_unnamed_instance_the_lock_grain_reads()
    {
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextClaimLeases(Configuration("1200"));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.CurrentValue.MaxLockLeaseDuration, Is.EqualTo(TimeSpan.FromSeconds(1200)),
                "LatticeLockGrain.ResolveLeaseTicks reads optionsMonitor.CurrentValue, so a per-tree "
                + "registration would silently do nothing and every lease would stay capped at 5 minutes");
            Assert.That(monitor.Get(string.Empty).MaxLockLeaseDuration, Is.EqualTo(TimeSpan.FromSeconds(1200)));
        });
    }

    [Test]
    public void The_wiring_does_not_lengthen_the_defer_to_default_lease()
    {
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextClaimLeases(Configuration(null));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.That(monitor.CurrentValue.DefaultLockLeaseDuration, Is.EqualTo(LatticeOptions.DefaultLockLeaseDurationValue),
            "only the ceiling an explicit request may reach is raised; a caller that did not think "
            + "about its lease length is exactly the caller that should not be granted a long one");
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> that exposes only its service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}

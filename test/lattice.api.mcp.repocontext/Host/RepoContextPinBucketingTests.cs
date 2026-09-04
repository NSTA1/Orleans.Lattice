using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the WAL materialiser pin bucketing wiring: the host's opt-in
/// default, the environment override and its fail-fast validation, and the fact
/// that the option lands on the <b>unnamed</b> options instance (the one the pin
/// router actually reads).
/// </summary>
[TestFixture]
public sealed class RepoContextPinBucketingTests
{
    private static IConfiguration Configuration(string? raw)
    {
        var dict = new Dictionary<string, string?>();
        if (raw is not null)
        {
            dict[RepoContextPinBucketing.PinBucketsKey] = raw;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Test]
    public void The_host_default_is_a_bucketed_opt_in()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextPinBucketing.DefaultPinBuckets, Is.GreaterThan(1),
                "the whole point of the host wiring is to opt out of the library's "
                + "single-bucket default, which rewrites the whole pin blob on every advance");
            Assert.That(RepoContextPinBucketing.DefaultPinBuckets, Is.LessThanOrEqualTo(RepoContextPinBucketing.MaxPinBuckets));
        });

    [Test]
    public void An_absent_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextPinBucketing.ResolveBucketCount(Configuration(null)),
            Is.EqualTo(RepoContextPinBucketing.DefaultPinBuckets));

    [Test]
    public void A_blank_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextPinBucketing.ResolveBucketCount(Configuration("   ")),
            Is.EqualTo(RepoContextPinBucketing.DefaultPinBuckets));

    [Test]
    public void An_explicit_value_wins_over_the_default()
        => Assert.That(RepoContextPinBucketing.ResolveBucketCount(Configuration(" 16 ")), Is.EqualTo(16));

    [Test]
    public void One_is_accepted_so_an_operator_can_revert_to_the_legacy_write_path()
        => Assert.That(RepoContextPinBucketing.ResolveBucketCount(Configuration("1")), Is.EqualTo(1));

    [TestCase("0")]
    [TestCase("-1")]
    [TestCase("eight")]
    [TestCase("257")]
    public void An_out_of_range_or_unparseable_value_fails_the_host_fast(string raw)
        => Assert.That(
            () => RepoContextPinBucketing.ResolveBucketCount(Configuration(raw)),
            Throws.InvalidOperationException.With.Message.Contains(RepoContextPinBucketing.PinBucketsKey),
            "the container must refuse to start rather than silently ignore the operator's intent");

    [Test]
    public void ResolveBucketCount_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextPinBucketing.ResolveBucketCount(null!),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextPinBucketing_rejects_a_null_silo()
        => Assert.That(
            () => RepoContextPinBucketing.ConfigureRepoContextPinBucketing(null!, Configuration(null)),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextPinBucketing_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextPinBucketing.ConfigureRepoContextPinBucketing(new FakeSiloBuilder(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_option_lands_on_the_unnamed_instance_the_pin_router_reads()
    {
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextPinBucketing(Configuration("4"));

        var monitor = silo.Services.BuildServiceProvider().GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.Multiple(() =>
        {
            Assert.That(monitor.Get(string.Empty).WalMaterialiserPinBuckets, Is.EqualTo(4),
                "WalMaterialiserPinRouting.ResolveBucketCount reads the unnamed options instance, "
                + "so a per-tree registration would silently do nothing");
            Assert.That(monitor.Get(RepoContextHostTrees.VectorMetadata).WalMaterialiserPinBuckets, Is.EqualTo(4),
                "ConfigureAll applies to every name, so a named read agrees with the unnamed one");
        });
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> that exposes only its service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}

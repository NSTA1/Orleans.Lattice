using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the WAL byte-pressure wiring: the host's opt-in default, the
/// environment override and its fail-fast validation, the explicit off switch, and
/// the fact that the ceiling lands on <b>every</b> repo-context tree's named options
/// - including the write-once payload tree the churn list deliberately excludes.
/// </summary>
[TestFixture]
public sealed class RepoContextWalRetentionTests
{
    private static IConfiguration Configuration(string? raw)
    {
        var dict = new Dictionary<string, string?>();
        if (raw is not null)
        {
            dict[RepoContextWalRetention.MaxRetainedBytesKey] = raw;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Test]
    public void The_host_default_arms_a_policy_the_library_leaves_off()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextWalRetention.DefaultMaxRetainedBytes,
                Is.GreaterThanOrEqualTo(RepoContextWalRetention.MinMaxRetainedBytes));
            Assert.That(new LatticeOptions().WalMaxRetainedBytes, Is.Null,
                "the whole point of this wiring is that the library ships the policy disabled, "
                + "because it cannot know the size of the volume the WAL lives on; if that "
                + "default ever changes, this host wiring needs revisiting rather than silently "
                + "duplicating it");
        });

    [Test]
    public void An_absent_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextWalRetention.ResolveMaxRetainedBytes(Configuration(null)),
            Is.EqualTo(RepoContextWalRetention.DefaultMaxRetainedBytes));

    [Test]
    public void A_blank_variable_resolves_the_host_default()
        => Assert.That(
            RepoContextWalRetention.ResolveMaxRetainedBytes(Configuration("   ")),
            Is.EqualTo(RepoContextWalRetention.DefaultMaxRetainedBytes));

    [Test]
    public void An_explicit_value_wins_over_the_default()
        => Assert.That(
            RepoContextWalRetention.ResolveMaxRetainedBytes(Configuration(" 268435456 ")),
            Is.EqualTo(268435456L));

    [Test]
    public void The_off_sentinel_restores_the_library_default()
        => Assert.That(
            RepoContextWalRetention.ResolveMaxRetainedBytes(
                Configuration(RepoContextWalRetention.DisabledValue)),
            Is.Null,
            "an operator must be able to opt out without editing the image, and the host "
            + "refuses an unparseable value rather than ignoring it, so 'off' needs a spelling");

    [TestCase("-1")]
    [TestCase("1024")]
    [TestCase("one gigabyte")]
    [TestCase("1GiB")]
    public void An_out_of_range_or_unparseable_value_fails_the_host_fast(string raw)
        => Assert.That(
            () => RepoContextWalRetention.ResolveMaxRetainedBytes(Configuration(raw)),
            Throws.InvalidOperationException.With.Message.Contains(
                RepoContextWalRetention.MaxRetainedBytesKey),
            "the container must refuse to start rather than silently ignore the operator's intent");

    [Test]
    public void ResolveMaxRetainedBytes_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextWalRetention.ResolveMaxRetainedBytes(null!),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextWalRetention_rejects_a_null_silo()
        => Assert.That(
            () => RepoContextWalRetention.ConfigureRepoContextWalRetention(null!, Configuration(null)),
            Throws.ArgumentNullException);

    [Test]
    public void ConfigureRepoContextWalRetention_rejects_a_null_configuration()
        => Assert.That(
            () => RepoContextWalRetention.ConfigureRepoContextWalRetention(new FakeSiloBuilder(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void The_ceiling_lands_on_every_repo_context_tree()
    {
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextWalRetention(Configuration("134217728"));

        var monitor = silo.Services.BuildServiceProvider()
            .GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.Multiple(() =>
        {
            foreach (var tree in RepoContextHostTrees.All)
            {
                Assert.That(monitor.Get(tree).WalMaxRetainedBytes, Is.EqualTo(134217728L),
                    $"the WAL garbage collector resolves the ceiling from tree '{tree}' by name, "
                    + "so a tree missed here reclaims nothing");
            }
        });
    }

    [Test]
    public void The_ceiling_reaches_the_write_once_payload_tree_the_churn_list_excludes()
    {
        // The discriminating test, and the reason this wiring iterates All rather than
        // ChurnTrees. The two lists answer different questions: churn is about
        // tombstones, which is why the write-once payload tree is excluded from it - it
        // has no in-place deletes to reap. WAL retention is about bytes on disk, and on
        // the container that motivated this the payload tree held the second-largest WAL
        // on the box at 1.1 GB. Copying the churn list would have missed a third of the
        // growth while looking entirely reasonable.
        Assert.That(RepoContextHostTrees.ChurnTrees, Does.Not.Contain(RepoContextHostTrees.VectorPayload),
            "if the payload tree ever joins the churn list this test stops discriminating "
            + "and should be re-pointed at whichever tree All still covers and ChurnTrees does not");

        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextWalRetention(Configuration(null));

        var monitor = silo.Services.BuildServiceProvider()
            .GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.That(
            monitor.Get(RepoContextHostTrees.VectorPayload).WalMaxRetainedBytes,
            Is.EqualTo(RepoContextWalRetention.DefaultMaxRetainedBytes));
    }

    [Test]
    public void The_off_sentinel_registers_no_configuration_at_all()
    {
        // Disabling must leave the library default in place, not write a null over it:
        // an operator who turns the policy off should get exactly the upstream
        // behaviour, and a registration that ran and assigned null would mask a later
        // ConfigureLattice call that meant to set one.
        var silo = new FakeSiloBuilder();
        silo.ConfigureRepoContextWalRetention(Configuration(RepoContextWalRetention.DisabledValue));

        Assert.That(silo.Services, Is.Empty,
            "the off sentinel must register nothing, so the library default stands unaltered");
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> that exposes only its service collection.</summary>
    private sealed class FakeSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// The concrete test of whether "default-on" is real: the repository-context
/// container must pick up every bounded-cold-start mechanism with <b>no change
/// to its compose file and no new environment variable</b>.
/// <para>
/// This box is the measured deployment the epic was written for, and it is
/// deliberately the hardest case. Its compose file sets no Lattice option, its
/// durability wiring never calls <c>ConfigureLattice</c>, and the epic's
/// out-of-scope list forbids adding a tuning knob to either - so the only way a
/// mechanism can reach this host is by being on in the library's own defaults.
/// A mechanism that needed a compose entry would be exactly the failure the
/// epic exists to prevent: a capability shipped to a deployment that never
/// switches it on.
/// </para>
/// <para>
/// The one place the host does configure Lattice is per-tree tombstone
/// compaction for its churn trees. Those overrides must therefore be asserted
/// not to shadow any mechanism default, since a named-options override replaces
/// the whole option set for that tree.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextDefaultOnMechanismTests
{
    /// <summary>A minimal <see cref="ISiloBuilder"/> carrying only a service collection.</summary>
    private sealed class CollectingSiloBuilder(IServiceCollection services, IConfiguration configuration)
        : ISiloBuilder
    {
        public IServiceCollection Services { get; } = services;

        public IConfiguration Configuration { get; } = configuration;
    }

    private static string RepoRoot => Path.GetFullPath(
        Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "..", ".."));

    /// <summary>
    /// Wires the host exactly as <c>RepoContextHostBuilder</c> does with respect
    /// to Lattice options - which is to say, only the per-tree compaction
    /// overrides - and hands back the monitor a grain would read through.
    /// </summary>
    private static (ServiceProvider Provider, IOptionsMonitor<LatticeOptions> Options) WireHost()
    {
        var services = new ServiceCollection();
        services.AddOptions();

        IConfiguration configuration = new ConfigurationBuilder().Build();
        new CollectingSiloBuilder(services, configuration).ConfigureRepoContextCompaction();

        var provider = services.BuildServiceProvider();
        return (provider, provider.GetRequiredService<IOptionsMonitor<LatticeOptions>>());
    }

    [TestCaseSource(typeof(RepoContextDefaultOnMechanismTests), nameof(EveryTree))]
    public void Every_mechanism_is_armed_on_every_repository_context_tree(string tree)
    {
        var (provider, monitor) = WireHost();
        using var _ = provider;
        var options = monitor.Get(tree);

        Assert.Multiple(() =>
        {
            Assert.That(options.WalGcInterval, Is.GreaterThan(TimeSpan.Zero), "responsive WAL GC");
            Assert.That(options.WalGcStartupDelay, Is.GreaterThan(TimeSpan.Zero), "WAL GC startup stagger");
            Assert.That(options.WalGcMinInterval, Is.GreaterThan(TimeSpan.Zero), "WAL GC adaptive floor");
            Assert.That(options.WalGcMinInterval, Is.LessThan(options.WalGcInterval), "WAL GC adaptive band");
            Assert.That(options.LeafSnapshotBinaryEncodingEnabled, Is.True, "binary leaf-snapshot codec");
            Assert.That(options.LeafPartialHydrationEnabled, Is.True, "bounded leaf hydration");
            Assert.That(options.LeafHydrationResidentBytes, Is.GreaterThan(0L), "bounded hydration residency cap");
            Assert.That(options.HotShardMinSkewRatio, Is.GreaterThan(1.0d), "shape-aware split admission");
            Assert.That(options.MaxPhysicalShardsPerTree, Is.GreaterThan(0), "physical-shard ceiling");
            Assert.That(options.HotShardMinShardEntries, Is.GreaterThan(0), "split occupancy floor");
            Assert.That(options.ShardHealingEnabled, Is.True, "automatic over-split healing");
            Assert.That(options.ShardHealingInterval, Is.GreaterThan(TimeSpan.Zero), "healing observation cadence");
            Assert.That(options.MaxConcurrentShardConsolidations, Is.GreaterThan(0), "healing admission cap");
            Assert.That(options.LeafCachePreWarmCount, Is.GreaterThan(0), "leaf-cache pre-warm");
        });
    }

    public static IEnumerable<string> EveryTree() => RepoContextHostTrees.All;

    [Test]
    public void The_hosts_compaction_overrides_do_not_shadow_a_mechanism_default()
    {
        // A named-options override replaces the whole option set for that tree,
        // so the churn trees are the one place a host edit could silently switch
        // a mechanism off. Assert the overrides change what they say they change
        // and nothing else.
        var (provider, monitor) = WireHost();
        using var _ = provider;

        var churn = monitor.Get(RepoContextHostTrees.Memory);
        var untouched = monitor.Get(RepoContextHostTrees.VectorPayload);

        Assert.Multiple(() =>
        {
            Assert.That(churn.TombstoneGracePeriod, Is.EqualTo(RepoContextCompaction.ChurnTombstoneGracePeriod));
            Assert.That(churn.MinTombstoneRatioForCompaction, Is.EqualTo(RepoContextCompaction.ChurnMinTombstoneRatio));
            Assert.That(
                churn.MaxLeafEntriesBeforeForcedCompaction,
                Is.EqualTo(RepoContextCompaction.ChurnMaxLeafEntriesBeforeForcedCompaction));

            Assert.That(untouched.TombstoneGracePeriod, Is.EqualTo(LatticeOptions.DefaultTombstoneGracePeriod),
                "the write-once payload tree must keep the library default");
            Assert.That(
                untouched.MaxLeafEntriesBeforeForcedCompaction,
                Is.EqualTo(LatticeOptions.DefaultMaxLeafEntriesBeforeForcedCompaction));
        });
    }

    [Test]
    public void Every_mechanism_knob_stays_at_the_library_default_on_every_tree()
    {
        // The host holds no opinion about any of these, so each must equal the
        // library's own default on every tree, churn tree or not. A future host
        // edit that pinned one of them - even to the same value - would be the
        // first step back towards a deployment that has to be configured.
        var (provider, monitor) = WireHost();
        using var _ = provider;

        Assert.Multiple(() =>
        {
            foreach (var tree in RepoContextHostTrees.All)
            {
                var options = monitor.Get(tree);
                Assert.That(options.WalGcInterval, Is.EqualTo(LatticeOptions.DefaultWalGcInterval), tree);
                Assert.That(options.WalGcStartupDelay, Is.EqualTo(LatticeOptions.DefaultWalGcStartupDelay), tree);
                Assert.That(options.WalGcMinInterval, Is.EqualTo(LatticeOptions.DefaultWalGcMinInterval), tree);
                Assert.That(options.WalMaxRetainedBytes, Is.Null, tree);
                Assert.That(options.LeafHydrationResidentBytes, Is.EqualTo(LatticeOptions.DefaultLeafHydrationResidentBytes), tree);
                Assert.That(options.HotShardMinSkewRatio, Is.EqualTo(LatticeOptions.DefaultHotShardMinSkewRatio), tree);
                Assert.That(options.HotShardConsolidationSkewRatio, Is.EqualTo(LatticeOptions.DefaultHotShardConsolidationSkewRatio), tree);
                Assert.That(options.MaxPhysicalShardsPerTree, Is.EqualTo(LatticeOptions.DefaultMaxPhysicalShardsPerTree), tree);
                Assert.That(options.ShardHealingInterval, Is.EqualTo(LatticeOptions.DefaultShardHealingInterval), tree);
                Assert.That(options.ShardHealingCooldown, Is.EqualTo(LatticeOptions.DefaultShardHealingCooldown), tree);
                Assert.That(options.ShardHealingBackpressureOpsPerSecond, Is.EqualTo(LatticeOptions.DefaultShardHealingBackpressureOpsPerSecond), tree);
                Assert.That(options.MaxConcurrentShardConsolidations, Is.EqualTo(LatticeOptions.DefaultMaxConcurrentShardConsolidations), tree);
                Assert.That(options.LeafCachePreWarmCount, Is.EqualTo(LatticeOptions.DefaultLeafCachePreWarmCount), tree);
            }
        });
    }

    [Test]
    public void The_compose_file_configures_no_lattice_option_at_all()
    {
        // The acceptance criterion, asserted against the file itself rather than
        // against an assumption about it. If a future change reaches for a
        // compose entry to switch a mechanism on, this fails and the mechanism's
        // default is what needs fixing instead.
        var compose = Path.Combine(RepoRoot, "samples", "RepoContextContainer", "docker-compose.yml");
        Assert.That(File.Exists(compose), Is.True, $"expected the sample compose file at {compose}");

        var text = File.ReadAllText(compose);

        Assert.Multiple(() =>
        {
            foreach (var knob in MechanismOptionNames)
            {
                Assert.That(text, Does.Not.Contain(knob),
                    $"the container must pick {knob} up from the library default, not from its compose file");
            }
        });
    }

    /// <summary>
    /// Every option this epic promotes or re-examines. Named here so the compose
    /// guard fails on any of them rather than on a single sentinel.
    /// </summary>
    private static readonly string[] MechanismOptionNames =
    [
        nameof(LatticeOptions.WalGcInterval),
        nameof(LatticeOptions.WalGcStartupDelay),
        nameof(LatticeOptions.WalGcMinInterval),
        nameof(LatticeOptions.WalMaxRetainedBytes),
        nameof(LatticeOptions.LeafSnapshotBinaryEncodingEnabled),
        nameof(LatticeOptions.LeafPartialHydrationEnabled),
        nameof(LatticeOptions.LeafHydrationResidentBytes),
        nameof(LatticeOptions.HotShardMinSkewRatio),
        nameof(LatticeOptions.HotShardConsolidationSkewRatio),
        nameof(LatticeOptions.HotShardMinShardEntries),
        nameof(LatticeOptions.MaxPhysicalShardsPerTree),
        nameof(LatticeOptions.ShardHealingEnabled),
        nameof(LatticeOptions.ShardHealingInterval),
        nameof(LatticeOptions.ShardHealingCooldown),
        nameof(LatticeOptions.ShardHealingBackpressureOpsPerSecond),
        nameof(LatticeOptions.MaxConcurrentShardConsolidations),
        nameof(LatticeOptions.LeafCachePreWarmCount),
        nameof(LatticeOptions.MaxLeafEntriesBeforeForcedCompaction),
    ];
}

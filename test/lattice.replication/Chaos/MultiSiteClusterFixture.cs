using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// N-site Orleans test harness for active-active convergence chaos
/// tests. Brings up <see cref="SiteCount"/> independent
/// <see cref="TestCluster"/> instances ("sites"), each with a single
/// silo configured with <c>AddLattice</c> + <c>AddLatticeReplication</c>
/// and the <em>default</em> <see cref="IReplogSink"/> so every
/// <c>ILattice</c> mutation lands in the real WAL and is observable
/// via <see cref="IChangeFeed"/>.
/// </summary>
internal sealed class MultiSiteClusterFixture
{
    /// <summary>Number of sites in the harness.</summary>
    public int SiteCount { get; }

    /// <summary>The cluster id assigned to site <paramref name="index"/>.</summary>
    public static string ClusterIdFor(int index) => $"site-{index}";

    private static readonly ConcurrentDictionary<string, ReplicationMode> Modes = new();

    /// <summary>
    /// Per-cluster silo-side <see cref="LatticeReplicationOptions"/>
    /// customisers. Keyed by the per-site cluster id so a single
    /// post-configure type can route every silo to the chaos test's
    /// requested overrides (e.g. <c>AtomicBatchDelivery=true</c>,
    /// tightened orphan timeouts) without per-site configurator
    /// duplication. The map is cleared in
    /// <see cref="DisposeAsync"/> so consecutive chaos tests do not
    /// leak overrides into the next fixture.
    /// </summary>
    private static readonly ConcurrentDictionary<string, Action<LatticeReplicationOptions>> SiloCustomizers = new();

    private readonly TestCluster[] _sites;
    private readonly IChangeFeed[] _changeFeeds;
    private readonly ReplicationApplier[] _appliers;
    private readonly ReplicationMode _mode;
    private readonly Action<LatticeReplicationOptions>? _siloCustomizer;
    private readonly Action<LatticeReplicationOptions>? _clientCustomizer;

    /// <summary>
    /// Creates the fixture. <paramref name="mode"/> is applied to every
    /// tree on every silo (chaos tests exercise a single tree per fixture).
    /// </summary>
    /// <param name="mode">
    /// Replication mode for every replicated tree on every silo.
    /// </param>
    /// <param name="siteCount">Number of independent silo clusters to spin up.</param>
    /// <param name="configureSilo">
    /// Optional silo-side customiser for every site's
    /// <see cref="LatticeReplicationOptions"/>. Runs as a
    /// post-configure after the cluster id has been mirrored from
    /// <see cref="Orleans.Configuration.ClusterOptions.ClusterId"/>;
    /// a chaos test that needs to opt into receiver-side
    /// atomic-batch delivery, tighten the orphan-sweep timeout, or
    /// flip any other replication option uses this hook.
    /// </param>
    /// <param name="configureClient">
    /// Optional client-side customiser for the test-fixture
    /// <see cref="ReplicationApplier"/>'s in-memory
    /// <see cref="LatticeReplicationOptions"/>. Mirrors
    /// <paramref name="configureSilo"/> for the manually-constructed
    /// per-site applier the chaos delivery pump dispatches into; the
    /// silo-side observer reads the silo-bound options via
    /// <see cref="IOptionsMonitor{TOptions}.Get(string)"/>, but the
    /// pump-side applier is constructed against this in-memory
    /// monitor so the two halves of the chaos pipeline must be
    /// configured symmetrically when receiver-side behaviour
    /// (e.g. <see cref="LatticeReplicationOptions.AtomicBatchDelivery"/>)
    /// is being exercised.
    /// </param>
    public MultiSiteClusterFixture(
        ReplicationMode mode,
        int siteCount = 3,
        Action<LatticeReplicationOptions>? configureSilo = null,
        Action<LatticeReplicationOptions>? configureClient = null)
    {
        if (siteCount < 2)
        {
            throw new ArgumentOutOfRangeException(nameof(siteCount), siteCount, "Chaos fixture requires at least two sites.");
        }

        _mode = mode;
        SiteCount = siteCount;
        _sites = new TestCluster[siteCount];
        _changeFeeds = new IChangeFeed[siteCount];
        _appliers = new ReplicationApplier[siteCount];
        _siloCustomizer = configureSilo;
        _clientCustomizer = configureClient;
    }

    /// <summary>Returns the cluster client for site <paramref name="index"/>.</summary>
    public IClusterClient ClientOf(int index) => _sites[index].Client;

    /// <summary>Returns the change feed for site <paramref name="index"/>.</summary>
    public IChangeFeed ChangeFeedOf(int index) => _changeFeeds[index];

    /// <summary>Returns the replication applier for site <paramref name="index"/>.</summary>
    public ReplicationApplier ApplierOf(int index) => _appliers[index];

    /// <summary>Stands up every site and prepares per-site change feeds and appliers.</summary>
    public async Task InitializeAsync()
    {
        for (var i = 0; i < SiteCount; i++)
        {
            Modes[ClusterIdFor(i)] = _mode;
            if (_siloCustomizer is not null)
            {
                SiloCustomizers[ClusterIdFor(i)] = _siloCustomizer;
            }
        }

        for (var i = 0; i < SiteCount; i++)
        {
            _sites[i] = await BuildSiteAsync(ClusterIdFor(i));

            var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
            var perSiteOptions = new LatticeReplicationOptions
            {
                ClusterId = ClusterIdFor(i),
                ReplogPartitions = 1,
            };
            _clientCustomizer?.Invoke(perSiteOptions);
            options.CurrentValue.Returns(perSiteOptions);
            options.Get(Arg.Any<string>()).Returns(perSiteOptions);

            _changeFeeds[i] = new ChangeFeed(_sites[i].Client, options);
            _appliers[i] = new ReplicationApplier(_sites[i].Client, options, new LocalVectorClockCache(_sites[i].Client));
        }
    }

    /// <summary>Stops and disposes every site.</summary>
    public async Task DisposeAsync()
    {
        for (var i = 0; i < SiteCount; i++)
        {
            if (_sites[i] is null)
            {
                continue;
            }

            await _sites[i].StopAllSilosAsync();
            await _sites[i].DisposeAsync();
        }

        for (var i = 0; i < SiteCount; i++)
        {
            Modes.TryRemove(ClusterIdFor(i), out _);
            SiloCustomizers.TryRemove(ClusterIdFor(i), out _);
        }
    }

    private static async Task<TestCluster> BuildSiteAsync(string clusterId)
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ClusterId = clusterId;
        builder.AddSiloBuilderConfigurator<ChaosSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private sealed class ChaosSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                // Placeholder; the post-configure below pulls the real
                // cluster id off ClusterOptions once Orleans has bound it.
                opts.ClusterId = "site-pending";
                opts.ReplogPartitions = 1;
            });

            siloBuilder.ConfigureServices(services =>
            {
                services.AddSingleton<IPostConfigureOptions<LatticeReplicationOptions>, ChaosClusterIdPostConfigure>();
                services.AddSingleton<IPostConfigureOptions<LatticeReplicationOptions>, ChaosCustomPostConfigure>();
                services.AddSingleton<IReplicationModeResolver, ChaosModeResolver>();
            });
        }
    }

    /// <summary>
    /// Mirrors the silo's <see cref="Orleans.Configuration.ClusterOptions.ClusterId"/>
    /// onto <see cref="LatticeReplicationOptions.ClusterId"/> so each site
    /// picks up its own id without per-site configurator code duplication.
    /// </summary>
    private sealed class ChaosClusterIdPostConfigure(IOptions<Orleans.Configuration.ClusterOptions> clusterOptions)
        : IPostConfigureOptions<LatticeReplicationOptions>
    {
        public void PostConfigure(string? name, LatticeReplicationOptions options)
        {
            options.ClusterId = clusterOptions.Value.ClusterId;
        }
    }

    /// <summary>
    /// Applies the per-cluster silo-side customiser registered on the
    /// fixture (if any). Runs as a separate post-configure so it
    /// always observes the cluster id that
    /// <see cref="ChaosClusterIdPostConfigure"/> mirrored — letting
    /// the customiser fan out per cluster id without re-resolving
    /// the cluster options itself.
    /// </summary>
    private sealed class ChaosCustomPostConfigure : IPostConfigureOptions<LatticeReplicationOptions>
    {
        public void PostConfigure(string? name, LatticeReplicationOptions options)
        {
            if (SiloCustomizers.TryGetValue(options.ClusterId, out var customizer))
            {
                customizer(options);
            }
        }
    }

    /// <summary>
    /// Returns the chaos test's configured mode for any tree, keyed off
    /// the silo's own cluster id so a single configurator type works for
    /// every site.
    /// </summary>
    private sealed class ChaosModeResolver(IOptionsMonitor<LatticeReplicationOptions> options) : IReplicationModeResolver
    {
        public ReplicationMode? Resolve(string treeId)
        {
            var clusterId = options.CurrentValue.ClusterId;
            return Modes.TryGetValue(clusterId, out var mode) ? mode : null;
        }
    }
}

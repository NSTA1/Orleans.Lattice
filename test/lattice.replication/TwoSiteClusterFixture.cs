using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Replication;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Two-site integration harness used by replication tests. Brings up two
/// independent <see cref="TestCluster"/> instances ("sites"), each with two
/// silos, every silo registering <c>AddLattice</c> + <c>AddLatticeReplication</c>
/// and an in-memory <see cref="LoopbackTransport"/>.
/// </summary>
internal sealed class TwoSiteClusterFixture
{
    /// <summary>Cluster id assigned to the first site.</summary>
    public const string SiteAClusterId = "site-a";

    /// <summary>Cluster id assigned to the second site.</summary>
    public const string SiteBClusterId = "site-b";

    private static readonly ConcurrentDictionary<string, LoopbackTransport> Transports = new();
    private static readonly ConcurrentDictionary<string, RecordingReplogSink> Sinks = new();

    /// <summary>
    /// Per-tree merge-mode overrides consulted by the silo merge-mode resolver.
    /// Defaults to <see cref="LatticeMergeMode.LwwRegister"/> for any tree not
    /// listed. Lets active-active tests declare a specific index tree (for
    /// example a <c>tag-{indexName}</c> tree) under a flag merge mode without
    /// re-deploying the cluster, because the test cluster runs in-process and
    /// the silo resolver reads this same static map.
    /// </summary>
    public static readonly ConcurrentDictionary<string, LatticeMergeMode> TreeModeOverrides = new();

    /// <summary>The first site's two-silo test cluster.</summary>
    public TestCluster SiteA { get; private set; } = null!;

    /// <summary>The second site's two-silo test cluster.</summary>
    public TestCluster SiteB { get; private set; } = null!;

    /// <summary>Loopback transport registered on every silo of <see cref="SiteA"/>.</summary>
    public LoopbackTransport SiteATransport { get; private set; } = null!;

    /// <summary>Loopback transport registered on every silo of <see cref="SiteB"/>.</summary>
    public LoopbackTransport SiteBTransport { get; private set; } = null!;

    /// <summary>Recording replog sink registered on every silo of <see cref="SiteA"/>.</summary>
    public RecordingReplogSink SiteASink { get; private set; } = null!;

    /// <summary>Recording replog sink registered on every silo of <see cref="SiteB"/>.</summary>
    public RecordingReplogSink SiteBSink { get; private set; } = null!;

    /// <summary>
    /// Captures every measurement recorded on the
    /// <c>orleans.lattice.replication</c> meter while the fixture is alive.
    /// Lets convergence tests assert on counter / histogram / gauge values
    /// rather than only on side effects.
    /// </summary>
    public ReplicationMetricsRecorder Metrics { get; private set; } = null!;

    /// <summary>Stands up both sites and waits for them to become ready.</summary>
    public async Task InitializeAsync()
    {
        Metrics = new ReplicationMetricsRecorder(LatticeReplicationMetrics.MeterName);

        SiteATransport = new LoopbackTransport();
        SiteBTransport = new LoopbackTransport();
        Transports[SiteAClusterId] = SiteATransport;
        Transports[SiteBClusterId] = SiteBTransport;

        SiteASink = new RecordingReplogSink();
        SiteBSink = new RecordingReplogSink();
        Sinks[SiteAClusterId] = SiteASink;
        Sinks[SiteBClusterId] = SiteBSink;

        SiteA = await BuildSiteAsync<SiteASiloConfigurator>();
        SiteB = await BuildSiteAsync<SiteBSiloConfigurator>();

        // The Orleans reminder service can still be completing initialization
        // for a moment after DeployAsync returns. The first write to any tree
        // registers the tombstone-compaction reminder, which faults with
        // "Reminder Service is still initializing" if it loses that startup
        // race. Warm each site here with a bounded retry so individual tests
        // never observe the race.
        await Task.WhenAll(
            WarmUpReminderServiceAsync(SiteA),
            WarmUpReminderServiceAsync(SiteB));
    }

    /// <summary>Stops and disposes both sites.</summary>
    public async Task DisposeAsync()
    {
        if (SiteA is not null)
        {
            await SiteA.StopAllSilosAsync();
            await SiteA.DisposeAsync();
        }

        if (SiteB is not null)
        {
            await SiteB.StopAllSilosAsync();
            await SiteB.DisposeAsync();
        }

        Metrics?.Dispose();

        Transports.TryRemove(SiteAClusterId, out _);
        Transports.TryRemove(SiteBClusterId, out _);
        Sinks.TryRemove(SiteAClusterId, out _);
        Sinks.TryRemove(SiteBClusterId, out _);
    }

    private static async Task<TestCluster> BuildSiteAsync<TConfigurator>()
        where TConfigurator : ISiloConfigurator, new()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.AddSiloBuilderConfigurator<TConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    /// <summary>
    /// Forces the reminder service on every silo of <paramref name="cluster"/>
    /// to finish initializing before tests run. Each warm-up write registers a
    /// tombstone-compaction reminder; the write is retried on the transient
    /// "Reminder Service is still initializing" fault. Several distinct tree ids
    /// are used so placement spreads across both silos in the site.
    /// </summary>
    private static async Task WarmUpReminderServiceAsync(TestCluster cluster)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        for (var i = 0; i < 4; i++)
        {
            var tree = cluster.Client.GetGrain<ILattice>($"reminder-warmup-{i}");
            while (true)
            {
                try
                {
                    await tree.SetAsync("warmup", new byte[] { 0 });
                    break;
                }
                catch (OrleansException ex) when (
                    ex.Message.Contains("Reminder Service is still initializing", StringComparison.Ordinal)
                    && DateTime.UtcNow < deadline)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(250));
                }
            }
        }
    }

    private static void ConfigureSilo(ISiloBuilder siloBuilder, string clusterId)
    {
        siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
        siloBuilder.UseInMemoryReminderService();
        siloBuilder.AddLatticeReplication(opts => opts.ClusterId = clusterId);

        // Replace the no-op transport registered by AddLatticeReplication with
        // the per-site loopback so tests can observe sends.
        if (Transports.TryGetValue(clusterId, out var transport))
        {
            siloBuilder.Services.AddSingleton<IReplicationTransport>(transport);
        }

        // Replace the default no-op replog sink with the per-site recorder so
        // change-feed tests can assert on captured entries.
        if (Sinks.TryGetValue(clusterId, out var sink))
        {
            siloBuilder.Services.AddSingleton<IReplogSink>(sink);
        }

        // Integration tests use many ad-hoc tree names; replace the default
        // options-backed mode resolver with a permissive stub that opts every
        // tree in to LwwRegister so individual tests do not need to enumerate
        // their tree ids on the silo configurator.
        siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            TreeModeOverrides.TryGetValue(treeId, out var mode) ? mode : LatticeMergeMode.LwwRegister;
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => ConfigureSilo(siloBuilder, SiteAClusterId);
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) => ConfigureSilo(siloBuilder, SiteBClusterId);
    }
}

using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Replication;
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

    /// <summary>Stands up both sites and waits for them to become ready.</summary>
    public async Task InitializeAsync()
    {
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

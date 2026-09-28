using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus.Integration;

/// <summary>
/// Two single-silo regions, <see cref="WestRegionId"/> and
/// <see cref="EastRegionId"/>, each running <c>AddLattice</c>,
/// <c>AddLatticeReplication</c> (replicating <see cref="TreeName"/> to the other
/// region) and <c>AddLatticeReplicationStatusApi</c>, joined by the in-process
/// <see cref="RegionLoopbackTransport"/>. The outbound liveness probe is disabled,
/// so an outbound contact can only come from a shipped batch.
/// </summary>
internal sealed class TwoRegionStatusClusterFixture : IAsyncDisposable
{
    /// <summary>The first region's cluster id.</summary>
    public const string WestRegionId = "status-it-west";

    /// <summary>The second region's cluster id.</summary>
    public const string EastRegionId = "status-it-east";

    /// <summary>The replicated tree.</summary>
    public const string TreeName = "status-it-orders";

    /// <summary>The west region's cluster.</summary>
    public TestCluster West { get; private set; } = null!;

    /// <summary>The east region's cluster.</summary>
    public TestCluster East { get; private set; } = null!;

    /// <summary>Deploys both regions and makes each reachable to the other.</summary>
    public async Task InitializeAsync()
    {
        West = await DeployAsync(WestRegionId);
        East = await DeployAsync(EastRegionId);
        RegionLoopbackTransport.Register(WestRegionId, SiloServices(West));
        RegionLoopbackTransport.Register(EastRegionId, SiloServices(East));
    }

    /// <summary>The peer-status facade hosted by <paramref name="cluster"/>'s silo.</summary>
    /// <param name="cluster">The region.</param>
    /// <returns>The facade.</returns>
    public static ILatticeReplicationStatus StatusOf(TestCluster cluster) =>
        SiloServices(cluster).GetRequiredService<ILatticeReplicationStatus>();

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        RegionLoopbackTransport.Unregister(WestRegionId);
        RegionLoopbackTransport.Unregister(EastRegionId);
        foreach (var cluster in new[] { West, East })
        {
            if (cluster is null)
            {
                continue;
            }

            await cluster.StopAllSilosAsync();
            await cluster.DisposeAsync();
        }
    }

    private static IServiceProvider SiloServices(TestCluster cluster) =>
        ((InProcessSiloHandle)cluster.Silos.First()).SiloHost.Services;

    private static async Task<TestCluster> DeployAsync(string regionId)
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ClusterId = regionId;
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = "pending");
            siloBuilder.Services.AddSingleton<IPostConfigureOptions<LatticeReplicationOptions>, RegionOptions>();
            siloBuilder.Services.AddSingleton<IReplicationTransport, RegionLoopbackTransport>();
            siloBuilder.AddLatticeReplicationStatusApi();
        }
    }

    private sealed class RegionOptions(IOptions<ClusterOptions> cluster) : IPostConfigureOptions<LatticeReplicationOptions>
    {
        public void PostConfigure(string? name, LatticeReplicationOptions options)
        {
            var local = cluster.Value.ClusterId;
            options.ClusterId = local;
            options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                [TreeName] = LatticeMergeMode.LwwRegister,
            };
            options.ReplicationPeers = new[] { local == WestRegionId ? EastRegionId : WestRegionId };
            options.ShipPhaseTimerPeriod = TimeSpan.FromMilliseconds(50);
            options.LivenessProbeInterval = Timeout.InfiniteTimeSpan;
        }
    }
}

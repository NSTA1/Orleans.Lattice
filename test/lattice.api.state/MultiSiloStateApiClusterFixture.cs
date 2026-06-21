using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Multi-silo fixture (three silos) used to prove the read facade and its gRPC
/// binding return cluster-wide state - not just the state local to the silo that
/// happens to serve a request. The lattice's shard, internal, and leaf grains,
/// plus the WAL and view-registry grains, are placed across silos by Orleans, so
/// every facade call exercises real cross-silo grain fan-out. In-memory grain
/// storage is backed by cluster-wide storage grains, so a grain's state survives
/// regardless of which silo it activates on.
/// </summary>
internal sealed class MultiSiloStateApiClusterFixture
{
    public const int SiloCount = 3;
    public const int ShardCount = 8;
    public const int MaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public IGrainFactory Client => Cluster.Client;

    /// <summary>Service providers of every silo, in deployment order.</summary>
    public IReadOnlyList<IServiceProvider> AllSiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().Select(s => s.SiloHost.Services).ToArray();

    /// <summary>The first silo's service provider.</summary>
    public IServiceProvider SiloServices => AllSiloServices[0];

    /// <summary>The read facade resolved from the first silo.</summary>
    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    /// <summary>
    /// The read facade resolved from a silo other than the one identified by
    /// <paramref name="notIndex"/>, so a request can be served by a silo that did
    /// not originate the state under test.
    /// </summary>
    public ILatticeStateQuery QueryFromOtherSilo(int notIndex = 0)
    {
        var services = AllSiloServices;
        var index = notIndex == 0 ? services.Count - 1 : 0;
        return services[index].GetRequiredService<ILatticeStateQuery>();
    }

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: SiloCount);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Registers <paramref name="treeId"/> with the given shard count and writes
    /// <paramref name="keyCount"/> keys, returning its grain reference.
    /// </summary>
    public async Task<ILattice> CreatePopulatedTreeAsync(
        string treeId,
        int keyCount,
        int shardCount = ShardCount)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = MaxLeafKeys,
            ShardCount = shardCount,
        });

        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"key-{i:D5}", new byte[] { (byte)(i & 0xFF) });
        }

        return tree;
    }

    /// <summary>
    /// Creates a runtime materialised view over <paramref name="sourceTreeId"/> by
    /// resolving the view factory from the silo identified by
    /// <paramref name="siloIndex"/>. The runtime view is recorded in the
    /// cluster-wide <c>IViewRegistryGrain</c>, so a facade served by a different
    /// silo must still observe it.
    /// </summary>
    public ILatticeView CreateViewOnSilo(string sourceTreeId, string viewName, int siloIndex)
    {
        var factory = AllSiloServices[siloIndex].GetRequiredService<ILatticeViewFactory>();
        var source = Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, new PredicateLatticeViewProjection()));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews();
            siloBuilder.AddLatticeStateApi();
        }
    }
}

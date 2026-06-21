using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Single-silo fixture for the tree-structure query endpoint. Pins
/// <see cref="LatticeOptions.DigestCoalescingWindowMs"/> to 0 so the pushed-up
/// topology is settled synchronously after each write, making structure reads
/// deterministic. Exposes helpers to populate multi-shard / multi-level trees
/// and to resolve a shard root for brute-force comparison.
/// </summary>
internal sealed class StructureClusterFixture
{
    public const int SmallMaxLeafKeys = 4;
    public const int SmallMaxInternalChildren = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    private ILatticeRegistry Registry =>
        Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
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

    /// <summary>Registers a tree and writes <paramref name="keyCount"/> sequential keys.</summary>
    public async Task<ILattice> CreatePopulatedTreeAsync(string treeId, int keyCount, int shardCount)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = shardCount,
            MaxLeafKeys = SmallMaxLeafKeys,
            MaxInternalChildren = SmallMaxInternalChildren,
        });

        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"key-{i:D5}", new byte[] { (byte)(i & 0xFF) });
        }

        return tree;
    }

    public async Task<IShardRootGrain> ResolveShardAsync(string treeId, int shardIndex)
    {
        var physicalTreeId = await Registry.ResolveAsync(treeId);
        return Cluster.GrainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
    }

    /// <summary>Registers a reserved <c>view-</c> backing tree directly in the registry.</summary>
    public Task RegisterViewBackingTreeAsync(string viewTreeId) =>
        Registry.RegisterAsync(viewTreeId, new TreeRegistryEntry { ShardCount = 1 });

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = 0);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeStateApi();
        }
    }
}

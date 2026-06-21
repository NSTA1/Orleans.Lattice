using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Single-silo fixture for the entry / key-range inspection endpoint. Pins a
/// small leaf fan-out so a modest key count still spreads across many leaves
/// and shards, and exposes helpers to register trees and write byte / string /
/// JSON / TTL entries for the scan and detail tests.
/// </summary>
internal sealed class EntryInspectionClusterFixture
{
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

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

    public async Task<ILattice> RegisterTreeAsync(string treeId, int shardCount)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = shardCount,
            MaxLeafKeys = SmallMaxLeafKeys,
        });

        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    /// <summary>Writes <paramref name="keyCount"/> sequential keys carrying small byte values.</summary>
    public async Task<ILattice> CreatePopulatedTreeAsync(string treeId, int keyCount, int shardCount)
    {
        var tree = await RegisterTreeAsync(treeId, shardCount);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync(KeyAt(i), Encoding.UTF8.GetBytes($"value-{i:D5}"));
        }

        return tree;
    }

    public static string KeyAt(int index) => $"key-{index:D5}";

    public static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>Registers a reserved <c>view-</c> backing tree directly in the registry.</summary>
    public Task RegisterViewBackingTreeAsync(string viewTreeId) =>
        Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .RegisterAsync(viewTreeId, new TreeRegistryEntry { ShardCount = 1 });

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

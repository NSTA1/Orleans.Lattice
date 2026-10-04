using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Single-silo cluster for the durable WAL move fence tests (issue #4525). Wires
/// the baseline ("default") provider and the hooked named ("secondary") provider
/// from <see cref="WalMoveFenceProviders"/>.
/// </summary>
public sealed class WalMoveFenceClusterFixture
{
    /// <summary>The currently-active test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Deploys the test cluster.</summary>
    public async Task InitializeAsync()
    {
        WalMoveFenceProviders.Reset();
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Tears down the test cluster.</summary>
    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>Pre-registers a single-shard tree and returns a reference to it.</summary>
    public async Task<ILattice> CreateTreeAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1 });
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalStorage(_ => WalMoveFenceProviders.Baseline);
            siloBuilder.AddLatticeWalStorageProvider("secondary", _ => WalMoveFenceProviders.Secondary);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

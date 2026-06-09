using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Shared in-memory WAL providers for the placement-move integration tests. The
/// silo configurator wires the baseline ("default") and a named ("secondary")
/// provider to these instances so the test can inspect each store directly.
/// </summary>
internal static class WalMoveProviders
{
    /// <summary>The "default" baseline WAL provider.</summary>
    public static InMemoryWalStorageProvider Baseline { get; private set; } = new();

    /// <summary>The "secondary" named WAL provider.</summary>
    public static InMemoryWalStorageProvider Secondary { get; private set; } = new();

    /// <summary>Resets both providers to empty stores between fixtures.</summary>
    public static void Reset()
    {
        Baseline = new InMemoryWalStorageProvider();
        Secondary = new InMemoryWalStorageProvider();
    }
}

/// <summary>
/// Cluster fixture for the WAL placement managed-move integration tests. Wires a
/// baseline ("default") WAL provider plus a named ("secondary") provider so a
/// partition can be moved between them through <see cref="ILatticeAdmin"/>.
/// </summary>
public sealed class WalPlacementMoveClusterFixture
{
    /// <summary>The currently-active test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Deploys the test cluster.</summary>
    public async Task InitializeAsync()
    {
        WalMoveProviders.Reset();
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
            // Override the baseline and add a named provider, both pointing at
            // the shared inspectable instances.
            siloBuilder.AddWalStorage(_ => WalMoveProviders.Baseline);
            siloBuilder.AddLatticeWalStorageProvider("secondary", _ => WalMoveProviders.Secondary);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

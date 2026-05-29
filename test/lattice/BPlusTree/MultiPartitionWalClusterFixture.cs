using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture for the multi-partition WAL replay integration
/// tests. Pins <see cref="LatticeOptions.WalPartitions"/> to a small
/// positive value (4) so the activation-time materialiser exercises
/// the per-partition fan-out, the per-partition checkpoint slot, the
/// per-partition cursor consumer ids, and the per-partition saga
/// clamp - all of which collapse to a no-op on the silo-wide default
/// (1).
/// </summary>
public sealed class MultiPartitionWalClusterFixture
{
    /// <summary>The pinned number of WAL partitions.</summary>
    public const int Partitions = 4;

    /// <summary>The currently-active test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Deploys the test cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
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

    /// <summary>
    /// Pre-registers <paramref name="treeId"/> in the tree registry
    /// with the fixture's pinned single-shard layout and returns a
    /// grain reference to it.
    /// </summary>
    public async Task<ILattice> CreateTreeAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = 1,
        });
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.WalPartitions = Partitions);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

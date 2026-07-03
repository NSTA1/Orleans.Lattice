using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture for the bounded read-through cache chaos test. Pins a very
/// small <see cref="LatticeOptions.MaxCacheValueBytes"/> so that a handful of
/// modestly-sized values overflows the budget and the vast majority of cached
/// payloads are evicted to the metadata sentinel - forcing the
/// eviction-delegation read path to carry almost every read. The correctness
/// invariant under test is that payload eviction never causes a false miss or a
/// stale / cross-key payload, no matter how aggressively the mirror is trimmed.
/// </summary>
public sealed class BoundedCacheClusterFixture
{
    /// <summary>The pinned per-activation value-payload budget, in bytes (8 KiB).</summary>
    public const long MaxCacheValueBytes = 8 * 1024;

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
    /// Registers <paramref name="treeId"/> with a large leaf-key cap (so the
    /// keyspace stays on a single leaf and the cache mirror is the dimension
    /// under test) and returns a grain reference to it.
    /// </summary>
    public async Task<ILattice> CreateTreeAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 100_000,
            ShardCount = 1,
        });
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.MaxCacheValueBytes = MaxCacheValueBytes);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

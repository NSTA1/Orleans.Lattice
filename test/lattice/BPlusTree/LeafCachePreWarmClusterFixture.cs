using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture for the opt-in post-restart leaf-cache pre-warm (issue
/// #332). Enables <see cref="LatticeOptions.LeafCachePreWarmCount"/> - which is
/// zero (off) everywhere else - and pins a small leaf-key cap so a modest
/// keyspace splits across several leaves, giving the per-shard leaf-access
/// Markov chain more than one state to rank.
/// </summary>
public sealed class LeafCachePreWarmClusterFixture
{
    /// <summary>The tree pre-registered by <see cref="InitializeAsync"/>.</summary>
    public const string TreeName = "prewarm-tree";

    /// <summary>The pinned number of leaf caches each shard pre-warms.</summary>
    public const int PreWarmCount = 8;

    /// <summary>The pinned per-leaf key cap, chosen to force several leaves.</summary>
    public const int MaxLeafKeys = 4;

    /// <summary>The currently-active test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Deploys the test cluster and registers <see cref="TreeName"/>.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();

        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(TreeName, new TreeRegistryEntry
        {
            MaxLeafKeys = MaxLeafKeys,
            ShardCount = 1,
        });
    }

    /// <summary>Tears down the test cluster.</summary>
    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.LeafCachePreWarmCount = PreWarmCount;

                // Keep the coalescing timer well outside the test's lifetime so
                // the ONLY flush under test is the deactivation flush. A timer
                // firing mid-test would mask a broken deactivation path.
                o.LeafAccessModelFlushIntervalMs = 10 * 60 * 1000;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}

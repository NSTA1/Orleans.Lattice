using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed class FourShardClusterFixture
{
    public const string TreeName = "four-shard-tree";
    public const int TestShardCount = 4;
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();

        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(TreeName, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = TestShardCount,
        });
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>
    /// Pre-registers <paramref name="treeId"/> in the tree registry with the fixture's
    /// pinned structural layout (<see cref="TestShardCount"/> shards, <see cref="SmallMaxLeafKeys"/>
    /// keys per leaf) and returns a grain reference to it. Tests that need a fresh per-test
    /// tree ID must call this instead of <c>GetGrain&lt;ILattice&gt;(...)</c> directly, otherwise
    /// the tree lazy-seeds from <see cref="LatticeConstants"/> defaults (64 shards / 128 keys
    /// per leaf) and subsequent <c>ReshardAsync</c> / <c>ResizeAsync</c> calls that target
    /// smaller values are rejected as shrinks.
    /// </summary>
    public async Task<ILattice> CreateTreeAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = TestShardCount,
        });
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
